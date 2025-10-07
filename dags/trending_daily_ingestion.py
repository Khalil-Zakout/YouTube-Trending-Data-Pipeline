from airflow import DAG
from airflow.models import Variable
from airflow.models import Param
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from datetime import datetime
import pytz
import pendulum
import isodate
from googleapiclient.discovery import build
import pandas as pd
import json
import logging


YT_API= Variable.get("YT_API")
PROJECT_ID= Variable.get("PROJECT_ID")
RAW_BUCKET= Variable.get("RAW_BUCKET")
RAW_BUCKET_FILE= Variable.get("RAW_BUCKET_FILE")

BQ_RAW_DATASET= Variable.get("BQ_RAW_DATASET")
BQ_ANALYTICS_DATASET= Variable.get("BQ_ANALYTICS_DATASET")

BQ_RAW_TRENDING_TABLE= Variable.get("BQ_RAW_TRENDING_TABLE")
BQ_CHANNELS_TABLE= Variable.get("BQ_CHANNELS_TABLE")

DEFAULT_REGIONS= ['QA','US','DE']
default_args= {"owner": "Khalil", "retries":1, "retry_delay":10}

qatar_tz = pytz.timezone('Asia/Qatar')
execution_date = datetime.now(qatar_tz).date()


def get_youtube_client():
    return build("youtube", "v3", developerKey=YT_API)


def convert_pt_to_seconds(pt):
    total_seconds= isodate.parse_duration(pt).total_seconds()
    return int(total_seconds)

def normalize_datetime(iso_str):
    return datetime.fromisoformat(iso_str.replace("Z", ""))

def check_if_data_exists(project, dataset, table, **kwargs):
    bq_hook= BigQueryHook(gcp_conn_id= "google_cloud_default", use_legacy_sql= False)
    query= f"""SELECT id FROM `{project}.{dataset}.{table}` WHERE date= DATE('{execution_date}') LIMIT 1"""
    df= bq_hook.get_pandas_df(sql= query)

    if df.empty:
        return "fetch_full_json"
    else:
        return "end_dag"
    

def end_dag():
    logger= logging.getLogger(__name__)
    logger.info(f"Data for {datetime.today().date()} has been already Fetched !!")


def fetch_full_json(raw_bucket, raw_bucket_file, **kwargs):
    youtube_object= get_youtube_client()
    all_data= {}
    regions = kwargs.get("params", {}).get("regions", DEFAULT_REGIONS)

    if not regions:
        regions= DEFAULT_REGIONS

    if isinstance(regions, str):
        regions = json.loads(regions)
    
    print(regions)

    for region in regions:
        print(region)
        request= youtube_object.videos().list(part= "snippet,contentDetails,statistics", regionCode= region, chart="mostPopular", maxResults= 20)
        response= request.execute()

        kwargs["ti"].xcom_push(key= f"{region}_response", value= response)
        all_data[region]= response
    final_json= json.dumps(all_data, indent= 2)

    gcs_hook= GCSHook(gcp_conn_id= "google_cloud_default")
    gcs_hook.upload(
        bucket_name= raw_bucket,
        object_name= f"{raw_bucket_file}/{execution_date.strftime('%Y-%m-%d')}.json",
        data= final_json,
        mime_type="application/json"
    )

    
def add_channel_data(channel_id, project, dataset, table):
    youtube_object= get_youtube_client()
    request= youtube_object.channels().list(part= "snippet,statistics,status,brandingSettings", id= channel_id)
    response= request.execute()

    data= {"id":channel_id,
        "channel_name": response["items"][0]["snippet"]["title"],
        "country": response["items"][0]["snippet"].get("country","Unkown"),
        "creation_date": normalize_datetime(response["items"][0]["snippet"]["publishedAt"]) ,
        "made_for_kids": response["items"][0]["status"].get("madeForKids",False),
        "subscribers_count":int(response["items"][0]["statistics"].get("subscriberCount",0)),
        "views_count":int(response["items"][0]["statistics"].get("viewCount",0)),
        "videos_count":int(response["items"][0]["statistics"].get("videoCount",0)),
        "keywords":response["items"][0]["brandingSettings"].get("keywords","")}
    
    df= pd.DataFrame([data])
    
    bq_hook= BigQueryHook(gcp_conn_id= "google_cloud_default", use_legacy_sql= False)
    client= bq_hook.get_client()
    table_ref= f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND,)

    bq_job = client.load_table_from_dataframe(df, table_ref, job_config)
    bq_job.result()

    if bq_job.state == 'DONE':
        if bq_job.error_result:
            logging.error(f"BigQuery job failed: {bq_job.error_result}")
            raise ValueError(f"BigQuery job failed: {bq_job.error_result}")
        else:
            print(f"Successfully loaded {bq_job.output_rows} rows to {table_ref}")
    else:
        raise ValueError(f"BigQuery job ended with state: {bq_job.state}")




def fetch_trending_videos_data(project_id, channels_dataset, channels_table,
                            raw_bucket, raw_bucket_file, bq_raw_dataset, bq_trnding_table, **kwargs):
    try:
        query= f"""SELECT id FROM `{project_id}.{channels_dataset}.{channels_table}`"""
        bq_hook= BigQueryHook(gcp_conn_id= "google_cloud_default", use_legacy_sql= False)
        channels_df= bq_hook.get_pandas_df(sql= query)
        channels_set= set(channels_df["id"].to_list())

        dfs= [] # This Will Contain Each DataSet For Each Region (3 DataSets)

        gcs_hook= GCSHook(gcp_conn_id= "google_cloud_default")
        today_json= gcs_hook.download(bucket_name= raw_bucket, object_name=f"{raw_bucket_file}/{execution_date.strftime('%Y-%m-%d')}.json")
        today_json= json.loads(today_json)

        regions = kwargs.get("params", {}).get("regions", DEFAULT_REGIONS)

        if not regions:
            regions= DEFAULT_REGIONS

        if isinstance(regions, str):
            regions= json.loads(regions)

        for region in regions:
            if region in today_json:
                region_data= today_json[region]
                videos= [item for item in region_data["items"]]    # All Videos For A Specific Region (Each Is A JSON With A Lot of Data)
                all_videos= []     # Here We Are Going To Make A Dictionary For Each Video Containing Only Data We Need For Each Video.

                for v in videos:
                    if not v['snippet']['channelId'] in channels_set:
                        add_channel_data(channel_id= v['snippet']['channelId'], project= project_id,
                                        dataset= channels_dataset, table= channels_table)
                        channels_set.add(v['snippet']['channelId'])

                    row= {"id": v["id"],
                        "date": execution_date,
                        "category_id": v['snippet']['categoryId'],
                        "channel_id": v['snippet']['channelId'],
                        "comments_count": int(v['statistics'].get('commentCount',0)),
                        "likes_count": int(v['statistics'].get('likeCount',0)),
                        "views_count": int(v['statistics'].get('viewCount',0)),
                        "duration": convert_pt_to_seconds(v['contentDetails']['duration']),
                        "title": v['snippet']['title'], 
                        "publish_date": normalize_datetime(v['snippet']['publishedAt']),
                        "region": region} 
                    
                    all_videos.append(row)
    
                    
                df= pd.DataFrame(all_videos)   # This Is The Full DataSet With All Rows (Videos) For a Region
                dfs.append(df)
        
        final_df= pd.concat(dfs, ignore_index= True)    # One Final DataSet For All Regions Combined

        if final_df.empty:
            print("Warning: Final DataFrame is empty. Nothing to load to BigQuery.")

        print(f"Loading {len(final_df)} rows to BigQuery")
        
        client= bq_hook.get_client()
        table_ref= f"{project_id}.{bq_raw_dataset}.{bq_trnding_table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )


        bq_job= client.load_table_from_dataframe(final_df, table_ref, job_config=job_config)
        bq_job.result()
        if bq_job.state == 'DONE':
            if bq_job.error_result:
                raise Exception(f"BigQuery job failed: {bq_job.error_result}")
            else:
                print(f"Successfully loaded {bq_job.output_rows} rows to {table_ref}")
        else:
            raise Exception(f"Job ended with state: {bq_job.state}")


    except Exception as e:
        logger= logging.getLogger(__name__)
        logger.error(f"Error in fetch_trending_videos_data: {e}")
        raise



with DAG(

    dag_id= "trending_daily_ingestion",
    start_date=datetime(2025, 9, 27, tzinfo=pendulum.timezone("Asia/Qatar")),
    catchup= False,
    schedule= "45 23 * * *",
    tags= ["YouTube","Trending","Daily"],
    default_args = default_args,
    params={
        'regions': Param(
            default= DEFAULT_REGIONS,
            type= "array",
            description='List of region codes to process (default: ["QA", "US", "DE"] )',
            nullable= True)},

    description= ("Fetch daily trending YouTube videos for selected countries. "
    "Save the full video metadata (JSON) in Cloud SQL and essential data in BigQuery. "
    "Store related channel information in the channels table.")
) as dag:



    checking_data_existance_task= BranchPythonOperator(task_id= "checking_data_existance", python_callable= check_if_data_exists,
                                                        op_kwargs={"project": PROJECT_ID, "dataset": BQ_RAW_DATASET, "table": BQ_RAW_TRENDING_TABLE},
                                                        provide_context=True)
    
    end_dag_task= PythonOperator(task_id= "end_dag", python_callable= end_dag)

    fetch_full_json_task= PythonOperator(task_id= "fetch_full_json", python_callable= fetch_full_json,
                                        op_kwargs= {"raw_bucket":RAW_BUCKET, "raw_bucket_file": RAW_BUCKET_FILE}, provide_context=True)
    
    fetch_trending_videos_data_task= PythonOperator(task_id= "fetch_trending_videos_data", python_callable=fetch_trending_videos_data,
                                                    op_kwargs= {"project_id":PROJECT_ID,
                                                    "channels_dataset": BQ_RAW_DATASET, "channels_table":BQ_CHANNELS_TABLE,
                                                    "raw_bucket":RAW_BUCKET, "raw_bucket_file":RAW_BUCKET_FILE,
                                                    "bq_raw_dataset":BQ_RAW_DATASET, "bq_trnding_table":BQ_RAW_TRENDING_TABLE}
                                                    ,provide_context=True)
    


    checking_data_existance_task >> [end_dag_task, fetch_full_json_task]
    fetch_full_json_task >> fetch_trending_videos_data_task