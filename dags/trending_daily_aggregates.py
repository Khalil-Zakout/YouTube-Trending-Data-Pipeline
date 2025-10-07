from datetime import datetime
import pytz
import pendulum
import logging
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from google.cloud import bigquery


YT_API= Variable.get("YT_API")

PROJECT_ID= Variable.get("PROJECT_ID")
BQ_RAW_DATASET= Variable.get("BQ_RAW_DATASET")
BQ_ANALYTICS_DATASET= Variable.get("BQ_ANALYTICS_DATASET")  
BQ_RAW_TRENDING_TABLE= Variable.get("BQ_RAW_TRENDING_TABLE")
BQ_ANALYTICS_TABLE= Variable.get("BQ_ANALYTICS_TABLE")

default_args= {"owner":"Khalil", "retries":1, "retry_delay":10}
logger = logging.getLogger(__name__)

qatar_tz = pytz.timezone('Asia/Qatar')
execution_date = datetime.now(qatar_tz).date()


def check_if_today_data_exists(project_id, bq_analytics_dataset, bq_analytics_table, **kwargs):
        try:
            query= f"""SELECT COUNT(region) as count FROM `{project_id}.{bq_analytics_dataset}.{bq_analytics_table}`
                    WHERE `date`= DATE('{execution_date}')"""
            bq_hook= BigQueryHook(gcp_conn_id= "google_cloud_default", use_legacy_sql= False)
            trending_data= bq_hook.get_pandas_df(sql= query)
            count= trending_data["count"].iloc[0]
            if count==0:
                return "aggregate_data"
            else:
                return "end_dag"

        except Exception as e:
            logger= logging.getLogger(__name__)
            logger.error(f"{e}")



def end_dag(logger_object, **kwargs):
    logger_object.info(f"Data for {execution_date} Already Aggregated!!")
    



def aggregate_data(project_id, bq_raw_dataset, bq_trending_table, bq_analytics_dataset, bq_daily_analytics_table, **kwargs):
    try:
        bq_hook= BigQueryHook(gcp_conn_id= "google_cloud_default", use_legacy_sql= False)
        query= f"""SELECT * FROM `{project_id}.{bq_raw_dataset}.{bq_trending_table}` WHERE `date`= DATE('{execution_date}')"""
        full_data = bq_hook.get_pandas_df(sql= query)
        rows_to_insert= []

        regions = set(full_data["region"].to_list())


        for region in regions:
            region_data= full_data[full_data["region"]== region]

            region_daily_insights_row = {
                "region":region,
                "date": execution_date,
                "total_views": int(region_data["views_count"].sum()),
                "average_views":round(float(region_data["views_count"].mean()), 2),
                "highest_views": int(region_data["views_count"].max()),
                "total_likes": int(region_data["likes_count"].sum()),
                "average_likes": round(float(region_data["likes_count"].mean()),2),
                "highest_likes": int(region_data["likes_count"].max()),
                "total_comments": int(region_data["comments_count"].sum()),
                "average_comments": round(float(region_data["comments_count"].mean()),2),
                "highest_comments": int(region_data["comments_count"].max())
            }

            total_likes= int(region_data["likes_count"].sum())
            total_comments= int(region_data["comments_count"].sum())
            total_views= int(region_data["views_count"].sum())
            engagement_ratio= ((total_likes + (2*total_comments)) / total_views) * 1000 if total_views > 0 else 0

            region_daily_insights_row["engagement_ratio"]= engagement_ratio


            top_category_id= int(region_data["category_id"].value_counts().idxmax())
            region_daily_insights_row["top_category_id"]= top_category_id
            
            rows_to_insert.append(region_daily_insights_row)


        df_to_insert= pd.DataFrame(rows_to_insert)
        table_ref= f"{project_id}.{bq_analytics_dataset}.{bq_daily_analytics_table}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        
        client= bq_hook.get_client()
        bq_job= client.load_table_from_dataframe(df_to_insert,table_ref, job_config)
        
        bq_job.result()  

        if bq_job.error_result:
            raise Exception(f"BigQuery job failed: {bq_job.error_result}")
        else:
            print(f"Successfully loaded {bq_job.output_rows} rows to {table_ref}")
        

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.exception(f"Error in aggregate_data: {str(e)}")
        raise




with DAG(
    dag_id= "trending_daily_aggregates",
    start_date=datetime(2025, 9, 27, tzinfo=pendulum.timezone("Asia/Qatar")),
    catchup= False,
    schedule= "55 23 * * *",
    tags= ["YouTube","Trending","Daily"],
    default_args= default_args,
    description= ("Transforms raw daily trending videos data into analytical aggregates (e.g., top category per region, engagement metrics)"
    "and stores them in the analytics dataset for reporting."),

) as dag:
    
    check_if_today_data_exists_kwargs= {"project_id": PROJECT_ID, "bq_analytics_dataset": BQ_ANALYTICS_DATASET, "bq_analytics_table": BQ_ANALYTICS_TABLE}
    aggregate_data_kwargs= {"project_id":PROJECT_ID, "bq_raw_dataset":BQ_RAW_DATASET, "bq_trending_table":BQ_RAW_TRENDING_TABLE, "bq_analytics_dataset":BQ_ANALYTICS_DATASET, "bq_daily_analytics_table":BQ_ANALYTICS_TABLE}
    
    
    check_if_today_data_exists_task= BranchPythonOperator(task_id= "check_if_today_data_exists", python_callable= check_if_today_data_exists
                                                          ,op_kwargs= check_if_today_data_exists_kwargs)
    

    end_dag_task= PythonOperator(task_id= "end_dag", python_callable=end_dag, op_kwargs={"logger_object":logger})


    aggregate_data_task= PythonOperator(task_id= "aggregate_data", python_callable= aggregate_data, op_kwargs=aggregate_data_kwargs)



    check_if_today_data_exists_task >> [end_dag_task, aggregate_data_task]