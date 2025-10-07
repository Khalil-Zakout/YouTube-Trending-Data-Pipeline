from airflow import DAG, configuration
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import logging
from datetime import datetime, timedelta, date
import pendulum
import requests


configuration.conf.set('email', 'email_backend', 'airflow.utils.email.send_email_smtp')
default_args= {"owner":"Khalil", "retries":1, "retry_delay":10}
logger = logging.getLogger(__name__)

PROJECT_ID= Variable.get("PROJECT_ID")
BQ_ANALYTICS_DATASET= Variable.get("BQ_ANALYTICS_DATASET")
BQ_ANALYTICS_TABLE= Variable.get("BQ_ANALYTICS_TABLE")
EMAIL_TO= Variable.get("EMAIL_TO")
SENDGRID_API= Variable.get("SENDGRID_API")


def generate_weekly_insights(project_id, bq_analytics_dataset, bq_analytics_table, **kwargs):
    start_date= kwargs["execution_date"].date() - timedelta(days=6) 
    end_date= kwargs["execution_date"].date()

    query= f"""SELECT * FROM `{project_id}.{bq_analytics_dataset}.{bq_analytics_table}` WHERE date BETWEEN DATE('{start_date}') AND DATE('{end_date}')"""
    bq_hook= BigQueryHook(gcp_conn_id= "google_cloud_default", use_legacy_sql= False)
    data= bq_hook.get_pandas_df(sql= query)
    regions= data["region"].unique().tolist()

    regions_insights= {}
    
    for region in regions:
        region_data= data[data["region"] == region]
        top_category =int(region_data["top_category_id"].value_counts().idxmax())
        region_insights= {
            "top_category": top_category,
            "total_views_for_top_category": "{:,}".format(int(region_data[region_data["top_category_id"]==top_category]["total_views"].sum())),
            "total_likes_for_top_category": "{:,}".format(int(region_data[region_data["top_category_id"]==top_category]["total_likes"].sum())),
            "average_engagement_ratio": round(region_data[region_data["top_category_id"]==top_category]["engagement_ratio"].mean(),2)
        }

        regions_insights[region]= region_insights

    kwargs["ti"].xcom_push(key="weekly_insights", value= regions_insights)





def  format_email(**kwargs):
    insights= kwargs["ti"].xcom_pull(key= "weekly_insights", task_ids= "generate_weekly_insights")
    if not insights:
        raise ValueError("❌ No insights found. Cannot generate email body.")
    
    lines = ["<h3>Dear Mr. Alex</h3>"]
    lines.append("<p>I hope this email finds you well. The following are the weekly insights for the past week:</p>")
    lines.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>")
    lines.append("<tr><th>Region</th><th>Top Category</th><th>Total Views</th><th>Total Likes</th><th>Avg Engagement Ratio</th></tr>")
    
    for region, stats in insights.items():
        lines.append(
            f"<tr>"
            f"<td>{region}</td>"
            f"<td>{stats['top_category']}</td>"
            f"<td>{stats['total_views_for_top_category']}</td>"
            f"<td>{stats['total_likes_for_top_category']}</td>"
            f"<td>{stats['average_engagement_ratio']}</td>"
            f"</tr>"
        )
    
    lines.append("</table>")
    lines.append("""
        <p></p>
        <p>Best Regards,<br>Khalil</p>""")
    
    body = "\n".join(lines)
    kwargs["ti"].xcom_push(key="weekly_email_body", value=body)


def debug_email_body(**kwargs):
    body = kwargs["ti"].xcom_pull(key="weekly_email_body", task_ids="format_email")
    if not body:
        raise ValueError("❌ No Body found.")
    else:
        print("Email body:", body)


def send_email_via_sendgrid(**kwargs):
    body= kwargs["ti"].xcom_pull(key="weekly_email_body", task_ids= "format_email")

    data= {
        "personalizations": [{"to": [{"email": EMAIL_TO}]}],
        "from": {"email": "77khalilzeyad77@gmail.com"},
        "subject": f"Weekly Insights - {date.today().strftime('%d %b %Y')}",
        "content": [{"type": "text/html", "value": body}]}

    headers = {
        "Authorization": f"Bearer {SENDGRID_API}",
        "Content-Type": "application/json"
    }

    response = requests.post("https://api.sendgrid.com/v3/mail/send", json=data, headers=headers)
    if response.status_code == 202:
        print("✅ Email sent successfully")
    else:
        print(f"❌ Failed to send email: {response.text}")
        raise RuntimeError(f"Failed to send email: {response.text}")
        



with DAG(
    dag_id= "weekly_report",
    start_date=datetime(2025, 9, 29, tzinfo=pendulum.timezone("Asia/Qatar")),
    schedule= "0 0 * * 0",
    catchup= False,
    tags= ["YouTube","Trending","Daily"],
    default_args= default_args,
    description="Aggregate daily trending data into weekly insights and metrics",
) as dag:
    

    generate_weekly_insights_task= PythonOperator(task_id= "generate_weekly_insights", python_callable= generate_weekly_insights,
                                                  op_kwargs={"project_id":PROJECT_ID, "bq_analytics_dataset": BQ_ANALYTICS_DATASET,
                                                            "bq_analytics_table": BQ_ANALYTICS_TABLE,})
    
    format_email_task= PythonOperator(task_id= "format_email", python_callable= format_email)

    debug_email_task= PythonOperator(task_id= "debug_email_body", python_callable=debug_email_body)

    send_email_task= PythonOperator(task_id= "send_email", python_callable= send_email_via_sendgrid)



    
    

    

    generate_weekly_insights_task >> format_email_task >>debug_email_task >> send_email_task 