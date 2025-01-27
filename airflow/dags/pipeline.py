from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.utils.dates import days_ago
from datetime import timedelta

AWS_CONN_ID = "aws_default"
S3_BUCKET = "your_s3_bucket_name"
S3_PREFIX = "your_s3_prefix"
LAMBDA_FUNCTION_NAME = {
    "articles": "lambda_function_articles",
    "blogs": "lambda_function_blogs",
    "reports": "lambda_function_reports"
}
REDSHIFT_CONN_ID = "redshift_connection"
SNOWFLAKE_CONN_ID = "snowflake_connection"

TABLES = {
    "articles": "articles_table",
    "blogs": "blogs_table",
    "reports": "reports_table"
}

dag = DAG(
    "lambda_to_s3_to_dw_dag",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval= "0 0 * * *",
    start_date=days_ago(1),
    catchup=False,
)

def run_lambda_and_load_to_dw(table_name):
    lambda_task = AwsLambdaInvokeFunctionOperator(
        task_id=f"invoke_lambda_{table_name}",
        function_name=LAMBDA_FUNCTION_NAME[table_name],
        aws_conn_id=AWS_CONN_ID,
        payload={"table_name": table_name},
        dag=dag,
    )
    
    s3_path = f"{S3_PREFIX}/{table_name}/"

    load_to_redshift = S3ToRedshiftOperator(
        task_id=f"load_{table_name}_to_redshift",
        schema="public",
        table=TABLES[table_name],
        s3_bucket=S3_BUCKET,
        s3_key=s3_path,
        copy_options=["FORMAT AS PARQUET"],
        aws_conn_id=AWS_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID,
        method="COPY",
        autocommit=True,
        dag=dag,
    )

    lambda_task >> load_to_redshift

for table_key in TABLES:
    run_lambda_and_load_to_dw(table_key)
