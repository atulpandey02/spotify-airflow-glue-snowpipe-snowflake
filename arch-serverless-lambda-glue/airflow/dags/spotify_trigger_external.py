from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta


default_args={
    "owenr" : "atul",
    "depends_on_past" : False,
    "start_date" : datetime(2025 ,8,25)
}

dag = DAG(
    dag_id = "spotify_trigger_external",
    default_args = default_args,
    description = "Dag to trigger Lambda Function and Check s3 Upload",
    schedule_interval = timedelta(days=1),
    catchup =False,
)

trigger_extract_lambda = LambdaInvokeFunctionOperator(
    task_id = "trigger_extract_lambda",
    function_name = "spotify_api_data_extract",
    aws_conn_id = "aws_s3_airbnb" ,
    region_name = "us-east-1" ,
    dag=dag,
)

check_s3_upload = S3KeySensor(
    task_id = "check_s3_upload",
    bucket_key = "s3://spotify-etl-project-atulkumar/raw_data/to_processed/*",
    wildcard_match=True,
    aws_conn_id = "aws_s3_airbnb",
    timeout=60*60 ,
    poke_interval =60,
    dag=dag,
)

trigger_transform_lambda = LambdaInvokeFunctionOperator(
    task_id = "trigger_transform_lambda",
    function_name = "sotify_transformation_load_funtion",
    aws_conn_id = "aws_s3_airbnb" ,
    region_name = "us-east-1" ,
    dag=dag,
)

# trigger_glue_job = GlueJobOperator(
#         task_id='trigger_glue_job',
#         job_name='spotify_transformation_job',
#         script_location='s3://aws-glue-assets-206986907456-us-east-1/scripts/spotify_transformation_job.py', 
#         aws_conn_id='aws_s3_airbnb',
#         region_name='us-east-1',  
#         iam_role_name='spotify_glue_iam_role', 
#         s3_bucket='aws-glue-assets-206986907456-us-east-1',
#     )

trigger_extract_lambda >> check_s3_upload >> trigger_transform_lambda