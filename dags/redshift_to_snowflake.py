from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'john.uzoma',
    'depends_on_past': False,
    'start_date': datetime.now().date(),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG
dag = DAG(
    'redshift_to_snowflake_pipeline',
    default_args=default_args,
    description='Pipeline to transfer data from Redshift to Snowflake',
    schedule_interval=timedelta(days=1)
)

# Task 1: Unload Redshift tables to S3
unload_to_s3 = RedshiftSQLOperator(
    task_id='unload_to_s3',
    sql='unload_tables_to_S3.sql',
    redshift_conn_id='redshift_conn',
    dag=dag
)

# Task 2: Extract from S3, transform, and load to Snowflake
snowflake_etl = SnowflakeOperator(
    task_id='snowflake_etl',
    sql='CALL PRACTICE.EVENT_MGMT.ETL_JOB();',
    snowflake_conn_id='snowflake_conn',
    dag=dag
)

# Set task dependencies
unload_to_s3 >> snowflake_etl