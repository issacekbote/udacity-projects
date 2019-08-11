import datetime
from datetime import timedelta
import logging
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries                                


#Define default arguments of DAG

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date' : datetime.datetime.now(),
    'retries': 3,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
}

#define DAG

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

#create tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Task to copy data from s3 to staging_events table in redshift 
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#configure task dependencies

start_operator >> end_operator