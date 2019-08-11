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