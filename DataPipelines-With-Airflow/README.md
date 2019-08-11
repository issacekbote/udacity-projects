# Data Pipelines with Airflow

## Objective: Automate and monitor data Warehouse ETL pipelines using Apache Airflow and do data quality check to catch any  discrepancies in the datasets. 

This document contain design details of Apache Airflow ETL pipeline.
1. DAG definition 
2. Tasks


## DAG definition
Define and instantiate DAG using below Default arguments and schedule it to run hourly.
owner
depends_on_past
start_date
retries
email_on_retry
retry_delay
catchup

## Task
### start_operator
This is a dummy operator to indicate the beginning of ETL process

### stage_events_to_redshift
This tasks loads staging_events table in redshift database from S3 data lake by using custom operator 'StageToRedshiftOperator' 