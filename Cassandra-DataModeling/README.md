# Cassandra Data Modeling and ETL Pipeline

## Objective: Design a data model and create a database to help analytical team at sparkify to understand what songs users are listening to

### This document contains below sections
1. ETL pipeline to preprocess the data
2. Modeling Apache Cassandra database
3. ETL pipeline to popualate tables

## ETL pipeline to preprocess the data
This part of ETL performs below steps to process the files to create the 'event_datafile_new.csv' data file csv that will be used for Apache Casssandra tables
* Get event data current folder and sub folder
* Create a for loop to create list of files and filepath


