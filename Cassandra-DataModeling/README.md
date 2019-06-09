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

## Apache Cassandra database modeling
#### songplayed_session
Table songplayed_session is designed and created to answer below query.

```sql
select artist, song, length from songplayed_session WHERE sessionid=338 AND itemInSession = 4
```
Design consideration:
A compund primary key is used to uniquely identify the rows with 'sessionId' as partition key and 'itemInSession' as clustering key


#### songplaylist_session
Table songplaylist_session is designed and created to answer below query.

```sql
select artist, song, firstname, lastname from songplaylist_session WHERE userid=10 AND sessionid=182
```
Design consideration:
A compound primary key is used to uniquely identify the rows with 'userid' and 'sessionId' as composite partition key, and 'itemInSession' as clustering key in descending order. 'itemInSession' is used in descending order as descending queries are faster due to the nature of the storage engine and ascending order is more efficient to store. A composite partition key is used to break data into smaller chunks/logical sets to facilitate data retrieval and avoid hotspotting in writting data to one node repeatedly.

