# Redshift and ETL Pipeline Documentation

## Objective: Design and build ETL pipeline and transform data to help analytical team at sparkify to understand the users interests in songs and artists

This document comprises of below parts
1. Redshift table design
2. ETL pipeline to populate staging, fact and dimension tables
3. Populate Fact and Dimension tables
4. Example queries

## Redshift table design

Designed a star schema comprising of Fact and Dimension tables optimized for querying data, staging tables to populate data from S3. Querying these tables will help in answering quesions related to users, artists, and songs. Below are the table details that are part of the STAR schema.

### Design details

#### Staging tables
##### staging_events

This table stores the songs data played by users. The data in this field is populated from log files stored in S3 storage of AWS.

staging_event schema

