# Postgres Data Modeling and ETL Pipeline

## Objective: Design a data model and create a database to help analytical team at sparkify to understand the users interests in songs and artists

This document comprises of below parts 
1. Database schema design and 
2. ETL pipeline to populate Fact and Dimension tables
3. Populate Facts and Dimension tables
4. Example queries

## Database Schema

Designed a STAR schema comprising of Fact and Dimension tables. Querying these tables will help in answering questions related to users, artists, and songs. Below are the table details that are part of the STAR schema

### Design details

#### Fact tables
##### songplays

This table stores the data about the songs played by users. The data in this table is populated from the log files, and from songs and artists dimension tables. The table has below columns

| Field         | Data Types                    |
|-------------- |-------------------------------|
| songplay_id   | serial primary key            |
| start_time    | timestamp NOT NULL            |
| user_id       | int NOT NULL                  |
| level         | varchar NOT NULL              |
| song_id       | varchar                       |
| artist_id     | varchar                       |
| session_id    | int                           |
| location      | varchar                       |
| user_gent     | varchar                       |

