# Postgres Data Modeling and ETL Pipeline

## Overview: Design a data model and create a database to help analytical team at sparkify to understand the users interests in songs and artists

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

| Field         | Data Types           |
|-------------- |----------------------|
| songplay_id   | serial primary key   |
| start_time    | timestamp NOT NULL   |
| user_id       | int NOT NULL         |
| level         | varchar NOT NULL     |
| song_id       | varchar              |
| artist_id     | varchar              |
| session_id    | int                  |
| location      | varchar              |
| user_gent     | varchar              |


#### Dimesnion tables
##### users
This table stores the users details who are subscribed to app. The data in this table is populated from log data set. The table has below columns.

| Field         | Data Types       |
|-------------- |------------------|
| user_id       | int primary key  |
| first_name    | varchar NOT NULL |
| last_name     | varchar NOT NULL |
| gender        | varchar          |
| level         | varchar          |

##### songs
This table stores the songs details. The data in this table is populated from the song data set. The table has below columns.

| Field         | Data Types            |
|-------------- |-----------------------|
| song_id       | varchar primary key   |
| title         | varchar               |
| artist_id     | varchar               |
| year          | int                   |
| duration      | float                 |

##### artists
This table contains the artists details and is populated from song data set. The table has below columns.

| Field         | Data Types            |
|-------------- |-----------------------|
| artist_id     | varchar primary key   |
| name          | varchar NOT NULL      |
| location      | varchar               |
| latitude      | float                 |
| longitude     | float                 |

##### time
This table is populated from the timestamp column of the log data set. The table has below columns.

| Field         | Data Types            |
|-------------- |-----------------------|
| start_time    | timestamp primary key |
| hour          | int NOT NULL          |
| day           | int NOT NULL          |
| week          | int NOT NULL          |
| month         | int NOT NULL          |
| year          | int NOT NULL          |
| weekday       | int NOT NULL          |


## ETL Pipeline:

The ETL processes the song data set and log data set and populates fact and dimension tables. The details of the design are below.

##### songs
**Source**: song data set
**Target**: songs dimension table
**Transformations**: Reads the data into pandas data frame. Extracts columns 'song_id', 'title', 'artist_id', 'year', 'duration' into a dataframe 'song_data', transforms the data frame to list and then loads the list into songs table using 'INSERT' SQL query.

##### artists:
**Source**: song data set
**Target**: artists dimension table
**Transformations**: Reads the data into pandas data frame. Extracts columns 'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' into a dataframe 'artist_data', transforms the data frame to list and then loads the list into artists table using 'INSERT' SQL query.

##### users
**Source**: log data set
**Target**: users dimension table
**Transformations**: Reads the data into pandas data frame from log data set. Extracts columns 'userId', 'firstName', 'lastName', 'gender', 'level' filtered by 'Nextsong' into a data frame 'user_df' and then loads the data frame into user table using 'INSERT' SQL query.

##### time
**Source**: log data set
**Target**: time dimension table
**Transformations**: Reads the data into pandas data frame from log data set. Extracts columns filtered by 'Nextsong' into a dataframe df. Converts column 'ts' into datetime to a series, and applies transformations to extract 'timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday'. Label the columns as 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday' and then loads data into time dimension table using 'INSERT' SQL query 

##### songplays
**Source**: log data set, songs and artists tables
**Target**: songplays fact table
**Transformations**: Get song ID and artist ID based on the title, artist name, and duration of a song from songs and artists tables by using 'Select' SQL query. Get timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent from log data file filtered by 'Nextsong' and load it into pandas data frame. Load the data into songplays fact table from dataframe and 'SELECT' query results using 'INSERT' SQL query. If there are no matching records for songid and artist id, store them as 'NULL' in songsplay table

## Populate Facts and Dimension tables
#### Perform below steps to populate Fact and Dimension tables
##### Run the create_tables.py script.
This script drops fact and dimension tables if exists and creates facts and dimension tables if not exists. The script imports sql_queries.py module that includes code to drop and create tables. The script uses psycopg2 library.

##### Run etl.py script
This script performs below steps
1. Processes song data to populate songs and artists tables
2. Processes log data to populate time, user and songplays tables.
The script imports sql_queries.py module to load data into fact and dimension tables and uses below libraries.
* os
* glob
* psycopg2
* pandas

## Example queries

#### Query to get song title,artist name played by user from the app
```sql
SELECT concat(u.first_name, ' ', u.last_name) as username, s.title as songtitle, a.name as artistname 
FROM ((songplays sp
JOIN songs s
ON sp.song_id = s.song_id)
JOIN users u
ON sp.user_id = u.user_id)                   
JOIN artists a
ON sp.artist_id = a.artist_id;
```

#### Query to get top 10 free subscription users for the year 2018
```sql
SELECT u.user_id, concat(u.first_name, ' ' ,u.last_name) AS username,t.year, sum(t.hour) FROM songplays sp
JOIN time t
ON sp.start_time = t.start_time
JOIN users u
ON sp.user_id = u.user_id
WHERE sp.level = 'free' AND t.year = 2018
GROUP BY t.year, u.user_id
ORDER BY sum(t.hour) DESC LIMIT 10;
```

#### Query to get top 10 paid subscription users for the year 2018
```sql
SELECT u.user_id, concat(u.first_name, ' ' ,u.last_name), t.year, sum(t.hour) FROM songplays sp
JOIN time t
ON sp.start_time = t.start_time
JOIN users u
ON sp.user_id = u.user_id
WHERE sp.level = 'paid' AND t.year = 2018
GROUP BY t.year, u.user_id
ORDER BY sum(t.hour) DESC LIMIT 10;
```

#### Query to get top 10 cities with highest number of listening hours (paid subscription) for the year 2018
```sql
SELECT sp.location, t.year, sum(t.hour) FROM songplays sp
JOIN time t
ON sp.start_time = t.start_time
WHERE sp.level = 'paid' AND t.year = 2018
GROUP BY t.year, sp.location
ORDER BY sum(t.hour) DESC LIMIT 10;
```

#### Query to get top 10 cities with highest number of listening hours (free subscription) for the year 2018
```sql
SELECT sp.location, sum(t.hour) FROM songplays sp
JOIN time t
ON sp.start_time = t.start_time
WHERE sp.level = 'free' AND t.year = 2018
GROUP BY t.year, sp.location
ORDER BY sum(t.hour) DESC LIMIT 10;
```

#### Query to get top 5 distinct free subscription users by location
```sql
SELECT location, count(DISTINCT(user_id)) usercount FROM songplays
WHERE level = 'free'
GROUP BY location
ORDER BY count(DISTINCT(user_id)) DESC LIMIT 5;
```

#### Query to get top 5 distinct paid subscription users by location
```sql
SELECT location, count(DISTINCT(user_id)) usercount FROM songplays
WHERE level = 'paid'
GROUP BY location
ORDER BY count(DISTINCT(user_id)) DESC LIMIT 5;
```