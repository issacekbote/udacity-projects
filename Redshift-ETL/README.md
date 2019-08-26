# Redshift and ETL Pipeline Documentation

## Overview: Design and build ETL pipeline and transform data to assist analytical team to understand the users interests in songs and artists
This document comprises of below parts
1. Redshift table design
2. ETL pipeline to populate staging, fact and dimension tables
3. Populate Fact and Dimension tables
4. Example queries

## Redshift table design
Designed a star schema comprising of Fact and Dimension tables optimized for querying data, staging tables to populate data from S3. Querying these tables will help in answering quesions related to users, artists, and songs. Below are the table details that are part of the STAR schema.

### Design details
#### Staging tables
##### staging_events:
This table stores the songs data played by users. The data in this table is populated from log files stored on S3 storage of AWS. The table contains below fields.

| Field         | Data Types |
|---------------|------------|
| artist        | varchar    |
| auth          | varchar    |
| firstname     | varchar    |
| gender        | varchar    |
| itemInSession | int        |
| lastName      | varchar    |
| length        | decimal    |   
| level         | varchar    |
| location      | varchar    |
| method        | varchar    |
| page          | varchar    |
| registration  | varchar    |
| sessionId     | int        |
| song          | varchar    |
| status        | varchar    |
| ts            | timestamp  |
| userAgent     | varchar    |
| userId        | int        |


##### staging_songs:
This table stores information about songs and artists. The data is populated from Song data files stored on S3 storage of AWS.
The table contains below fields.

| Field             | Data Types        |
|-------------------|-------------------|
| num_songs         | varchar NOT NULL  |
| artist_id         | varchar           |
| artist_latitude   | decimal           |
| artist_longitude  | decimal           |
| artist_location   | varchar           |
| artist_name       | varchar           |
| song_id           | varchar           |   
| title             | varchar           |
| duration          | decimal           |
| year              | int               |


#### Analytical tables (Fact and Dimension)
##### users
'users' is a dimension table that contains data about users information and their subscription level. Since amount of data in this table is small, 'ALL' distribution style strategy is used. The data is populated from 'staging_events' table. The table contains below fields.

| Field         | Data Types            |
|---------------|-----------------------|
| user_id       | int NOT NULL sortkey  |
| first_name    | varchar NOT NULL      |
| last_name     | varchar NOT NULL      |
| gender        | varchar               |
| level         | varchar               |


##### songs
This is a dimension table containing information about songs. The data is populated from 'staging_songs' table. Data is distributed on artist_id key to group songs by same artists and is sorted by song_id. The table contains below fields.

| Field         | Data Types                |
|---------------|---------------------------|
| song_id       | varchar NOT NULL sortkey  |
| title         | varchar                   |
| artist_id     | varchar distkey           |
| year          | int                       |
| duration      | decimal                   |


##### artists
'artists' is a dimension that table holds data about artists name and their location. Data in this table is populated from 'staging_songs'. The data is distributed and sorted by 'artist_id'. The table contains below fields.

| Field        | Data Types                         |
|--------------|------------------------------------|
| artist_id    | varchar NOT NULL sortkey distkey   |
| name         | varchar NOT NULL                   |
| location     | varchar distkey                    |
| latitude     | int                                |
| longitude    | decimal                            |


##### time
This is a dimension table containing transformed data from 'start_time' field of songplays fact table. 'ALL' distribution style strategy is used as the amount of data is small and is sorted by 'start_time'. The table has below fields.

| Field        | Data Types                     |
|--------------|--------------------------------|
| start_time   | timestamp NOT NULL sortkey     |
| hour         | int NOT NULL                   |
| day          | int NOT NULL                   |
| week         | int NOT NULL                   |
| month        | int NOT NULL                   |
| year         | int NOT NULl                   |
| weekday      | int NOT NULL                   |


#### songplays
This is a fact table and data is populated from 'staging_events', 'songs' and 'artists'. 'ALL' distribution style strategy is used as the amount data is small and is sorted by 'start_time'. The table has below fields.

| Field         | Data Types                    |
|-------------- |-------------------------------|
| songplay_id   | INT IDENTITY(1,1) NOT NULL    |
| start_time    | TIMESTAMP NOT NULL sortkey    |
| user_id       | int NOT NULL                  |
| level         | varchar NOT NULL              |
| song_id       | varchar                       |
| artist_id     | varchar                       |
| session_id    | int                           |
| location      | varchar                       |
| user_gent     | varchar                       |


## ETL Pipeline:

The ETL processes the song data set and log data set and populates staging tables. The data from staging tables is then populated to fact and dimension tables. The details of the design are below.

##### staging_event
Source: log data set
Target: staging_event table
Transformations: Reads the JSON data from S3 bucket on AWS to staging_event table. 'start_time' data is transformed using 'epochmillisecs' timeformat. Data is copied using copy command with 'JSONPaths' file option

##### staging_songs
Source: song data set
Target: staging_songs table
Transformations: Reads the JSON data from S3 bucket on AWS to staging_songs table. Data is copied using copy command with 'auto' option 

##### songs
Source: staging_songs
Target: songs dimension table
Transformations: Extracts distinct data for columns 'song_id', 'title', 'artist_id', 'year', 'duration', loads the data into songs table using 'Insert' SQL query.

##### artists
Source: staging_songs
Target: artists dimension table
Transformations: Extracts distinct data for columns 'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' and loads the data into artists table using 'Insert' SQL query.

##### users
Source: staging_events
Target: users dimension table
Transformations: Extracts distinct data for columns 'userId', 'firstName', 'lastName', 'gender', 'level' filtered by 'Nextsong'  and loads the data into user table using 'Insert' SQL query.

##### time
Source: songplays
Target: time dimension table
Transformations: Extracts data for column 'start_time' filtered by 'Nextsong'. Applies transformations to extract 'hour', 'day', 'week', 'month', 'year', 'weekday' and loads data into time dimension table using 'Insert' SQL query 

##### songplays
Source: staging_events
Target: songplays fact table
Transformations: Extract data for columns start_time, user_id, level, song_id, artist_id, session_id, location, user_agent from tables staging_events, songs, and artists filtered by 'Nextsong' and loads the data into songplays fact table using 'INSERT' SQL query. 

## Populate Fact and Dimension tables
#### Perform below steps to populate Fact and Dimension tables
##### Run the create_tables.py script.
This script drops staging, fact and dimension tables if exists and creates facts and dimension tables if not exists. The script imports sql_queries.py module that includes code to drop and create tables. The script uses psycopg2 library.

##### Run etl.py script
This script performs below steps
1. Processes song data to populate staging_events table
2. Processes log data to populate staging_songs table.
3. Loads data into users, songs, artists, songplays and time tables.
The script imports sql_queries.py module to load data into staging fact and dimension tables. The etl scripts reads 'dwh.cfg' file to get credentials to connect to redshift database. The script uses below libraries
*configparser
*psycopg2

## Example queries
#### Query to get song title,artist name played by user from the app
```sql
SELECT DISTINCT u.first_name || ' ' || u.last_name as username, s.title as songtitle, a.name as artistname 
FROM ((songplays sp
JOIN songs s
ON sp.song_id = s.song_id)
JOIN users u
ON sp.user_id = u.user_id)                   
JOIN artists a
ON sp.artist_id = a.artist_id limit 5;
```

#### Query to get top 10 free subscription users for the year 2018
```sql
SELECT DISTINCT u.user_id, u.first_name || ' ' || u.last_name as username, t.year, sum(t.hour) FROM songplays sp
JOIN time t
ON sp.start_time = t.start_time
JOIN users u
ON sp.user_id = u.user_id
WHERE sp.level = 'free' AND t.year = 2018
GROUP BY t.year, u.user_id, username
ORDER BY sum(t.hour) DESC LIMIT 10;
```

#### Query to get top 10 paid subscription users for the year 2018
```sql
SELECT DISTINCT u.user_id, u.first_name || ' ' || u.last_name as username, t.year, sum(t.hour) FROM songplays sp
JOIN time t
ON sp.start_time = t.start_time
JOIN users u
ON sp.user_id = u.user_id
WHERE sp.level = 'paid' AND t.year = 2018
GROUP BY t.year, u.user_id, username
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
