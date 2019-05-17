# DROP STAGING TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# DROP Domension and Fact TABLES

songplay_fact_drop = "DROP TABLE IF EXISTS factsongplays"
user_dim_drop = "DROP TABLE IF EXISTS dimusers"
song_dim_drop = "DROP TABLE IF EXISTS dimsongs"
artist_dim_drop = "DROP TABLE IF EXISTS dimartists"
time_dim_drop = "DROP TABLE IF EXISTS dimtime"

# CREATE STAGING TABLES

songplay_table_create = (""" \
    CREATE TABLE IF NOT EXISTS songplays ( \
        songplay_id INT IDENTITY(1,1) PRIMARY KEY, \
        start_time timestamp NOT NULL, \
        user_id int NOT NULL, \
        level varchar NOT NULL, \
        song_id varchar, \
        artist_id varchar, \
        session_id int, \
        location varchar, \
        user_agent varchar \
        ); \
        """)

user_table_create = (""" \
    CREATE TABLE IF NOT EXISTS users ( \
    user_id int PRIMARY KEY, \
    first_name varchar NOT NULL, \
    last_name varchar NOT NULL, \
    gender varchar, \
    level varchar \
    ); \
    """)

song_table_create = (""" \
    CREATE TABLE IF NOT EXISTS songs ( \
    song_id varchar PRIMARY KEY, \
    title varchar, \
    artist_id varchar, \
    year int, \
    duration float \
    ); \
    """)

artist_table_create = (""" \
    CREATE TABLE IF NOT EXISTS artists ( \
    artist_id varchar PRIMARY KEY, \
    name varchar NOT NULL, \
    location varchar, \
    lattitude float, \
    longitude float \
    ); \
    """)

time_table_create = (""" \
    CREATE TABLE IF NOT EXISTS time ( \
    start_time timestamp PRIMARY KEY, \
    hour int NOT NULL, \
    day int NOT NULL, \
    week int NOT NULL, \
    month int NOT NULL, \
    year int NOT NULL, \
    weekday int NOT NULL \
    ); \
    """)


#CREATE Analytical tables

songplay_fact_create = (""" \
    CREATE TABLE IF NOT EXISTS factsongplays ( \
        songplay_id INT NOT NULL, \
        start_time timestamp NOT NULL sortkey, \
        user_id int NOT NULL, \
        level varchar NOT NULL, \
        song_id varchar, \
        artist_id varchar, \
        session_id int, \
        location varchar, \
        user_agent varchar \
        ); \
        """)

user_dim_create = (""" \
    CREATE TABLE IF NOT EXISTS dimusers ( \
    user_id int NOT NULL sortkey, \
    first_name varchar NOT NULL, \
    last_name varchar NOT NULL, \
    gender varchar, \
    level varchar \
    )
    diststyle all;; \
    """)


song_dim_create = (""" \
    CREATE TABLE IF NOT EXISTS dimsongs ( \
    song_id varchar NOT NULL sortkey, \
    title varchar, \
    artist_id varchar, \
    year int, \
    duration float \
    ); \
    """)


artist_dim_create = (""" \
    CREATE TABLE IF NOT EXISTS dimartists ( \
    artist_id varchar NOT NULL sortkey, \
    name varchar NOT NULL, \
    location varchar, \
    lattitude float, \
    longitude float \
    ); \
    """)

time_dim_create = (""" \
    CREATE TABLE IF NOT EXISTS dimtime ( \
    start_time timestamp NOT NULL sortkey, \
    hour int NOT NULL, \
    day int NOT NULL, \
    week int NOT NULL, \
    month int NOT NULL, \
    year int NOT NULL, \
    weekday int NOT NULL \
    ); \
    """)

# QUERY LISTS

create_stagingtable_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_stagingtable_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop,
                      time_table_drop]

create_dimensionfact_queries = [songplay_fact_create, user_dim_create,
                        song_dim_create, artist_dim_create,
                        time_dim_create]
drop_dimensionfact_queries = [songplay_fact_drop, user_dim_drop,
                      song_dim_drop, artist_dim_drop,
                      time_dim_drop]