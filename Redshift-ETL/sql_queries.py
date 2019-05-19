import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create =(""" \
    CREATE TABLE IF NOT EXISTS staging_events ( \
        songplay_id INT IDENTITY(1,1) NOT NULL, \
        artist varchar, \
        auth varchar, \
        firstName varchar, \
        gender varchar, \
        itemInSession int, \
        lastName varchar, \
        length decimal, \
        level varchar, \
        location varchar, \
        method varchar, \
        page varchar, \
        registration varchar, \
        sessionId int, \
        song varchar, \
        status varchar, \
        ts TIMESTAMP, \
        userAgent varchar, \
        userId int \
        ); \
        """)

staging_songs_table_create = (""" \
    CREATE TABLE IF NOT EXISTS staging_songs ( \
    num_songs int NOT NULL, \
    artist_id varchar, \
    artist_latitude decimal, \
    artist_longitude decimal, \
    artist_location varchar, \
    artist_name varchar, \
    song_id varchar, \
    title varchar, \
    duration decimal, \
    year int \
    ); \
    """)

songplay_table_create = (""" \
    CREATE TABLE IF NOT EXISTS songplays ( \
        songplay_id INT NOT NULL sortkey, \
        start_time TIMESTAMP NOT NULL, \
        user_id int NOT NULL, \
        level varchar NOT NULL, \
        song_id varchar, \
        artist_id varchar, \
        session_id int, \
        location varchar, \
        user_agent varchar \
        )diststyle all; \
        """)

user_table_create = (""" \
    CREATE TABLE IF NOT EXISTS users ( \
    user_id int NOT NULL sortkey, \
    first_name varchar NOT NULL, \
    last_name varchar NOT NULL, \
    gender varchar, \
    level varchar \
    )diststyle all; \
    """)

song_table_create = (""" \
    CREATE TABLE IF NOT EXISTS songs ( \
    song_id varchar NOT NULL sortkey, \
    title varchar, \
    artist_id varchar distkey, \
    year int, \
    duration decimal \
    ); \
    """)

artist_table_create = (""" \
    CREATE TABLE IF NOT EXISTS artists ( \
    artist_id varchar NOT NULL sortkey, \
    name varchar NOT NULL, \
    location varchar distkey, \
    latitude decimal, \
    longitude decimal \
    ); \
    """)

time_table_create = (""" \
    CREATE TABLE IF NOT EXISTS time ( \
    start_time timestamp NOT NULL sortkey, \
    hour int NOT NULL, \
    day int NOT NULL, \
    week int NOT NULL, \
    month int NOT NULL, \
    year int NOT NULL, \
    weekday int NOT NULL \
    )diststyle all; \
    """)


# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}\
                        credentials 'aws_iam_role={}'\
                        compupdate off region 'us-west-2' FORMAT AS JSON {} timeformat 'epochmillisecs';\
                        """).format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = """copy staging_songs from {}
         credentials 'aws_iam_role={}'
         compupdate off region 'us-west-2' FORMAT AS JSON 'auto';
         """.format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))                        


# FINAL TABLES

songplay_table_insert = (""" \
        INSERT INTO songplays (songplay_id, start_time, user_id, \
        level, song_id, artist_id, session_id, location, user_agent) \
        SELECT DISTINCT songplay_id, ts, userID, level, songs.song_id,\
        artists.artist_id, sessionId, staging_events.location, \
        userAgent \
        FROM staging_events \
        JOIN songs \
        ON (songs.title = staging_events.song AND \
        songs.duration = staging_events.length) \
        JOIN artists \
        ON songs.artist_id = artists.artist_id \
        WHERE page like 'NextSong'; \
        """)

user_table_insert = (""" \
        INSERT INTO users (user_id, first_name, last_name, \
        gender, level) \
        SELECT DISTINCT userId, firstName, lastName, gender,\
        level from staging_events \
        WHERE page like 'NextSong'; \
        """)

song_table_insert = (""" \
        INSERT INTO songs (song_id, title, artist_id, year, duration) \
        SELECT DISTINCT song_id, title, artist_id, year, duration \
        FROM staging_songs; \
        """)

artist_table_insert = (""" \
        INSERT INTO artists (artist_id, name, location, latitude,\
        longitude) \
        SELECT DISTINCT artist_id, artist_name, artist_location, \
        artist_latitude, artist_longitude from staging_songs; \
        """)

time_table_insert = (""" \
        INSERT INTO time (start_time, hour, day, week, month, year, \
        weekday)\
        SELECT start_time, \
               EXTRACT(hour FROM start_time)    AS hour, \
               EXTRACT(day FROM start_time)     AS day, \
               EXTRACT(week FROM start_time)    AS week, \
               EXTRACT(month FROM start_time)   AS month, \
               EXTRACT(year FROM start_time)    AS year, \
               EXTRACT(dow FROM start_time)     AS weekday \
        FROM songplays; \
        """)


# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        songplay_table_create, user_table_create, 
                        song_table_create, artist_table_create, 
                        time_table_create]
drop_table_queries = [staging_events_table_drop, 
                        staging_songs_table_drop, 
                        songplay_table_drop, user_table_drop, 
                        song_table_drop, 
                        artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, 
                        song_table_insert, artist_table_insert, 
                        time_table_insert]
 