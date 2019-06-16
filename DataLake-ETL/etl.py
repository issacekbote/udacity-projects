import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, \
    monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, \
    hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType

#Read donfig file to get AWS keys
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config\
            .get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config\
            .get('AWS','AWS_SECRET_ACCESS_KEY') 


def create_spark_session():
    """
    This function creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", \
            "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function processes the song files to extract songs and 
    artists data. The function transforms the extracted data and
    it then writes songs and artists data to s3 as a parquet files. 
    """
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*'
    
    # read song data file
    df = spark.read.format("json").load(song_data) 

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year",\
            "duration").dropna(how = "any", \
            subset = ["song_id"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id')\
    .parquet(output_data+'song_table.parquet')

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", \
            "artist_location", "artist_latitude", "artist_longitude")\
            .withColumnRenamed('artist_name', 'name')\
            .withColumnRenamed('artist_location', 'location')\
            .withColumnRenamed('artist_latitude', 'latitude')\
            .withColumnRenamed('artist_longitude', 'longitude')\
            .dropna(how = "any", subset = ["artist_id"])\
            .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    """
    This function processes the log files to extract users, time 
    amd songplay data. It transforms the extracted and writes the
    transformed data to s3 as a parquet files.
    """
    # get filepath to log data file
    log_data = input_data+'log-data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter((df.page == "NextSong") & (df.userId != ""))

    # extract columns for users table    
    users_table = df.select("UserId", "firstName", "lastName", \
        "gender", "level") \
        .withColumn("UserId", df["UserId"].cast("long"))\
        .withColumnRenamed("UserId", "user_id") \
        .withColumnRenamed("firstName", "first_name") \
        .withColumnRenamed("lastName", "last_name") \
        .dropna(how = "any", subset=['user_id']).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp\
                    (x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = (lambda x: from_unixtime(x / 1000)\
                    .cast(TimestampType()))
    df = df.withColumn("datetime", get_datetime(df.ts))
        
    # extract columns to create time table
    time_table = df.select("datetime", hour('timestamp')\
                .alias('hour'), dayofmonth('timestamp').alias('day'),\
                weekofyear('timestamp').alias('week'), \
                month('timestamp').alias('month'), \
                year('timestamp').alias('year'), \
                dayofweek('timestamp').alias('weekday')) \
                .filter(df.page == "NextSong") \
                .withColumnRenamed("datetime", "start_time")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month')\
                    .parquet(output_data+'time_table.parquet')

    # read in song data to use for songplays table
    song_df = df.select(monotonically_increasing_id()\
        .alias('songplay_id') ,"timestamp", "datetime", \
        month('timestamp').alias('month'),\
        year('timestamp').alias('year'),\
        "UserId", "level", "sessionId", "location",\
        "userAgent", "song", "length") \
        .withColumnRenamed("datetime", "start_time") \
        .withColumn("UserId", df["UserId"].cast("long")) \
        .withColumnRenamed("UserId", "user_id") \
        .withColumnRenamed("sessionId", "session_id") \
        .withColumnRenamed("userAgent", "user_agent") \
        .filter(df.page == "NextSong")

    # extract data for songs
    songs_table = spark.read.parquet(output_data+'song_table.parquet')

    # extract data for artists
    artists_table = spark.read.parquet(output_data+'artists_table.parquet')
    
    # extract columns from joined song and log datasets to 
    # create songplays table 
    songplays_table = (song_df.alias('sp')
    .join(songs_table.alias('s'),(col('sp.song')==col('s.title')) & \
    (col('sp.length')==col('s.duration')))\
    .join(artists_table.alias('a'),\
    col('s.artist_id')==col('a.artist_id'))) \
    .select(col('sp.songplay_id'), col('sp.start_time'), \
    col('sp.month'), col('sp.year'),col('sp.user_id'), \
    col('sp.level'), col('sp.session_id'),\
    col('sp.location'),col('sp.user_agent'), col('s.song_id'), \
    col('a.artist_id'))

    # write songplays table to parquet files partitioned by 
    # year and month
    songplays_table.write.partitionBy('year', 'month') \
    .parquet(output_data+'songplays_table.parquet')


def main():
    """
    This is main function of this module
    """
    spark = create_spark_session()
    input_data = "s3a://iss-dend/"
    output_data = "s3a://iss-dend/outdata/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
