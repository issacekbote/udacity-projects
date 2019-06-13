import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = '/Users/ISSAC/Documents/Projects/DEND/ \
                DataLakesWithSpark/data/song_data/*/*/*'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year",\
            "duration").dropna(how = "any", \
            subset = ["song_id"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet \
    ('song_table.parquet')


    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", \
            "artist_location", "artist_latitude", "artist_longitude") \
            .withColumnRenamed('artist_name', 'name')\
            .withColumnRenamed('artist_location', 'location')\
            .withColumnRenamed('artist_latitude', 'latitude')\
            .withColumnRenamed('artist_longitude', 'longitude')\
            .dropna(how = "any", subset = ["artist_id"]).dropDuplicates()
    
    # write artists table to parquet files
    artists.write.parquet('artists.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
