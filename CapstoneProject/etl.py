import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, \
    monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, \
    hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DoubleType, IntegerType
from pyspark.sql import functions as F


def create_spark_session():
    """
    This function creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", \
            "saurfang:spark-sas7bdat:2.0.0-s_2.11", \
            "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("sparkimmietl") \
        .getOrCreate()
    return spark

def process_immigration_data(spark, input_data, output_data):
    """
    This function processes the immigration files to extract immigrants data. 
    The function transforms the extracted data and
    it then writes data to s3 as a parquet files. 
    """
    
    #read immigration data file
    df=spark.read.format('com.github.saurfang.sas.spark')\
        .load(input_data)

    #extract required filed into staging table
    immistaging_table = df.select(["i94yr", "i94mon", "i94cit", \
                        "i94port", "arrdate", "i94mode", "i94addr",\
                         "depdate", "i94bir", "i94visa", "gender",\
                         "visatype"])

    #update depdate column
    immistaging_table = immistaging_table.withColumn('depdate',F.when\
                        (immistaging_table.depdate.isNull(), 0)\
                        .otherwise(immistaging_table.depdate))                

    #function to convert SAS date to format 'YYYY-MM-DD'
    get_date = udf(lambda x: (datetime.timedelta(days=x) + datetime\
                .datetime(1960,1,1)).strftime('%Y-%m-%d'))

    #convert arrival sas date to timestamp format 'YYYY-MM-DD'
    immistaging_table = immistaging_table.withColumn("arrivaldate", \
                        (get_date(immistaging_table.arrdate))\
                        .cast(TimestampType()))              

    #conver depdate sas date timestamp format 'YYYY-MM-DD'
    immistaging_table = immistaging_table.withColumn('departuredate',\
                        F.when(immistaging_table.depdate == 0.0, 0)\
                        .otherwise(get_date(immistaging_table.depdate))\
                        .cast(TimestampType()))                            

def main():
    """
    This is main function of this module
    """
    spark = create_spark_session()
    input_data = "/Users/ISSAC/Documents/Projects/DEND/CapstoneProject/data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat"
    output_data = "/Users/ISSAC/Documents/Projects/DEND/CapstoneProject/outdata/"
    
    process_immigration_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()