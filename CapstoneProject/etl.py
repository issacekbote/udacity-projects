import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, \
    monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, \
    hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DoubleType, IntegerType, LongType
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
    immistaging_table = df.select([monotonically_increasing_id().\
                        alias('passid'), "cicid","i94yr", "i94mon",\
                        "i94cit", "i94port", "arrdate", "i94mode", \
                        "i94addr","depdate", "i94bir", "i94visa",\
                        "gender", "visatype"])

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

    #convert depdate sas date timestamp format 'YYYY-MM-DD'
    immistaging_table = immistaging_table.withColumn('departuredate',\
                        F.when(immistaging_table.depdate == 0.0, 0)\
                        .otherwise(get_date(immistaging_table.depdate))\
                        .cast(TimestampType()))                            

    #extract date data from staging table
    date_table = immistaging_table.select("passid", "arrivaldate", 
                       month('arrivaldate').alias('month'), year('arrivaldate').alias('year'), 
                       dayofmonth('arrivaldate').alias('day'), weekofyear('arrivaldate').alias('week'), 
                       dayofweek('arrivaldate').alias('dayofweek'))

    #extract passenger data from staging table
    passenger_table = immistaging_table\
            .select("passid", "cicid", "i94yr", "i94mon",\
            "i94cit", "i94port", "i94mode", "i94addr",\
            "i94bir", "i94visa", "gender", \
            "arrivaldate", "departuredate")\
            .withColumn("year", df["i94yr"].cast(IntegerType()))\
            .withColumn("month", df["i94mon"].cast(IntegerType()))\
            .withColumn("cic_id", df["cicid"].cast(LongType()))\
            .withColumn("res_code", df["i94cit"].cast(IntegerType()))\
            .withColumn("port_code", df["i94port"].cast(IntegerType()))\
            .withColumn("mode_code", df["i94mode"].cast(IntegerType()))\
            .withColumn("addr_code", df["i94addr"].cast(IntegerType()))\
            .withColumnRenamed("i94bir", "age")\
            .withColumnRenamed("i94visa", "visa")\
            .drop("i94yr")\
            .drop("i94mon")\
            .drop("cicid")\
            .drop("i94cit")\
            .drop("i94port")\
            .drop("i94mode")\
            .drop("i94addr")                       


def process_code_data(spark, input_data, output_data):
    """
    This fnction processes code files data
    """
    

def main():
    """
    This is main function of this module
    """
    spark = create_spark_session()
    input_data = "/Users/ISSAC/Documents/Projects/DEND/CapstoneProject/data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat"
    output_data = "/Users/ISSAC/Documents/Projects/DEND/CapstoneProject/outdata/"
    
    process_immigration_data(spark, input_data, output_data)
    process_code_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()