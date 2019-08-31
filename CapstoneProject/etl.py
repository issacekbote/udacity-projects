import configparser
import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, \
    monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, \
    hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DoubleType, \
    IntegerType, LongType
from pyspark.sql import functions as F
from pyspark.sql.functions import split

#Read config file to get AWS keys
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
        "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .appName("sparkimmietl") \
        .getOrCreate()
    return spark

def process_immigration_data(spark, input_data, output_data):
    """
    This function processes the immigration files to 
    extract immigrants data. The function transforms 
    the extracted data and it then writes data to s3 as 
    a parquet files. 
    """
    
    #set input path
    input_data = input_data + "18-83510-I94-Data-2016/"
    in_jul = input_data + "i94_jan16_sub.sas7bdat"
    in_aug = input_data + "i94_jan16_sub.sas7bdat"
    
    #read immigration data file
    df_jul=spark.read.format('com.github.saurfang.sas.spark')\
        .load(in_jul)
    df_aug=spark.read.format('com.github.saurfang.sas.spark')\
        .load(in_aug)
    
    #merge dataframes
    df = df_jul.union(df_aug)


    #extract required filed into staging table
    immistaging_table = df.select([monotonically_increasing_id().\
        alias('passid'), "cicid","i94yr", "i94mon",\
        "i94cit", "i94port", "arrdate", "i94mode", \
        "i94addr","depdate", "i94bir", "i94visa",\
        "gender", "visatype"])

    # drop null values in arrdate column to avoid wrong analysis 
    # and ensure high data quality
    immistaging_table = immistaging_table.na.drop(subset=["arrdate"])
    
    #update depdate column with zero for null values
    immistaging_table = immistaging_table.withColumn('depdate',F.when\
        (immistaging_table.depdate.isNull(), 0)\
        .otherwise(immistaging_table.depdate))                

    #function to convert SAS date to format 'YYYY-MM-DD'
    get_date = udf(lambda x: (datetime.timedelta(days=x) + datetime\
        .datetime(1960,1,1)).strftime('%Y-%m-%d'))

    #convert arrival sas date to timestamp format 'YYYY-MM-DD'
    immistaging_table = immistaging_table.\
    withColumn("arrivaldate", (get_date(immistaging_table.arrdate)))             

    #convert depdate sas date timestamp format 'YYYY-MM-DD'
    immistaging_table = immistaging_table.withColumn('departuredate',\
        F.when(immistaging_table.depdate == 0, 0)\
        .otherwise(get_date(immistaging_table.depdate))\
        .cast(TimestampType()))                            

    #extract date data from staging table
    date_table = immistaging_table.select("passid", "arrivaldate", 
        month('arrivaldate').alias('month'), year('arrivaldate')\
        .alias('year'), 
        dayofmonth('arrivaldate').alias('day'), \
        weekofyear('arrivaldate').alias('week'), 
        dayofweek('arrivaldate').alias('dayofweek'))
    
    #write date table to parquet file 
    
    date_table.write.partitionBy('year', 'month')\
                    .parquet(output_data+'date_table.parquet')
    
    # extract passenger data from staging table and convert 
    # data types to correct data types to get joins right
    immigration_table = immistaging_table\
        .select("passid", "cicid", "i94yr", "i94mon",\
        "i94cit", "i94port", "i94mode", "i94addr",\
        "i94bir", "i94visa", "gender", \
        "arrivaldate", "departuredate")\
        .withColumn("year", immistaging_table["i94yr"]\
            .cast(IntegerType()))\
        .withColumn("month", immistaging_table["i94mon"]\
            .cast(IntegerType()))\
        .withColumn("cic_id", immistaging_table["cicid"]\
            .cast(LongType()))\
        .withColumn("res_code", immistaging_table["i94cit"]\
            .cast(IntegerType()))\
        .withColumn("port_code", immistaging_table["i94port"])\
        .withColumn("mode_code", immistaging_table["i94mode"]\
            .cast(IntegerType()))\
        .withColumn("addr_code", immistaging_table["i94addr"])\
        .withColumnRenamed("i94bir", "age")\
        .withColumnRenamed("i94visa", "visa_code")\
        .drop("i94yr")\
        .drop("i94mon")\
        .drop("cicid")\
        .drop("i94cit")\
        .drop("i94port")\
        .drop("i94mode")\
        .drop("i94addr")                       

    # update mode and visa code with values
    immigration_table = immigration_table\
        .select("passid", "cic_id", "year", "month",\
        "res_code", "port_code", "mode_code", "addr_code",\
        "age", "visa_code", "gender", \
        "arrivaldate", "departuredate")\
        .withColumn("mode", F.when(immigration_table\
        .mode_code == 1, 'Air')
        .when(immigration_table.mode_code == 2, "Sea")\
        .when(immigration_table.mode_code == 3, "Land")\
        .when(immigration_table.mode_code == 9, 'Notreported')\
        .otherwise(None))\
        .withColumn("visa", F.when(immigration_table\
        .visa_code == 1, 'Business')\
        .when(immigration_table.visa_code == 2, "Pleasure")\
        .when(immigration_table.visa_code == 3, "Student")\
        .otherwise(None))
    
       
    #write passenger table to parquet
    immigration_table.write.partitionBy('year', 'month')\
        .parquet(output_data+'immigration_table.parquet') 
    
    
def process_code_data(spark, code_data, output_data):
    """
    This fnction processes code files data
    """

    #set input path for port codes
    input_data = code_data + "I94PORT.txt"
    
    #load port codes data into dataframe
    df=spark.read.csv(input_data, header=True, sep="=")

    #Create port code table
    port_table = df.select("value", "i94prtl")\
        .withColumnRenamed("value", "port_code")\
        .withColumnRenamed("i94prtl", "port_city")
    
    port_table = port_table.withColumn\
        ('city', \
        split(port_table['port_city'], ',')[0])\
        .withColumn('state', \
        split(port_table['port_city'], ',')[1])
    
    #write port table to parquet file
    port_table.write.parquet(output_data+'port_table.parquet')
    
    #set input path for address city codes
    input_data = code_data + "I94ADDR.txt"

    #Load address city codes data into dataframe
    df=spark.read.csv(input_data, header=True, sep="=")
        
    #Create address city code table
    addr_table = df.select("value", "i94addrl")\
        .withColumnRenamed("value", "addr_code")\
        .withColumnRenamed("i94addrl", "addr_city")
    
    #write address table to parquet file
    addr_table.write.parquet(output_data+'addr_table.parquet')

    #set input path for resident country codes
    input_data = code_data + "I94RES.txt"
       
    #Load address city codes data into dataframe
    df=spark.read.csv(input_data, header=True, sep="=")

    #Create resident country code table
    rescountry_table = df.select("value", "i94cntyl")\
        .withColumn("res_code", df["value"]\
        .cast(IntegerType()))\
        .withColumnRenamed("i94cntyl", "res_country")\
        .drop("value")
    
    #Write resident country table to parquet file
    rescountry_table.write.\
        parquet(output_data+'rescountry_table.parquet')
    
    #set input path for airport data
    input_data = code_data + "airport-codes_csv.csv"
    
    #Create airport table for country US
    df=spark.read.csv(input_data, header=True)
    airport_table = df.select("name", "iso_country", "iso_region", \
        "municipality").filter(df.iso_country == 'US')

    #split region to separate state code and country
    airport_table = airport_table\
        .withColumn('state', split(df.iso_region, '-')[1])
    
    #write airport table to parquet file
    airport_table.write.partitionBy('iso_country')\
        .parquet(output_data+'airport_table.parquet')
    
def main():
    """
    This is main function of this module
    """
    spark = create_spark_session()
    input_data  = "s3a://immi-data/data/"
    code_data   = "s3a://immi-data/code/"
    output_data = "s3a://immi-data/data/outdata/"
    
    process_immigration_data(spark, input_data, output_data)
    process_code_data(spark, code_data, output_data)

if __name__ == "__main__":
    main()