from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("transforms") \
        .getOrCreate()


def parse_lat(x):
    '''gathers float value before , '''
    y = x.strip().split(',')
    return float(y[0])
udf_parse_lat = udf(lambda x: parse_lat(x), FloatType())

def parse_long(x):
    '''gathers float value after ,'''
    y = x.strip().split(',')
    return float(y[1])
udf_parse_long = udf(lambda x: parse_long(x), FloatType())

def parse_state(x):
    '''gathers state value after hyphen'''
    return x.strip().split('-')[-1]
udf_parse_state = udf(lambda x: parse_state(x), StringType())

#
city = spark.read.parquet("s3://capstone-mk/lake/city/")
us_airport = spark.read.format('csv').load('s3://capstone-mk/airport-codes_csv.csv', header=True, inferSchema=True)\
                        .withColumn("airport_latitude", udf_parse_lat("coordinates"))\
                        .withColumn("airport_longitude", udf_parse_long("coordinates"))\
                        .filter("iso_country = 'US'")\
                        .withColumn("state", udf_parse_state("iso_region"))\
                        .withColumnRenamed("ident", "icao_code")\
                        .drop("coordinates", "gps_code", "local_code", "continent",
                                "iso_region", "iso_country")
us_airport = us_airport.join(city, (us_airport.municipality==city.city) & (us_airport.state==city.state_code), "left")\
                        .drop("municipality", "state", "city", "state_code")
us_airport.write.mode("overwrite").parquet("s3://capstone-mk/airport_code/")
