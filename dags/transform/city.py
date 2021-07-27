from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession



spark = SparkSession \
        .builder \
        .appName("transforms") \
        .getOrCreate()

def parse_state(x):
    '''strips hyphen'''
    return x.strip().split('-')[-1]
udf_parse_state = udf(lambda x: parse_state(x), StringType())

#
demo = spark.read.format('csv').load('s3://capstone-mk/us-cities-demographics.csv', header=True, inferSchema=True, sep=';')\
                .select("State Code", "City")\
                .withColumnRenamed("State Code", "state_code")\
                .withColumnRenamed("City", "city")

#
us_airport = spark.read.format('csv').load('s3://capstone-mk/airport-code_csv.csv', header=True, inferSchema=True)\
                        .filter("iso_country = 'US'")\
                        .withColumn("state", udf_parse_state("iso_region"))\
                        .selectExpr("municipality AS city", "state AS state_code")

#
city = us_airport.union(demo)\
                 .drop_duplicates()\
                 .withColumn("city_id", F.monotonically_increasing_id())

#
city.write.mode("overwrite").parquet("s3://capstone-mk/city/")
