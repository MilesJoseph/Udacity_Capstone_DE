from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf

spark = SparkSession \
        .builder \
        .appName("data_qaulity_check") \
        .getOrCreate()

Logger= spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger("DAG")
spark.sparkContext.setLogLevel('WARN')


def check(path, table):
    df = spark.read.parquet(path)
    if len(df.columns) > 0 and df.count() > 0:
        mylogger.warn("{} SUCCESS".format(table))
    else:
        mylogger.warn("{} FAIL".format(table))



check("s3a://capstone-mk/city/", "city")
check("s3a://capstone-mk/lake/codes/state_code/", "state_code")
check("s3a://capstone-mk/lake/codes/country_code/", "country_code")
check("s3a://capstone-mk/lake/codes/airport_code/", "airport_code")
check("s3a://capstone-mk/lake/us_cities_demographics/", "us_cities_demographics")
check("s3a://capstone-mk/lake/us_cities_temperatures/", "us_cities_temperatures")
check("s3a://capstone-mk/lake/us_airports_weather/", "us_airport_weather")
