from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("transforms") \
        .getOrCreate()


us_airport = spark.read.parquet("s3://capstone-mk/airport_code/")
city = spark.read.parquet("s3://capstone-mk/city/")
us_wea = spark.read.parquet("s3://capstone-mk/us_cities_temperatures/")


airport_weather = us_airport.select("name", "elevation_ft", "city_id")\
                            .join(city.select("city", "city_id", "state_code"), "city_id", "left")
airport_weather = airport_weather.join(us_wea, "city_id", "inner").drop("city_id")
airport_weather.write.mode("overwrite").parquet("s3://capstone-mk/us_airports_weather/")
