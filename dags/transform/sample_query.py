from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import *


spark = SparkSession \
        .builder \
        .appName("transforms") \
        .getOrCreate()


city = spark.read.parquet("s3://de-capstone/lake/city/")
demo = spark.read.parquet("s3://de-capstone/lake/immigration_demographic/")

demo.select("city_id","median_age", "total_population", "foreign_born")\
    .join(city.select("state_code", "city_id", "city"), "city_id")\
    .where(col('state_code',)=='IL')\
    .drop("city_id")\
    .groupBy('city').agg(F.mean("median_age").alias("median_age"),\
                     F.mean("total_population").alias("total_population"),\
                     F.mean("foreign_born").alias("foreign_born"))\
    .orderBy('median_age')\
    .show()
