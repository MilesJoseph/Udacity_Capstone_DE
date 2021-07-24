from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import *
import math
from datetime import datetime, timedelta

spark = SparkSession \
        .builder \
        .appName("transforms") \
        .getOrCreate()


immigrant = spark.read.parquet("s3://capstone-mk/immigrant/")\
                      .filter("i94_dt = '{}'".format(month_year))

city = spark.read.parquet("s3://de-capstone/lake/city/")
demo = spark.read.parquet("s3://capstone-mk/us_cities_demographics/")\
            .select("median_age", "city_id", "total_population", "foreign_born")\
            .join(city.select("state_code", "city_id"), "city_id")\
            .drop("city_id")\
            .groupBy("state_code").agg(F.mean("median_age").alias("median_age"),
                                       F.sum("total_population").alias("total_population"),
                                       F.sum("foreign_born").alias("foreign_born"))


immigration_demographic = immigrant.select("cicid", "from_country_code", "age", "occupation", "gender", 'i94_dt')

immigration_demographic = immigration_demographic.join(demo, "state_code").drop("state_code")
immigration_demographic.write.partitionBy("i94_dt").mode("append")\
                       .parquet("s3://de-capstone/lake/immigration_demographic/")
