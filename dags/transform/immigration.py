from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import *
import math
from datetime import datetime, timedelta
import sys
from pyspark.sql import SparkSession



spark = SparkSession \
        .builder \
        .appName("transforms") \
        .getOrCreate()

immigrant = spark.read.format('com.github.saurfang.sas.spark')\
                 .load('s3://capstone-mk/i94_immigration_data/i94_{}_sub.sas7bdat'.format(month_year))\
                 .selectExpr('cast(cicid as int) AS cicid', 'cast(i94res as int) AS from_country_code',
                             'cast(i94bir as int) AS age', 'cast(i94visa as int) AS visa_code',
                             'visapost AS visa_post', 'occup AS occupation',
                             'visatype AS visa_type', 'cast(biryear as int) AS birth_year', 'gender')\
                 .withColumn("i94_dt", F.lit(month_year))
#
immigrant.write.partitionBy("i94_dt").mode("append").parquet("s3://capstone-mk/immigrant/")
