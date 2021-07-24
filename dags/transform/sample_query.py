from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession \
        .builder \
        .appName("transforms") \
        .getOrCreate()

immig_demo = spark.read.parquet("s3;//capstone-mk/lake/immigration_demographic")

immig_demo.select('city_id', 'mediang_age', 'total_population', 'foreign_born') \
    .where(col('state_id') == 'Illinois') \
    .show()
