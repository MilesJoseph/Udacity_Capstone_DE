## Capstone Project

  This repo is for the final project in the data engineering nano degree for Udacity. This repo is the culmination of what we have learned during the course. I am choosing to complete the project with the data that is being provided by Udacity.

## Project Parameters

  This project is based on the assumption that this data would be inserted into an s3 bucket at regular intervals/real time.

## Project Goals

  I will build a data lake on s3 that can be used to query weather and demographics of popular immigration destinations. Prospective immigrants could use this to determine where they might want to live. This data could also be used by any persons or companies that might look to invest in these locations based on the variables available. I am going to limit the data to the U.S.

## Data Sources

  * I94 Immigration Data: This data comes from the US National Tourism and Trade Office. This data records immigration records partitioned by month of every year.

  * World temperature Data: This dataset comes from a Kaggle Source. Includes temperature recordings of cities around the world for a period of time

  * US City Demographic Data: This dataset comes Udacity. This includes data pertaining to city demographics.

  * Aiport Code table: Source. Includes a collection of airport codes and their respective cities, countries around the world.

## AWS Setup

  I chose to use the recently implemented Managed Workflows Apache Airflow. This is recently developed by the AWS team and relatively easy to use.

  You first have to set up your connections in the admin portion of the MWAA as typical in any other airflow setup.

  I chose to programatically spin up an emr cluster within the body of my dag to do all of my ETL work. You can use Livy, but I found this to be a little cumbersome and the API a little clumsy.

  Utilizing EMR is pretty easy in the airflow environment as there is a robust library that allows the DAG 'dag_cluster' to be broken down into four distinct steps;

                - creating a cluster

                - running Spark steps

                - checking that the steps are complete

                - terminating the cluster


## Data Schema

  I decided to keep my schema in s3.  s3 also is easy to use, has lower costs than other microservices such a redshift and can be faster since we are writing to parquet files. In addition, many users can access s3 buckets, which is particularly helpful when we consider the context of the data, i.e. for public consumption.

    - Anyone with the knowledge and access to the publicly available s3 bucket perform analytics or queries on the data. However, they would need to have the technical understanding to read the parquet files. Ideally, a business intelligence analyst could provide a web UI with embedded dashboards for the people looking to travel or visit U.S. locations.

    - Some metrics that could be either queried or displayed in a dashboard could include;
```python
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
        .show()>`
```
## Table Designs

1. US city - built on the codes from airport and demographics
2. US Airport- built using airport data and filtered for the US.
3. Weather - built using the global weather data, filtered for US as well.
4. US Demographics - built on demographics data.
5. Airport Weather (Denorm) - joining weather data with airport location.
6. Immigrant - Info about individual immigrants, gender, etc.
7. immigration demo (denorm) - info about demographics joined with immigration data to get info about specific cities.

## Considerations

  We would most likely want to write the spin up in such a way that the EMR cluster could increase should the size of the data increase.

## Scenario

  You could run the pipeline whenever you want, at a daily cadence with an email notification for failure. This would only happen after a predetermined number of retries.

  Another thing we would write in order for this to be considered production ready would be to write quality checks. If a check were to fail then it would notifiy the admin.

## ETL

![](assets/README-0b071138.png)

## Attributes

  I found a very helpful article by Gary A. Stafford ' Running Spark Jobs on Amazon EMR with Apache Airflow'. This artcle is great for learning how to spin up a cluster programatically.

  https://itnext.io/running-spark-jobs-on-amazon-emr-with-apache-airflow-2e16647fea0c
