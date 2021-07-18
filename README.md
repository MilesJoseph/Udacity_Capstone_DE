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

  I chose to programatically spin up a cluster within the body of my dag. You can use Livy, but I found this to be a little cumbersome and the API a little clumsy.

  Utilizing EMR is pretty easy in the airflow environment as there is a robust library that allows the DAG 'dag_cluster' to be broken down into four distinct steps;

                - creating a cluster

                - running Spark steps

                - checking that the steps are complete

                - termintating the cluster


  if test 
