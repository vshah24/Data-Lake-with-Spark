# Data-Lake-with-Spark

## Overview
A startup Sparkify(music streaming company),over the past few months has grown exponentially and want to move their data warehouse to a data lake.

As a data engineer, we will build an ETL pipeline as follow:
   * **Extract** - We will extract data from Amazon s3 bucket.
   *   **Transform** - We will processes/transform the data using Spark .
   *   **Load** - We will load the processed data to Amazon S3 bucket as a set of dimensional table. 
  
This will allow the analytics team to continue finding insights in what songs their users are listening to.

## Technologies Used
Python,Spark,SQL,Amazon S3 and Data Lake


## Dataset
Dataset used in the project are provided by two public S3 buckets. One bucket contains information about songs and artist. The other bucket contains log information i.e. information about the users.


## Project Files
1. etl.py - The Python script is an ETL that is responsible for extracting data (json files) from AWS S3, processing the data using Spark and writing data back to AWS S3 as Spark parquet files.
2. dl.cfg - It's a configuration file that stores information about our AWS credentials (Access Key Id and Secret Access Key).
3. etl_prototype.ipynb - A notebook which was used for a trial run using sample data. This trial run writes the output parquet files into the local machine. If this notebook is running successfully then we can run our etl.py script.
