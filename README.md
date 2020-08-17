# Data-Lake-with-Spark

## Overview
A startup Sparkify(music streaming company),over the past few months has grown exponentially and want to move their data warehouse to a data lake.

As a data engineer, we will build an ETL pipeline as follow:
  * **Extract** - We will extract data from Amazon s3 bucket.
  * **Transform** - We will processes/transform the data using Spark .
  * **Load** - We will load the processed data to Amazon S3 bucket as a set of dimensional table. 
  
This will allow the analytics team to continue finding insights in what songs their users are listening to.

## Technologies Used
Python,Spark,SQL,Amazon S3 and Data Lake


## Dataset
Dataset used in the project are provided by two public S3 buckets. One bucket contains information about songs and artist. The other bucket contains log information i.e. information about the users.

## Data Lake Schema
 
#### Fact Tables
1. **Songplays** - records in log data associated with song plays i.e. records with page `Next Song`
  
  ```songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent```

#### Dimension Tables
2. **users** - users in the app

    ```user_id, first_name, last_name, gender, level```
    
3. **songs** - songs in music database

    ```song_id, title, artist_id, year, duration```
    
4. **artists** - artists in music database

   ``` artist_id, name, location, latitude, longitude```
 
5. **time** - timestamps of records in songplays broken down into specific units

    ```start_time, hour, day, week, month, year, weekday```


## Project Files
* etl.py - The Python script is an ETL that is responsible for extracting data (json files) from AWS S3, processing the data using Spark and writing data back to AWS S3 as Spark parquet files.
* dl.cfg - It's a configuration file that stores information about our AWS credentials (Access Key Id and Secret Access Key).
* etl_prototype.ipynb - A notebook which was used for a trial run using sample data. This trial run writes the output parquet files into the local machine. If this notebook is running successfully then we can run our etl.py script.
