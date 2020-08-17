import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import time


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """ This function creates a spark session for us
        .config file helps us in retrieving and pushing data to s3
    """
    
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark







def process_song_data(spark, input_data, output_data):
    
    """ETL for songs and artist table.
    
    Arguments:
    spark(object): spark object
    input_data : Location of s3 bucket from where we read the data
    output_data : Location of s3 bucket where we will load our data after transformation as parquet table.
    
    Returns:
    Loads dimensional to s3
    """
    
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    start = time.time()
    df = spark.read.json(song_data)
    end = time.time()
    print('The time taken to Read song data from s3 is :',(end-start))

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    start = time.time()
    songs_table = songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs/")     
    end = time.time()
    print('The time taken to write song table as parquet file to s3 is :',(end-start))
    
    
    
    # extract columns to create artists table
    artists_table = df.select("artist_id",
                              col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_longitude").alias("longitude"),
                              col("artist_latitude").alias("latitude")
                             ).dropDuplicates()
    
    # write artists table to parquet files
    start = time.time()
    artists_table = artists_table.write.mode("overwrite").parquet(output_data + "artists/")
    end = time.time()
    print('The time taken to write artist table as parquet file to s3 is :',(end-start))

    
    
    
    
    
    
    

def process_log_data(spark, input_data, output_data):
    
    
     """ETL for user,time and songplays table.
    
    Arguments:
    spark(object): spark object
    input_data : Location of s3 bucket from where we read the data
    output_data : Location of s3 bucket where we will load our data after transformation as parquet table.
    
    Returns:
    Loads dimensional and facts table to s3
    """
    
    
    # get filepath to log data file . If we want to see the files inside this path we had studied in previous chapter how to get them.
    log_data = input_data + "log_data/*/*/*.json"
    
    
    # read log data file
    start = time.time()
    df = spark.read.json(log_data)
    end = time.time()
    print('The time taken to Read song data from s3 is :',(end-start))
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    
    
    
    
    # extract columns for users table    
    user_table = df.select(col("userId").alias("user_id"),
                           col("firstname").alias("first_name"),
                           col("lastname").alias("last_name"),
                           "gender",
                           "level"
                          ).dropDuplicates()
    
    # write users table to parquet files
    start = time.time()
    user_table =  user_table.write.mode("overwrite").parquet(output_data + "users/")
    end = time.time()
    print('The time taken to write user table as parquet file to s3 is :',(end-start))

    
    
    
    # create timestamp column from original timestamp column
    from pyspark.sql.types import TimestampType
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000),TimestampType())
    df = df.withColumn("timestamp",get_timestamp(df.ts))
    
    # extract columns to create time table
    import pyspark.sql.functions as F
    time_table = df.select(col("timestamp").alias("start_time"),
                           F.hour("timestamp").alias("hour"),
                           date_format(col("timestamp"),"E").alias("day"),
                           F.weekofyear("timestamp").alias("week"),
                           F.month("timestamp").alias("month"),
                           F.year("timestamp").alias("year")
                          ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    start = time.time()
    time_table = time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"time/") 
    end = time.time()
    print('The time taken to write time table as parquet file to s3 is :',(end-start))

    
    
    

    # read in song data to use for songplays table  #Read the parquet file stored in s3
    song_df = spark.read.parquet(output_data+ "songs/")

    #Create temporary table that we will use in our sql query below to fetch songplays 
    df.createOrReplaceTempView("log_table")
    songs_table.createOrReplaceTempView("song_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                                SELECT  
                                    l.timestamp as start_time,
                                    l.userId as user_id,
                                    l.level,
                                    s.song_id,
                                    s.artist_id,
                                    l.sessionId as session_id,
                                    l.location,
                                    l.useragent as user_agent,
                                    year(l.timestamp) as year,
                                    month(l.timestamp) as month

                                FROM 
                                    log_table as l 
                                INNER JOIN
                                    song_table as s
                                ON 
                                    s.duration=l.length and s.title=l.song and s.artist_name=l.artist
                                    ''')

    # write songplays table to parquet files partitioned by year and month
    start = time.time()
    songplays_table = songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays/") 
    end = time.time()
    print('The time taken to write songplays table as parquet file to s3 is :',(end-start))
    
    
    
    
    
    
    

def main():
    spark = create_spark_session()
    input_data =  "s3a://udacity-dend/"
    output_data = "s3a://datalake-spark-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
