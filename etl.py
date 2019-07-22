#%%
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *

#%%
# LOCAL CONNECTION

# def create_spark_session():
#     spark = SparkSession.builder \
#         .config("spark.jars.packages", "JohnSnowLabs:spark-nlp:1.8.2") \
#         .getOrCreate()
    
#     return spark


# AWS CONNECTION

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    '''
        Creates Apache Spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
  
    return spark

#%%
def process_song_data(spark, input_data, output_data):
    '''
        Files in song_data are processed and loaded into the following tables: songs_table, artists_table
    '''

    # Table schema for song_data files
    song_data_schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("year", IntegerType(), True)
    ])


    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"  

    # read song data file
    df = spark.read.json(song_data, schema=song_data_schema)

    print("SONG_DATA LOADED")

    # extract columns to create songs table
    songs_table = df.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "song_table")

    print("SONGS_TABLE DONE")

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude"))

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table")

    print("ARTISTS_TABLE DONE")

def process_log_data(spark, input_data, output_data):
    '''
        Files in log_data are processed and loaded into the following tables: users_table, time_table
        FIles in both song_data and log_data are merged and then loaded into the following table: songplays_table
    '''

    # Table schema for log_data files
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data fil
    df = spark.read.json(log_data, schema=log_data_schema)
    
    print("LOG_DATA LOADED")

    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # extract columns for users table    
    users_table = df.select(
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        "gender",
        "level")
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table")

    print("USERS_TABLE DONE")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # Create datetime column from original timeestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        col("timestamp").alias("start_time"),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'F').alias('weekday')
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table")

    print("TIME_TABLE DONE")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    print("SONG_DATA LOADED")

    # merge song_data and log_data
    merged_df = df.join(song_df, df.artist == song_df.artist_name, 'inner')

    print("SONG_DATA & LOG_DATA MERGED")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = merged_df.select(
        col("ts").alias("start_time"),
        col("userId").alias("user_id"), 
        "level", 
        "song_id",
        "artist_id", 
        col("sessionId").alias("session_id"),
        "location", 
        col("userAgent").alias("user_agent"),
        month("datetime").alias('month'),
        year("datetime").alias('year'))

    # Add serialize column as songplay_id
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id() + 1).select("songplay_id", *songplays_table.columns)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table")

    print("SONGPLAYS_TABLE DONE")

#%%
def main():
    spark = create_spark_session()

    print("CONNECTED")

    # AWS
    input_data = "s3a://udacity-dend/"                # Insert local path for local environment
    output_data = ""                                  # Insert path where output_data will be saved

    process_song_data(spark, input_data, output_data)  
    print("SONG DATA PROCESS COMPLETE")

    process_log_data(spark, input_data, output_data)
    print("LOG DATA PROCESS COMPLETE")

    print("COMPLETE")

#%%  
if __name__ == "__main__":
    main()

#%%
