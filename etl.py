import argparse
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
import sys

from sqlalchemy import desc
from yaml import parse


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, mode, input_data, output_data):
    """
    read input data in from S3, transform data by writing it to parquet files

    :param spark: spark session object, used for context and data wrangling
    :type spark: spark.SparkSession
    :param mode: debug/release mode flag, used for testing on smaller data set before running ELT on 
    entirety of song data
    :type mode: bool
    :param input_data: path to S3 resource to extract song data from (file or bucket of files?)
    :type input_data: str
    :param output_data: path to S3 location to write output data to
    :type output_data: str
    """

    # get filepath to song data file
    song_data_path = f"{input_data}/song_data"
    
    # read song data file
    songs_dataset_df = spark.read.format("json").option("recursiveFileLookup", "true").load(song_data_path)

    # extract columns to create songs table
    songs_table_df = songs_dataset_df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates('song_id')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_df.write.parquet(f"{output_data}/songs.parquet")

    # extract columns to create artists table
    artists_table_df = songs_dataset_df.select('artist_id', 'name', 'location', 'latitude', 'longitude').dropDuplicates('artist_id')
    
    # write artists table to parquet files
    # save to output_data which will be a path to a location in S3
    artists_table_df.write.parquet(f"{output_data}/artists.parquet")


def process_log_data(spark, mode, input_data, output_data):
    """
    read input data from S3, transform data by adjusting it and writing it to parquet files

    :param spark: spark session object, used for context and data wrangling
    :type spark: spark.SparkSession
    :param mode: debug/release mode flag, used for testing on smaller data set before running ELT on 
    entirety of log data
    :type mode: bool
    :param input_data: path to S3 resource to extract log data from (file or bucket of files?)
    :type input_data: str
    :param output_data: path to S3 location to write output data to
    :type output_data: str
    """
    # do we need to parallelize the dataframe?
    # sparkcontext.parallelize(data)

    # get filepath to log data file
    log_data_path = f"{input_data}/log_data"

    # read log data file
    log_dataset_df = spark.read.format("json").option("recursiveFileLookup", "true").load(log_data_path)
    
    # filter by actions for song plays
    log_dataset_df = log_dataset_df.filter((log_dataset_df.page == 'NextSong'))

    # extract columns for users table    
    users_table_df = log_dataset_df.select(
        'userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level').dropDuplicates()

    # write users table to parquet files
    users_table_df.write.parquet(f"{output_data}/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: (x.cast('float')/1000).cast("timestamp"))
    #events_df = events_df.withColumn('timestamp', get_timestamp(events_df.ts))
    
    # create datetime column from original timestamp column
    #get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000))
    log_dataset_df = log_dataset_df.withColumn('start_time', get_timestamp(log_dataset_df.ts))
    
    # extract columns to create time table
    log_dataset_df = log_dataset_df.select(
        col('start_time').alias('start_time'),
        hour('start_time').alias('hour'),
        dayofmonth('start_time').alias('day'),
        weekofyear('start_time').alias('week'),
        month('start_time').alias('month'),
        year('start_time').alias('year'),
        date_format(col('start_time'), 'E').alias('weekday')
        )
    time_table_df = log_dataset_df.select('start_time', 'hour', 'day','week', 'month', 'year', 'weekday').dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table_df.write.parquet(f"{output_data}/time.parquet")

    # read in song data to use for songplays table
    #songs_table_df = spark.read.parquet(f'{output_data}/songs.parquet')

    # opt 1) join with spark.sql
    #register log data dataframe as sql temporary view
    log_dataset_df.createOrReplaceTempView("events")
    # register song dataframe as sql temporary view
    #song_dataset_df.createOrReplaceTempView("songs")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = spark.sql("""
    SELECT 
        events.start_time,
        events.userId AS user_id, 
        events.level, 
        songs.song_id, 
        songs.artist_id, 
        events.sessionId as session_id,
        events.location, 
        events.userAgent AS user_agent,
        row_number() AS songplay_id
        year(events.start_time) AS year,
        month(events.start_time) AS month
    FROM events
    JOIN songs ON events.artist = songs.artist_name AND events.song = songs.title;
    """)

    # opt 2) join with dataframe.join
    #songplays_df = events_df.join(
    #    songs_df, (events_df.artist == songs_df.artist_name) and (events_df.song == songs_df.title), "Left").\
    #    select(events_df['start_time'], events_df['userId'], events_df['level'], songs_df['song_id'], songs_df['artist_id'], 
    #    events_df['session_id'], events_df['location'], events_df['user_agent'], events_df['year'], events_df['month'])

    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.partitionBy('year', 'month')\
        .mode('overwrite').parquet(f"{output_data}/songplays.parquet")


def process_data(spark, debug_mode, input_data, output_data):
    """
    read input data from S3, transform data by adjusting it and writing it to parquet files

    :param spark: spark session object, used for context and data wrangling
    :type spark: spark.SparkSession
    :param mode: debug/release mode flag, used for testing on smaller data set before running ELT on 
    entirety of log data
    :type mode: bool
    :param input_data: path to S3 resource to extract song and log data from (file or bucket of files)
    :type input_data: str
    :param output_data: path to S3 location to write output data to
    :type output_data: str
    """
    # get filepath to song data file
    # if in debug mode, choose a smaller dataset for song_data
    if debug_mode:
        song_data_path = f"{input_data}/song_data/A/A/A"
    else:
        song_data_path = f"{input_data}/song_data"
    
    # read song data file
    song_dataset_df = spark.read.format("json").option("recursiveFileLookup", "true").load(song_data_path)

    # extract columns to create songs table
    songs_table_df = song_dataset_df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates('song_id')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_df.write.parquet(f"{output_data}/songs.parquet")

    # extract columns to create artists table
    artists_table_df = song_dataset_df.select('artist_id', 'name', 'location', 'latitude', 'longitude').dropDuplicates('artist_id')
    
    # write artists table to parquet files
    # save to output_data which will be a path to a location in S3
    artists_table_df.write.parquet(f"{output_data}/artists.parquet")
    
    # get filepath to log data file
    log_data_path = f"{input_data}/log_data"

    # read log data file
    log_dataset_df = spark.read.format("json").option("recursiveFileLookup", "true").load(log_data_path)
    
    # filter by actions for song plays
    log_dataset_df = log_dataset_df.filter((log_dataset_df.page == 'NextSong'))

    # extract columns for users table    
    users_table_df = log_dataset_df.select(
        'userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level').dropDuplicates()

    # write users table to parquet files
    users_table_df.write.parquet(f"{output_data}/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: (x.cast('float')/1000).cast("timestamp"))
    #events_df = events_df.withColumn('timestamp', get_timestamp(events_df.ts))
    
    # create datetime column from original timestamp column
    #get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000))
    log_dataset_df = log_dataset_df.withColumn('start_time', get_timestamp(log_dataset_df.ts))
    
    # extract columns to create time table
    log_dataset_df = log_dataset_df.select(
        col('start_time').alias('start_time'),
        hour('start_time').alias('hour'),
        dayofmonth('start_time').alias('day'),
        weekofyear('start_time').alias('week'),
        month('start_time').alias('month'),
        year('start_time').alias('year'),
        date_format(col('start_time'), 'E').alias('weekday')
        )
    time_table_df = log_dataset_df.select('start_time', 'hour', 'day','week', 'month', 'year', 'weekday').dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table_df.write.parquet(f"{output_data}/time.parquet")

    # opt 1) join with spark.sql
    #register log data dataframe as sql temporary view
    log_dataset_df.createOrReplaceTempView("log_dataset")
    # register song dataframe as sql temporary view
    song_dataset_df.createOrReplaceTempView("song_dataset")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table_df = spark.sql("""
    SELECT 
        log_dataset.start_time AS start_time,
        log_dataset.userId AS user_id, 
        log_dataset.level AS level, 
        song_dataset.song_id AS song_id, 
        song_dataset.artist_id AS artist_id, 
        log_dataset.sessionId AS session_id,
        log_dataset.location AS location, 
        log_dataset.userAgent AS user_agent,
        row_number() AS songplay_id
        year(log_dataset.start_time) AS year,
        month(log_dataset.start_time) AS month
    FROM log_dataset
    JOIN song_dataset ON log_dataset.artist = song_dataset.artist_name AND log_dataset.song = song_dataset.title;
    """)

    # opt 2) join with dataframe.join
    #songplays_df = events_df.join(
    #    songs_df, (events_df.artist == songs_df.artist_name) and (events_df.song == songs_df.title), "Left").\
    #    select(events_df['start_time'], events_df['userId'], events_df['level'], songs_df['song_id'], songs_df['artist_id'], 
    #    events_df['session_id'], events_df['location'], events_df['user_agent'], events_df['year'], events_df['month'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table_df.write.partitionBy('year', 'month')\
        .mode('overwrite').parquet(f"{output_data}/songplays.parquet")


def create_songs_table(song_dataset, output_path):
    """
    Creates songs table given songs dataset and path to location to write table to.
    Verifiable stage of ELT. would be nice to be able to verify ELT before running on EMR(song_dataset should be configurable)
    It would also be nice to keep AWS out of it so i don't incur charges for usage (write location should be configurable)
    - GIVEN smaller dataset
    - verify songs table is created correctly

    :song_dataset: pyspark dataframe containing song dataset to create songs table from
    :output_path: location to write songs.parquet to
    """
    # extract columns to create songs table
    songs_table = song_dataset.select(
        'song_id', 
        'title', 
        'artist_id', 
        'year', 
        'duration'
        ).dropDuplicates('song_id')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f"{output_path}/songs.parquet")


def create_artists_table(song_dataset, output_path):
    """
    Creates artists table given songs dataset and path to location to write table to
    Verifiable stage of ELT. would be nice to be able to verify ELT before running on EMR(song_dataset should be configurable)
    It would also be nice to keep AWS out of it so i don't incur charges for usage (write location should be configurable)
    - GIVEN smaller dataset
    - verify songs table is created correctly

    :song_dataset: pyspark dataframe containing song dataset to create songs table from
    :output_path: location to write songs.parquet to
    """
    # extract columns to create artists table
    artists_table_df = song_dataset.select(
        'artist_id', 
        'name', 
        'location', 
        'latitude', 
        'longitude'
        ).dropDuplicates('artist_id')
    
    # write artists table to parquet files
    artists_table_df.write.parquet(f"{output_path}/artists.parquet")


def create_users_table(log_dataset, output_path):
    """
    Creates users table given log dataset and path to location to write table to
    Verifiable stage of ELT. would be nice to be able to verify ELT before running on EMR(song_dataset should be configurable)
    It would also be nice to keep AWS out of it so i don't incur charges for usage (write location should be configurable)
    - GIVEN smaller dataset
    - verify songs table is created correctly

    :song_dataset: pyspark dataframe containing song dataset to create songs table from
    :output_path: location to write songs.parquet to
    """
    # extract columns for users table    
    users_table_df = log_dataset.select(
        'userId as user_id', 
        'firstName as first_name', 
        'lastName as last_name', 
        'gender', 
        'level'
        ).dropDuplicates()

    # write users table to parquet files
    users_table_df.write.parquet(f"{output_path}/users.parquet")


def create_time_table(log_dataset, output_path):
    """
    Creates time table given log dataset and path to location to write table to
    Verifiable stage of ELT. would be nice to be able to verify ELT before running on EMR(song_dataset should be configurable)
    It would also be nice to keep AWS out of it so i don't incur charges for usage (write location should be configurable)
    - GIVEN smaller dataset
    - verify songs table is created correctly

    :song_dataset: pyspark dataframe containing song dataset to create songs table from
    :output_path: location to write songs.parquet to
    """
    
    
    # extract columns to create time table
    log_dataset = log_dataset.select(
        col('start_time').alias('start_time'),
        hour('start_time').alias('hour'),
        dayofmonth('start_time').alias('day'),
        weekofyear('start_time').alias('week'),
        month('start_time').alias('month'),
        year('start_time').alias('year'),
        date_format(col('start_time'), 'E').alias('weekday')
        )
    time_table_df = log_dataset.select(
        'start_time', 
        'hour', 
        'day',
        'week', 
        'month', 
        'year', 
        'weekday'
        ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table_df.write.parquet(f"{output_path}/time.parquet")


def create_songplays_table(spark, song_dataset, log_dataset, output_path):
    """
    Creates songplays table given song dataset, log dataset and path to location to write table to
    Verifiable stage of ELT. would be nice to be able to verify ELT before running on EMR(song_dataset should be configurable)
    It would also be nice to keep AWS out of it so i don't incur charges for usage (write location should be configurable)
    - GIVEN smaller dataset
    - verify songs table is created correctly

    :song_dataset: pyspark dataframe containing song dataset to create songs table from
    :output_path: location to write songs.parquet to
    """
    #register log data dataframe as sql temporary view
    log_dataset.createOrReplaceTempView("log_dataset")
    # register song dataframe as sql temporary view
    song_dataset.createOrReplaceTempView("song_dataset")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table_df = spark.sql("""
    SELECT 
        log_dataset.start_time AS start_time,
        log_dataset.userId AS user_id, 
        log_dataset.level AS level, 
        song_dataset.song_id AS song_id, 
        song_dataset.artist_id AS artist_id, 
        log_dataset.sessionId AS session_id,
        log_dataset.location AS location, 
        log_dataset.userAgent AS user_agent,
        row_number() AS songplay_id
        year(log_dataset.start_time) AS year,
        month(log_dataset.start_time) AS month
    FROM log_dataset
    JOIN song_dataset ON log_dataset.artist = song_dataset.artist_name AND log_dataset.song = song_dataset.title;
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_df.write.partitionBy('year', 'month')\
        .mode('overwrite').parquet(f"{output_path}/songplays.parquet")


def process_data_v2(spark, input_data, output_data):
    """
    """
    song_data_path = f"{input_data}/song_data"
    
    # read song data file
    song_dataset = spark.read.format("json").option("recursiveFileLookup", "true").load(song_data_path)

    create_songs_table(song_dataset, output_data)

    create_artists_table(song_dataset, output_data)
    
    # get filepath to log data file
    log_data_path = f"{input_data}/log_data"

    # read log data file
    log_dataset = spark.read.format("json").option("recursiveFileLookup", "true").load(log_data_path)
    
    # filter by actions for song plays
    log_dataset = log_dataset.filter((log_dataset.page == 'NextSong'))

    create_users_table(log_dataset, output_data)

    # create start_time column from original ts column
    # time table and songplays table both need start_time for their schema
    get_timestamp = udf(lambda x: (x.cast('float')/1000).cast("timestamp"))
    log_dataset = log_dataset.withColumn('start_time', get_timestamp(log_dataset.ts))

    create_time_table(log_dataset, output_data)

    create_songplays_table(spark, song_dataset, log_dataset, output_data)

def main():
    """
    Entry point for etl.py
    """
    input_data = config['AWS']['INPUT_DATA']
    output_data = config['AWS']['OUTPUT_DATA']
    #debug_mode = config.getboolean['DEBUG_MODE']

    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3://jblackman-bucket-1/project-data-lake/"
    
    # template formerly had two methods for processing data
    # with the need for loading the song dataset in both methods, 
    # i don't think there's a reason to have two separate methods,
    # especially since loading the song data set seems computationally expensive

    process_data_v2(spark, input_data, output_data)
    #process_song_data(spark, input_data, output_data)
    #process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
