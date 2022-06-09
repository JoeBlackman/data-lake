from datetime import datetime
import pathlib
import pyspark.sql.functions as F
import sys

test_dir = pathlib.Path(__file__).parent.resolve()
project_dir = test_dir.parent.resolve()
sys.path.insert(0, str(project_dir))
print(sys.path)

from etl import create_songs_table, create_artists_table, create_users_table, create_time_table, create_songplays_table

song_data_path = f'{test_dir}/data/song-data'
log_data_path = f'{test_dir}/data/log-data'
output_path = f'{test_dir}/data/data_lake'

def test_create_songs_table(spark, clear_data_lake):
    # load song dataset
    song_dataset = spark.read.format("json").option("recursiveFileLookup", "true").load(song_data_path)
    # create songs table
    create_songs_table(song_dataset, output_path)
    # assertions
    # column equality?
    songs_table = spark.read.parquet(f"{output_path}/songs.parquet")
    for col in songs_table.columns:
        assert col in ['song_id', 'title', 'artist_id', 'year', 'duration']
    # to test quality of data being inserted, I'd need more control over dataset
    # use a contrived dataset with known edge cases to see if they get handled
    # for now, i'd prefer to just see that each create_table function writes correctly
    # almost easier to read parquet in notebook and visually inspect it


def test_create_artists_table(spark, clear_data_lake):
    song_dataset = spark.read.format("json").option("recursiveFileLookup", "true").load(song_data_path)
    create_artists_table(song_dataset, output_path)
    artists_table = spark.read.parquet(f"{output_path}/artists.parquet")
    for col in artists_table.columns:
        assert col in ['artist_id', 'name', 'location', 'latitude', 'longitude']


def test_create_users_table(spark, clear_data_lake):
    log_dataset = spark.read.format("json").option("recursiveFileLookup", "true").load(log_data_path)
    log_dataset = log_dataset.filter((log_dataset.page == 'NextSong'))
    create_users_table(log_dataset, output_path)
    users_table = spark.read.parquet(f"{output_path}/users.parquet")
    for col in users_table.columns:
        assert col in ['user_id', 'first_name', 'last_name', 'gender', 'level']


def test_create_time_table(spark, clear_data_lake):
    log_dataset = spark.read.format("json").option("recursiveFileLookup", "true").load(log_data_path)
    log_dataset = log_dataset.filter((log_dataset.page == 'NextSong'))
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S.%f'))
    log_dataset = log_dataset.withColumn('start_time', get_timestamp(log_dataset.ts))
    create_time_table(log_dataset, output_path)
    time_table = spark.read.parquet(f"{output_path}/time.parquet")
    for col in time_table.columns:
        assert col in ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']


def test_create_songplays_table(spark, clear_data_lake):
    song_dataset = spark.read.format("json").option("recursiveFileLookup", "true").load(song_data_path)
    log_dataset = spark.read.format("json").option("recursiveFileLookup", "true").load(log_data_path)
    log_dataset = log_dataset.filter((log_dataset.page == 'NextSong'))
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S.%f'))
    log_dataset = log_dataset.withColumn('start_time', get_timestamp(log_dataset.ts))
    create_songplays_table(spark, song_dataset, log_dataset, output_path)
    songplays_table = spark.read.parquet(f"{output_path}/songplays.parquet")
    for col in songplays_table.columns:
        assert col in ['start_time', 'user_id', 'level', 'song_id', 'artist_id',
        'session_id', 'location', 'user_agent', 'songplay_id', 'year', 'month']
