"""ETLs JSON files in S3 using Spark to model Sparkify database.

1) Extracts JSON files representing song and event data from S3.
2) Transforms the data using Spark to create fact and dimension tables.
fact: songplays
dimensions: songs, artists, users, time
3) Loads the tables into Parquet files and stores them back into S3.
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.types import LongType, TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session() -> None:
    """Connects to spark cluster on AWS"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """Creates song and artist dimension tables from song_data directory.
    Tables are saved as parquet files.

    Args:
        spark: a session to interact with spark
        input_data: filepath to read data from
        output_data: filepath to output the data
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*')

    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id",
                            "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    (songs_table
     .write
     .mode("overwrite")
     .partitionBy("year", "artist_id")
     .parquet(os.path.join(output_data, "songs")))

    # extract columns to create artists table
    artists_table = (df
                     .select("artist_id",
                             col("artist_name").alias("name"),
                             col("artist_location").alias("location"),
                             col("artist_latitude").alias("latitude"),
                             col("artist_longitude").alias("longitude"))
                     .dropDuplicates())

    # write artists table to parquet files
    (artists_table
     .write
     .mode("overwrite")
     .parquet(os.path.join(output_data, "artists")))


def process_log_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """Creates users and time dimension tables from log-data directory, and
    also creates the songplays fact table from all available dimension tables.
    All tables are saved in parquet files.

    Args:
        spark: a session to interact with spark
        input_data: filepath to read data from
        output_data: filepath to output the data
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log-data/*/*/*")

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = (df
                   .select(col("userId").alias("user_id"),
                           col("firstName").alias("first_name"),
                           col("lastName").alias("last_name"),
                           "gender",
                           "level")
                   .dropDuplicates())

    # write users table to parquet files
    (users_table
     .write
     .mode("overwrite")
     .parquet(os.path.join(output_data, "users")))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x // 1000, LongType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.utcfromtimestamp(x), TimestampType())
    df = df.withColumn("datetime_temp", get_datetime("start_time"))

    # extract columns to create time table
    time_table = (df
                  .withColumn("hour", hour("datetime_temp"))
                  .withColumn("day", dayofmonth("datetime_temp"))
                  .withColumn("week", weekofyear("datetime_temp"))
                  .withColumn("month", month("datetime_temp"))
                  .withColumn("year", year("datetime_temp"))
                  .withColumn("weekday", dayofweek("datetime_temp"))
                  .select("start_time", "hour", "day", "week", "month", "year", "weekday")
                  .dropDuplicates())

    # write time table to parquet files partitioned by year and month
    (time_table
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(os.path.join(output_data, "time")))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs"))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = (df
                       .join(song_df,
                             (df.song == song_df.title) &
                             (df.length == song_df.duration))
                       .join(time_table, df.start_time == time_table.start_time)
                       .select(df.start_time,
                               df.userId.alias("user_id"),
                               df.level,
                               song_df.song_id,
                               song_df.artist_id,
                               df.sessionId.alias("session_id"),
                               df.location,
                               df.userAgent.alias("user_agent"),
                               time_table.year,
                               time_table.month)
                       .dropDuplicates())

    create_ids = row_number().over(Window.partitionBy().orderBy("start_time"))
    songplays_table = songplays_table.withColumn("songplay_id", create_ids)

    # write songplays table to parquet files partitioned by year and month
    (songplays_table
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(os.path.join(output_data, "songplays")))


def main() -> None:
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
