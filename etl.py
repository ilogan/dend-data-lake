import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import LongType, TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
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
    artists_table = df.select("artist_id", "artist_name", "artist_location",
                              "artist_latitude", "artist_longitude").dropDuplicates()

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
    log_data = os.path.join(input_data, "log-data/*")

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select("userId", "firstName",
                            "lastName", "gender", "level").dropDuplicates()

    # write users table to parquet files
    (users_table
     .write
     .mode("overwrite")
     .parquet(os.path.join(output_data, "users")))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x // 1000, LongType())
    df = (df
          .withColumn("start_time", get_timestamp("ts"))
          .select("start_time")
          .dropDuplicates())

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
                  .drop("datetime_temp")
                  )

    # write time table to parquet files partitioned by year and month
    (time_table
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(os.path.join(output_data, "time")))

    # read in song data to use for songplays table
    song_df =

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
