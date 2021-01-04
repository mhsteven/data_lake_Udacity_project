import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, to_timestamp, monotonically_increasing_id, round
from pyspark.sql.types import DateType, TimestampType

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


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song-data-sample/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data-sample/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType() )
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
#    get_datetime = udf(lambda x: datetime(x / 1000) if x is not None else None, DateType())
#    df = df.withColumn("datetime", get_datetime(col("ts")))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")).withColumn("day", dayofmonth("start_time")).withColumn("week", weekofyear("start_time")).withColumn("month", month("start_time")).withColumn("year", year("start_time")).withColumn("weekday", dayofweek("start_time")).select("start_time", "hour","day","week","month","year","weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, [round(song_df.duration, 0) == round(df.length, 0), song_df.title == df.song]).withColumn("songplay_id", monotonically_increasing_id()).withColumn("month", month("start_time")).withColumn("year", year("start_time")).select("songplay_id","start_time","userId","level","song_id", "artist_id", "sessionId", "location", "userAgent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays/")


def main():
    spark = create_spark_session()
    input_data = "s3a://aws-emr-resources-369751981827-us-west-2/data_lake_project/"  # "s3a://udacity-dend/"
    output_data ="s3a://aws-emr-resources-369751981827-us-west-2/data_lake_project/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
