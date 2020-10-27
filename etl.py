import configparser
from datetime import datetime, date
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

import numpy as np
import pandas as pd

config = configparser.ConfigParser()
config.read('dl.cfg')


os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
spark = SparkSession \
        .builder \
        .appName("sparkify") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    df = spark.read.json(song_data)

    songs_table = df.select('song_id', 
                        'title', 
                        'artist_id', 
                        'year', 
                        'duration')

    
    songs_table.write.parquet(os.path.join(output_path, '/songs'), 
                              mode='overwrite', 
                              partitionBy=('year', 'artist_id'))

    
    artists_table = df.selectExpr('artist_id as id', 
                                'artist_name as name', 
                                'artist_location as location', 
                                'artist_latitude as latitude', 
                                'artist_longitude as longitude')
    artist_table.write.parquet(os.path.join(output_path, '/artists'), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    log_data = os.path.join(input_data, '/log_data/*.json')
    song_data = os.path.join(input_data, '/song_data/*/*/*/*.json')
    
    # Spark SQL
    df = spark.read.json(log_data)
    df = spark.sql("""
            SELECT * 
            FROM log_data 
            WHERE page = 'NextSong'
            """)
    df.createTempView('log_data')
    
    df_song = spark.read.json(song_data)
    df_song.createTempView('song_data')

    # User table
    users_table = df.selectExpr('userId as user_id', 
                                'firstName as first_name', 
                                'lastName as last_name', 
                                'gender', 
                                'level')
    
    users_table.write.parquet(os.path.join(output_path, '/users'), mode='overwrite')

    # Time Table
    get_timestamp = udf(lambda ts: datetime.datetime.fromtimestamp(ts/1000.0), TimestampType())
    new_table_datetime_converted = df.withColumn('ts', get_timestamp(df.ts)).fillna(0)
    
    spark.udf.register('get_timestamp', get_timestamp)
    
    time_table = new_table_datetime_converted.selectExpr('ts as start_time', 
                                                     'extract(hour from ts) as hour',
                                                     'extract(day from ts) as day', 
                                                     'weekofyear(ts) as week', 
                                                     'extract(month from ts) as month', 
                                                     'extract(year from ts) as year', 
                                                     'weekday(ts) as weekday')
    
    time_table.write.parquet(os.path.join(output_path, 'times'), partitionBy=('year', 'month'))

    # Songsplay Table
    songplay_table = spark.sql("""
            SELECT ts as start_time, userId as user_id, level, song, sessionId as session_id, 
            location, userAgent as user_agent, year(get_timestamp(ts)) as year, month(get_timestamp(ts)) as month
            FROM log_data
            JOIN songs_table
            ON log_data.artist == songs_table.artist_name
            AND log_data.song == songs_table.title
            AND log_data.length == songs_table.duration
        """).withColumn('songplay_id', monotonically_increasing_id())
    
    songplays_table.write.parquet(output_data, mode='overwrite', partitionBy=('year', 'month'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

