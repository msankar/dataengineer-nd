import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import monotonically_increasing_id
import logging

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# REF: https://docs.python.org/3/howto/logging.html
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s  [%(name)s] %(message)s')
LOG = logging.getLogger('datalake_etl')

def create_spark_session():
    """ 
    Create and return a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("Data Lake Sparkify") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Create song and artists table by staging songs data from S3.
    Extracted data is saved to S3.
    
    Args:
        spark: Spark session
        input_data: S3 location of songs json files
        output_data: S3 location where extracted data is written to.
        
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    LOG.info('Reading song data from ' + song_data)
    df = spark.read.json(song_data)

    # Stage song data
    LOG.info('Staging song data')
    df.createOrReplaceTempView("staging_song")
    
    # extract columns to create songs table
    LOG.info('Create song table from staged table')
    songs_table = spark.sql("""
        SELECT DISTINCT song_id, title, artist_id, 
            CASE WHEN year != 0 THEN year ELSE null END AS year, duration 
        FROM staging_song
    """)
    
    # write songs table to parquet files partitioned by year and artist
    LOG.info('Writing songs table to parquet files partitioned by year/artist')
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    LOG.info('Creating artists table from staged data')
    artists_table = spark.sql("""
    SELECT artist_id, name, location, latitude, longitude
    FROM (
        SELECT 
            artist_id, 
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude,
            RANK() OVER (PARTITION BY artist_id ORDER BY year DESC) AS rank
        FROM staging_song
    ) AS artist
    WHERE rank = 1
    """)
    
    # write artists table to parquet files
    LOG.info('Writing artists table to parquet file')
    artists_table.write.parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """ Create users, time and songplays tables by staging log data from S3.
    Extracted data is saved to S3.
    
    Args:
        spark: Spark session
        input_data: S3 location of log json files
        output_data: S3 location where extracted data is written to.
        
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    LOG.info('Reading log data from '+ log_data)
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    LOG.info('Filter by actions for song plays')
    df = df.filter(df.page == 'NextSong')
    
    LOG.info('Staging log data')
    df.createOrReplaceTempView("staging_events")

    # extract columns for users table    
    LOG.info('Creating users table')
    users_table = spark.sql("""
         SELECT user_id, first_name, last_name, gender, level
         FROM (
            SELECT 
                userId AS user_id, 
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level,
                ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS row_number
            FROM staging_events
         ) AS user
         WHERE row_number = 1
    """)
  
    # write users table to parquet files
    LOG.info('Writing users to parquet files.')
    users_table.write.parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    LOG.info('Creating timestamp column start_time.')
    df = df.withColumn("start_time", to_timestamp(df.ts/1000))
    
    # create id column
    # Credit: https://stackoverflow.com/questions/33102727/primary-keys-with-apache-spark
    LOG.info('Creating id column songplay_id.')
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    
    LOG.info('Refreshing Staged log data')
    df.createOrReplaceTempView("staging_events")
    
    # extract columns to create time table
    LOG.info('Creating time table')
    time_table = df.select('start_time') \
        .dropDuplicates() \
        .withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    LOG.info('Writing time to parquet, partitioned by year and month')
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time.parquet')

    # read in song data to use for songplays table
    LOG.info('Creating the staging_time view')
    time_table.createOrReplaceTempView("staging_time")

    # extract columns from joined song and log datasets to create songplays table 
    LOG.info('Creating songplays table')
    songplays_table = spark.sql("""
        SELECT 
            se.songplay_id,
            se.start_time,
            se.userId as user_id,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId as session_id,
            se.location,
            se.userAgent,
            et.year,
            et.month
        FROM staging_events se
        INNER JOIN staging_song ss ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
        INNER JOIN staging_time et ON se.start_time = et.start_time
    """)

    # write songplays table to parquet files partitioned by year and month
    LOG.info('Writing songplay to parquet partitioned by year and month')
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays.parquet')


def main():
    """
    ETL process to extract songs, artists, user, time, and songplay information from data and save them to S3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://ms-sparkify-lake/"
    
    LOG.info('Start data lake creation')
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    LOG.info('Completed data lake creation')


if __name__ == "__main__":
    main()
