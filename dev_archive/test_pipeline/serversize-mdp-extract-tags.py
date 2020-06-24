# import/configure packages
#import pyspark.sql
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark import SparkConf
from pyspark import SparkContext
#from pyspark.ml import Pipeline
#from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH
#import pymysql
import boto3
from tinytag import TinyTag as tt
#from io import BytesIO
import os
import sys
import time


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/lib")
#import config
time_seq = []

#####

# create local Spark instance (for non-cluster dev)
#sc = SparkContext('local')
#spark = SparkSession (sc)
#spark.builder.config('spark.jars', '/usr/local/spark/jars/postgresql-42.2.13.jar')

# define Spark config
def spark_conf():
 	conf = SparkConf().setAppName("extract_mp3_tags")
 	sc = SparkContext(conf=conf)
 	spark = SparkSession.builder.getOrCreate()
 	return spark
 	
spark = spark_conf()

#####

# Function to write spark-dataframe to mySQL
def write_df_to_mysql(df, tablename):
    # mysql_user = os.environ.get('MYSQL_USER')
    # mysql_pwd = os.environ.get('MYSQL_PWD')
    # df.write.format('jdbc').options(
    #     url='jdbc:mysql://10.0.0.6/spectralize',
    #     driver='com.mysql.jdbc.Driver',
    #     dbtable=tablename,
    #     user=mysql_user,
    #     password=mysql_pwd).mode('append').save()
    
    
        psql_user = os.environ.get('PSQL_USR')
        psql_pwd = os.environ.get('PSQL_PWD')

        tablename='metadata1'

        df.write.format('jdbc').options(
        url='jdbc:postgresql://10.0.0.6:5432/spectralize',
        #driver='org.postgresql.Driver',
        #driver='com.postgresql.jdbc.Driver',
        dbtable=tablename,
        user=psql_user,
        password=psql_pwd).mode('append').save()
    

#####

# function to process audio data
# def process_df(df):
#     time_seq.append(['start process-df', time.time()])
#     time_seq.append(['process-df df', time.time()])
#     write_df_to_mysql(df, 'metadata')
#     time_seq.append(['write mysql', time.time()])
#     print('time_seq', time_seq)

#####

# function to read audio files from S3 bucket and extract tags
def read_audio_files():
    # basic initialization
    time_seq.append(['start-read-audio', time.time()])
        
    
    # DataFrame schema
    File_Tags = Row("s3_key", "album", "albumartist", "artist", "audio_offset", 
                "bitrate", "channels", "comment", "composer", "disc", 
                "disc_total", "duration", "filesize", "genre", "samplerate", 
                "title", "track", "track_total", "year")
    tag_seq = []
    
    # configure S3 access
    s3_bucket = 'mdp-spectralize-pal'
    number_of_files = 0
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    number_of_files=0
    file_limit=5
    
    
    #read each file from S3 bucket            
    
    for obj in bucket.objects.all():
        number_of_files+=1
            
        s3_key = obj.key
    
        this_path, this_filen = os.path.split(s3_key)
            
        #print(s3_key)
        #print(this_path)
        #print(this_filen)
            
        # extract tags from mp3 files
        if "mp3" in s3_key:
            local_path = './local_file.mp3'
            bucket.download_file(s3_key, local_path)
            #mp3 = MP3File(local_path)
                
            # try to run tag-extract, may crash on invalid files
            #tags = mp3.get_tags()
            tags = tt.get(local_path)
            
            #print(tags)
            #print( )
            # extract tags from tinytag object
            indiv_tags = (s3_key, tags.album, tags.albumartist, tags.artist, 
                          tags.audio_offset, tags.bitrate, tags.channels, 
                          tags.comment, tags.composer, tags.disc, 
                          tags.disc_total, tags.duration, tags.filesize, 
                          tags.genre, tags.samplerate, tags.title, tags.track, 
                          tags.track_total, tags.year)
            
            # convert tuple object to list
            indiv_tag_list = list(indiv_tags)
            indiv_tag_list = [str(i) for i in indiv_tag_list]
            
            #print(indiv_tag_list)
            #print( )
            
            
            tag_seq.append(indiv_tag_list)
            
           # print(tag_seq)
           # print( )
    
                
        # stop process when file_limit is crossed (small batches)
        if (number_of_files >= file_limit):
            break
    
    time_seq.append(['end read-file', time.time()])
    df_file_tags = spark.createDataFrame(tag_seq, schema=File_Tags)
    #display(df_file_tags)
    write_df_to_mysql(df_file_tags, 'metadata')
    # Additional run to 
    #df_audio_data = spark.createDataFrame(file_audio_data)
    #process_df(df_audio_data)


if __name__ == '__main__':
    time_seq.append(['start', time.time()])
    read_audio_files()
            
