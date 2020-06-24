# import/configure packages
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark import SparkConf
from pyspark import SparkContext
#from pyspark.ml import Pipeline
#from pyspark.ml.feature import RegexTokenizer, NGram, HasingTF, MinHashLSH
import boto3
from tinytag import TinyTag as tt
from io import BytesIO
import os
import sys
import time


# define Spark config
def spark_conf():
	conf = SparkConf().setAppName("extract_mp3_tags")
	sc = SparkContext(conf=conf)
	spark = SparkSession.builder.getOrCreate()
	return spark
	
spark = spark_conf()

# Function to write spark-dataframe to mySQL
#def write_df_to_mysql(df, table_name)
	



# DataFrame schema
File_Tags = Row("s3_key", "album", "albumartist", "artist", "audio_offset", "bitrate", "channels", "comment", "composer", "disc", "disc_total", "duration", "filesize", "genre", "samplerate", "title", "track", "track_total", "year")
file_tags_seq = []



# configure S3 access
s3_bucket = 'mdp-spectralize-pal'
number_of_files = 0
s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_bucket)
number_of_files=0
file_limit=100


#read each file from S3 bucket    
for obj in bucket.objects.all():
    number_of_files+=1
        
    s3_key = obj.key

    this_path, this_filen = os.path.split(s3_key)
        
    print(s3_key)
    #print(this_path)
    #print(this_filen)
        
    # extract tags from mp3 files
    if "mp3" in s3_key:
        local_path = './local_file.mp3'
        bucket.download_file(s3_key, local_path)
        #mp3 = MP3File(local_path)
            
        try:
            #tags = mp3.get_tags()
            tags = tt.get(local_path)
            print(tags)
        except:
            print("invalid file metadata")
            
    if (number_of_files >= file_limit):
        break
        
