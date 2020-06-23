#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 16:12:56 2020

@author: dylanroyston
"""

# import/configure packages
import numpy as np
import pandas as pd
#import pyarrow as pa
import librosa
import librosa.display
from pathlib import Path
#import Ipython.display as ipd
#import matplotlib.pyplot as plt
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark import SparkConf, SparkContext, SQLContext
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


# function to read audio files from S3 bucket and extract tags
def read_audio_files():
    
    # basic initialization
    time_seq.append(['start-read-audio', time.time()])
        
    # DataFrame schema
    File_Tags = Row("s3_key", "song_id", "album", "albumartist", "artist", 
                "audio_offset", "bitrate", "channels", "comment", "composer",
                "disc", "disc_total", "duration", "filesize", "genre",
                "samplerate", "title", "track", "track_total", "year")
    
    
    spec_labels = []
    for sn in range(0,128):
        spec_labels.append('spec' + str(sn+1)) 
        
    spec_df_labels = ['song_id','timeseries'] + spec_labels

    Spec_Tags = Row(spec_df_labels)
    
    # configure S3 access
    s3_bucket = 'mdp-spectralize-pal'
    number_of_files = 0
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    number_of_files=0
    file_limit=100
    #local_path = './local_file.'
    
    known_ext = [".mp3", ".wav", ".m4a"]
    

    #read each file from S3 bucket            
    for obj in bucket.objects.all():
        
        s3_key = obj.key       
            
        # extract tags from mp3 files
        #if "mp3" in s3_key:
        if any(ext in s3_key for ext in known_ext):
            
            print(number_of_files)
                
            ext = s3_key[-4:]
            local_path = './localfile' + ext
            
            number_of_files+=1
            bucket.download_file(s3_key, local_path)
            
            #base_df = sc.binaryFiles("s3a://mdp-spectralize-pal/copied-audio/_unsorted/Sylvan Esso - Coffee (Official Audio).mp3") 
            
            
                
            ##### tags
            tags = tt.get(local_path)
            
            # extract tags from tinytag object
            indiv_tags = (s3_key, number_of_files, tags.album, tags.albumartist, tags.artist, 
                          tags.audio_offset, tags.bitrate, tags.channels, 
                          tags.comment, tags.composer, tags.disc, 
                          tags.disc_total, tags.duration, tags.filesize, 
                          tags.genre, tags.samplerate, tags.title, tags.track, 
                          tags.track_total, tags.year)
            
            # convert tuple object to list
            indiv_tag_list = list(indiv_tags)
            indiv_tag_list = [str(i) for i in indiv_tag_list]
            
            tag_seq=[]
            tag_seq.append(indiv_tag_list)
            
            tags_pdf = pd.DataFrame(data=tag_seq)
            
            tag_df = spark.createDataFrame(tags_pdf, schema=File_Tags)
                        
            
            
            ##### audio
            # load audio file with Librosa
            #y, sr = librosa.load(str(Path(local_path)), sr=None)
            y, sr = librosa.load(local_path, sr=None)

            
            # create indexing variables (song_id, timestamp)
            
            # song_id defined as "repeat(number_of_files)"
            song_num = pd.Series([number_of_files])
            num_points = len(y)
            song_id = song_num.repeat(num_points)
            song_id = song_id.to_numpy()
            
            # timeseries defined as "1 : length(audio_data)"
            timeseries = np.arange(num_points)
            timeseries = timeseries.transpose()
                        
            
            full_audio = {'song_id': song_id, 'timeseries': timeseries,
                               'intensity': y}
            
            # create combined dataframe
            audio_pdf = pd.DataFrame(data = full_audio)
            
            audio_df = spark.createDataFrame(audio_pdf)
                        
            
            
            
            
            ##### spectral
            S = librosa.feature.melspectrogram(y, sr=sr, n_mels=128)
            log_S = librosa.power_to_db(S, ref=np.max)
            log_S = log_S.transpose()
            
            # song_id defined as "repeat(number_of_files)"
            song_num = pd.Series([number_of_files])
            num_points = len(S.transpose())
            song_id = song_num.repeat(num_points)
            song_id = song_id.to_numpy()
            
            # timeseries defined as "1 : length(audio_data)"
            timeseries = np.arange(num_points)
            timeseries = timeseries.transpose()

            full_index = {'song_id': song_id, 'timeseries': timeseries}
            index_pdf = pd.DataFrame(full_index)
            
            spec_pdf = pd.DataFrame(data=log_S, columns=spec_labels)
            
            full_spec = pd.concat([index_pdf, spec_pdf], axis=1)
            
            spec_df = spark.createDataFrame(full_spec)
            
            


            
            
        # stop process when file_limit is crossed (small batches)
        if (number_of_files >= file_limit):
            break
    
        #####







# Function to write spark-dataframe to mySQL
def write_df_to_psql(df, tablename):
    
        psql_user = os.environ.get('PSQL_USR')
        psql_pwd = os.environ.get('PSQL_PWD')

        df.write.format('jdbc').options(
        url='jdbc:postgresql://10.0.0.6:5432/spectralize',
        dbtable=tablename,
        user=psql_user,
        #password=psql_pwd).mode('append').save()
        password=psql_pwd).save()
    

#####




if __name__ == '__main__':
    time_seq.append(['start', time.time()])
    read_audio_files()
            




    # create local Spark instance (for non-cluster dev)
    sc = SparkContext('local')
    spark = SparkSession (sc)
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    
    
    # define Spark config
    def spark_conf():
     	conf = SparkConf().setAppName("decompress_audio_files")
     	sc = SparkContext(conf=conf)
     	spark = SparkSession.builder.getOrCreate()
     	return spark
     	
    spark = spark_conf()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    
    #####
    
    
        ##### write dataframes to psql
    write_df_to_psql(tag_df, 'clean_metadata')
    write_df_to_psql(audio_df, 'clean_audio')
    write_df_to_psql(spec_df, 'clean_spec')

        
    time_seq.append(['end read-file', time.time()])
    #df_tags = spark.createDataFrame(tag_seq, schema=File_Tags)
    #df_audio = spark.createDataFrame(audio_seq)
    #df_spec = spark.createDataFrame(audio_seq, schema=Spec_Tags)
    
    # Additional run to 
    #df_audio_data = spark.createDataFrame(file_audio_data)
    #process_df(df_audio_data)
    
    
    #####
        
    
