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
#from pathlib import Path
#import Ipython.display as ipd
#import matplotlib.pyplot as plt
from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext, SQLContext
import boto3
from tinytag import TinyTag as tt
#from io import BytesIO
import os
import sys
import time

#import operator
#import pywt
#####


#####

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/lib")
#import config
time_seq = []

#####



#####
def get_metadata(sfile):
    time_seq.append(['start-read-metadata', time.time()])

    # DataFrame schema
    File_Tags = Row("s3_key", "song_id", "album", "albumartist", "artist", 
                "audio_offset", "bitrate", "channels", "comment", "composer",
                "disc", "disc_total", "duration", "filesize", "genre",
                "samplerate", "title", "track", "track_total", "year")
    
    


    #Spec_Tags = Row(spec_df_labels)

    #local_path = './local_file.'
    
    #known_ext = [".mp3", ".wav", ".m4a"]
    
    #ext = s3_key[-4:]
        
    ##### tags
    tags = tt.get(sfile)
    
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
    
    
    tags_df = pd.DataFrame(data=tag_seq)
    #tags_pdf = pd.DataFrame(data=tag_seq)
    
    #tag_df = spark.createDataFrame(tags_pdf, schema=File_Tags)
    
    return tag_df
# get metadata
    

def get_ASdata(sfile):
    time_seq.append(['start-read-audio', time.time()])
    
    y, sr = librosa.load(local_path, sr=None)

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
    
    time_seq.append(['start-read-spec', time.time()])
    
    
    
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
    
    spec_labels = []
    for sn in range(0,128):
        spec_labels.append('spec' + str(sn+1)) 
        
    spec_df_labels = ['song_id','timeseries'] + spec_labels
    
    spec_pdf = pd.DataFrame(data=log_S, columns=spec_labels)
    
    full_spec = pd.concat([index_pdf, spec_pdf], axis=1)
    
    spec_df = spark.createDataFrame(full_spec)
    
    return audio_df, spec_df
# get audio data
    
##### read audio files




#####
# Function to write spark-dataframe to mySQL
def write_df_to_psql(df, tablename):
    
        time_seq.append(['start-write-db', time.time()])

        psql_user = os.environ.get('PSQL_USR')
        psql_pwd = os.environ.get('PSQL_PWD')

        df.write.format('jdbc').options(
        url='jdbc:postgresql://10.0.0.6:5432/spectralize',
        dbtable=tablename,
        user=psql_user,
        #password=psql_pwd).mode('append').save()
        password=psql_pwd).save()
##### write to psql
    
    
    
    
#####
#####
#####
# main

if __name__ == '__main__':
    
    time_seq.append(['start', time.time()])
    
    # create local Spark instance (for non-cluster dev)
    sc = SparkContext('local')
    spark = SparkSession (sc)
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    
    
    # define EC2 cluster Spark config
    # def spark_conf():
    #  	conf = SparkConf().setAppName("decompress_audio_files")
    #  	sc = SparkContext(conf=conf)
    #  	spark = SparkSession.builder.getOrCreate()
    #  	return spark
     	
    # spark = spark_conf()
    # spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    
    
    #####
    # configure S3 bucket access and retrieve all object keys
    s3_bucket = 'mdp-spectralize-pal'
    number_of_files = 0
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    
    s3 = boto3.client('s3')

    # loop through all objects
    def get_all_s3_keys(bucket):
        """Get a list of all keys in an S3 bucket."""
        keys = []
    
        kwargs = {'Bucket': bucket}
        while True:
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                keys.append(obj['Key'])
    
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
    
        return keys
    #
    
    key_list = get_all_s3_keys(s3_bucket)
    
    ##### S3 bucket setup
    
    #####
    # create dataframe of all 
    
    #base_df = sc.binaryFiles("s3a://mdp-spectralize-pal/copied-audio/_unsorted/Sylvan Esso - Coffee (Official Audio).mp3") 
    df_allbin = sc.binaryFiles(key_list)
    
    
    
    
    # def sort_and_analyze_time_series(l):
    #     if len(l) < 3687: # ideally this needs to be 4096; 10% tolerance
    #         return None
    #     res = sorted(l, key=operator.itemgetter(0))
    #     time_series = [item[1] for item in res]
    #     abnormality_indicator = get_delta_ap_en(time_series)
    #     return abnormality_indicator
    
    # analyze_udf = udf(sort_and_analyze_time_series)
    
    # df_analyzed = df_windowed.select(
    # "instr_time",
    # df_windowed.window.end.alias("ingest_time"),
    # "subject_id",
    # "channel",
    # analyze_udf(df_windowed.time_series).cast(FloatType()).alias("abnormality_indicator"),
    # "num_datapoints"
    # )
    
    
    
    
    def extract_file_data(audiofile):
        
        tag_df = get_metadata(audiofile)
        #audio_df, spec_df = get_ASdata(audiofile)
        
        return tag_df#, audio_df, spec_df
     
    exploded_files_udf = udf(extract_file_data)
    
    
    df_split = df_allbin.select(
        
        exploded_files_udf(df_allbin)        
        
    )
    # extract file data
    #####
    
    
    
    ##### write dataframes to psql
    write_df_to_psql(tag_df, 'clean_metadata')
    #write_df_to_psql(audio_df, 'clean_audio')
    #write_df_to_psql(spec_df, 'clean_spec')

        
    time_seq.append(['end read-file', time.time()])

    print(time_seq)

    
    
    #####
        
    
