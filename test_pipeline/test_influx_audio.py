#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 17:48:26 2020

@author: dylanroyston
"""


# import/configure packages
import numpy as np
import pandas as pd
import pyarrow as pa
import librosa
import librosa.display
#import Ipython.display as ipd
import matplotlib.pyplot as plt
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark import SparkConf
from pyspark import SparkContext
import boto3
from tinytag import TinyTag as tt

from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import DenseVector
from pyspark.ml.linalg import DenseMatrix
from pyspark.sql.types import _infer_schema

#from io import BytesIO
import os
import sys
import time


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/lib")
#import config

#####

# create local Spark instance (for non-cluster dev)
sc = SparkContext('local')
spark = SparkSession (sc)
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# define Spark config
# def spark_conf():
#  	conf = SparkConf().setAppName("extract_mp3_tags")
#  	sc = SparkContext(conf=conf)
#  	spark = SparkSession.builder.getOrCreate()
#  	return spark
 	
# spark = spark_conf()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")


#####


audio_path = '/home/dylanroyston/Music/spectralize_data/Circle of Innocence.wav'

y, sr = librosa.load(audio_path, sr=None)


# Let's make and display a mel-scaled power (energy-squared) spectrogram
S = librosa.feature.melspectrogram(y, sr=sr, n_mels=128)

# Convert to log scale (dB). We'll use the peak power (max) as reference.
log_S = librosa.power_to_db(S, ref=np.max)

# Make a new figure
#plt.figure(figsize=(12,4))

# Display the spectrogram on a mel scale
# sample rate and hop length parameters are used to render the time axis
librosa.display.specshow(log_S, sr=sr, x_axis='time', y_axis='mel')

#####


#df = np.concatenate([np.random.randint(0,2, size=(1000)), np.random.randn(1000), 3*np.random.randn(1000)+2, 6*np.random.randn(1000)-2]).reshape(1000,-1)
#dff = map(lambda x: (int(x[0]), Vectors.dense(x[1:])), df)
#mydf = spark.createDataFrame(dff,schema=["label", "features"])

df = y
dff = map(lambda x: (Vectors.dense(x)),  df)

df = DenseVector(y)
#_infer_schema((df, ))

pdf = pd.DataFrame(y,columns=(list("I")))

# this step takes a long time
mydf2 = spark.createDataFrame(pdf, schema=["intensity"])

#mydf = spark.createDataFrame((pdf, ),schema=["intensity"])


mysql_user = os.environ.get('MYSQL_USER')
mysql_pwd = os.environ.get('MYSQL_PWD')

tablename='raw_audio'
mydf2.write.format('jdbc').options(
url='jdbc:postgresql://127.0.0.1/spectralize',
driver='com.mysql.jdbc.Driver',
dbtable=tablename,
user=mysql_user,
password=mysql_pwd).mode('append').save()






#####
# Put a descriptive title on the plot
#plt.title('mel power spectrogram')

# draw a color bar
#plt.colorbar(format='%+02.0f dB')

# Make the figure layout compact
#plt.tight_layout()

#####


# y_harmonic, y_percussive = librosa.effects.hpss(y)

# # What do the spectrograms look like?
# # Let's make and display a mel-scaled power (energy-squared) spectrogram
# S_harmonic   = librosa.feature.melspectrogram(y_harmonic, sr=sr)
# S_percussive = librosa.feature.melspectrogram(y_percussive, sr=sr)

# # Convert to log scale (dB). We'll use the peak power as reference.
# log_Sh = librosa.power_to_db(S_harmonic, ref=np.max)
# log_Sp = librosa.power_to_db(S_percussive, ref=np.max)

# # Make a new figure
# plt.figure(figsize=(12,6))

# plt.subplot(2,1,1)
# # Display the spectrogram on a mel scale
# librosa.display.specshow(log_Sh, sr=sr, y_axis='mel')

# # Put a descriptive title on the plot
# plt.title('mel power spectrogram (Harmonic)')

# # draw a color bar
# plt.colorbar(format='%+02.0f dB')

# plt.subplot(2,1,2)
# librosa.display.specshow(log_Sp, sr=sr, x_axis='time', y_axis='mel')

# # Put a descriptive title on the plot
# plt.title('mel power spectrogram (Percussive)')

# # draw a color bar
# plt.colorbar(format='%+02.0f dB')

# # Make the figure layout compact
# plt.tight_layout()


# #####
# # We'll use a CQT-based chromagram with 36 bins-per-octave in the CQT analysis.  An STFT-based implementation also exists in chroma_stft()
# # We'll use the harmonic component to avoid pollution from transients
# C = librosa.feature.chroma_cqt(y=y_harmonic, sr=sr, bins_per_octave=36)

# # Make a new figure
# plt.figure(figsize=(12,4))

# # Display the chromagram: the energy in each chromatic pitch class as a function of time
# # To make sure that the colors span the full range of chroma values, set vmin and vmax
# librosa.display.specshow(C, sr=sr, x_axis='time', y_axis='chroma', vmin=0, vmax=1)

# plt.title('Chromagram')
# plt.colorbar()

# plt.tight_layout()


# #####
# # Next, we'll extract the top 13 Mel-frequency cepstral coefficients (MFCCs)
# mfcc        = librosa.feature.mfcc(S=log_S, n_mfcc=13)

# # Let's pad on the first and second deltas while we're at it
# delta_mfcc  = librosa.feature.delta(mfcc)
# delta2_mfcc = librosa.feature.delta(mfcc, order=2)

# # How do they look?  We'll show each in its own subplot
# plt.figure(figsize=(12, 6))

# plt.subplot(3,1,1)
# librosa.display.specshow(mfcc)
# plt.ylabel('MFCC')
# plt.colorbar()

# plt.subplot(3,1,2)
# librosa.display.specshow(delta_mfcc)
# plt.ylabel('MFCC-$\Delta$')
# plt.colorbar()

# plt.subplot(3,1,3)
# librosa.display.specshow(delta2_mfcc, sr=sr, x_axis='time')
# plt.ylabel('MFCC-$\Delta^2$')
# plt.colorbar()

# plt.tight_layout()

# # For future use, we'll stack these together into one matrix
# M = np.vstack([mfcc, delta_mfcc, delta2_mfcc])


# #####

# # Now, let's run the beat tracker.
# # We'll use the percussive component for this part
# plt.figure(figsize=(12, 6))
# tempo, beats = librosa.beat.beat_track(y=y_percussive, sr=sr)

# # Let's re-draw the spectrogram, but this time, overlay the detected beats
# plt.figure(figsize=(12,4))
# librosa.display.specshow(log_S, sr=sr, x_axis='time', y_axis='mel')

# # Let's draw transparent lines over the beat frames
# plt.vlines(librosa.frames_to_time(beats),
#            1, 0.5 * sr,
#            colors='w', linestyles='-', linewidth=2, alpha=0.5)

# plt.axis('tight')

# plt.colorbar(format='%+02.0f dB')

# plt.tight_layout();

# #####

# print('Estimated tempo:        %.2f BPM' % tempo)

# print('First 5 beat frames:   ', beats[:5])

# # Frame numbers are great and all, but when do those beats occur?
# print('First 5 beat times:    ', librosa.frames_to_time(beats[:5], sr=sr))

# # We could also get frame numbers from times by librosa.time_to_frames()


# #####

# # feature.sync will summarize each beat event by the mean feature vector within that beat

# M_sync = librosa.util.sync(M, beats)

# plt.figure(figsize=(12,6))

# # Let's plot the original and beat-synchronous features against each other
# plt.subplot(2,1,1)
# librosa.display.specshow(M)
# plt.title('MFCC-$\Delta$-$\Delta^2$')

# # We can also use pyplot *ticks directly
# # Let's mark off the raw MFCC and the delta features
# plt.yticks(np.arange(0, M.shape[0], 13), ['MFCC', '$\Delta$', '$\Delta^2$'])

# plt.colorbar()

# plt.subplot(2,1,2)
# # librosa can generate axis ticks from arbitrary timestamps and beat events also
# librosa.display.specshow(M_sync, x_axis='time',
#                          x_coords=librosa.frames_to_time(librosa.util.fix_frames(beats)))

# plt.yticks(np.arange(0, M_sync.shape[0], 13), ['MFCC', '$\Delta$', '$\Delta^2$'])             
# plt.title('Beat-synchronous MFCC-$\Delta$-$\Delta^2$')
# plt.colorbar()

# plt.tight_layout()







#####


if __name__ == '__main__':
    time_seq.append(['start', time.time()])
    read_audio_files()





















