#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 18:54:48 2020

@author: dylanroyston
"""


# -*- coding: utf-8 -*-

# import packages
#import dash_player
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import psycopg2
import os
import pandas as pd
import numpy as np
import plotly
import plotly.express as px
import plotly.graph_objects as go
import librosa
import librosa.display as ld
import IPython.display as ipd
import pylab as pl
import boto3
#import matplotlib as mpl
#import matplotlib.pyplot as plt
#from matplotlib import cm
#from colorspacious import cspace_converter
#from collections import OrderedDict

######


# connect to PSQL and retrieve 
psql_usr = os.environ.get('PSQL_USR')
psql_pw = os.environ.get('PSQL_PW')

conn = psycopg2.connect(host = 'ec2-13-58-251-142.us-east-2.compute.amazonaws.com',
                        dbname = 'spectralize',
                        user='postgres',
                        password=psql_pw)



##### read out metadata
metadata = conn.cursor()

metadata.execute("SELECT * FROM clean_metadata WHERE false;")
cols = set(metadata.fetchall())

metadata.execute("SELECT * FROM clean_metadata;")
md = set(metadata.fetchall())

cols = ["s3_key", "song_id", "album", "albumartist", "artist", 
                "audio_offset", "bitrate", "channels", "comment", "composer",
                "disc", "disc_total", "duration", "filesize", "genre",
                "samplerate", "title", "track", "track_total", "year"]

tag_df = pd.DataFrame(data=md, columns=cols)




##### s3 acess for playing audio files
s3_bucket = 'mdp-spectralize-pal'
number_of_files = 0
s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_bucket)
    
# placeholders for callback initialization
standin_fp = '/home/dylanroyston/Documents/GIT/spectralize/app/hello.wav'
audio_sd_file = standin_fp
#audio_rawfile, new_sr = librosa.load(standin_fp, sr=None)
standin_data = np.array([[0,0],[0,0]])
standin_df = pd.DataFrame(standin_data, columns=['x','y'])
#audio_fig = px.line(standin_df, x='x', y='y', title='audio data', render_mode='webgl')
spec_fig = px.imshow(standin_df)

def load_audio_data(selected_row):
    # read out audio data
    
    #curr_song_id = tag_df.iloc[selected_row]['song_id']
    curr_song_id = selected_row
    
    # audiodata = conn.cursor()
    
    # qstring = 'SELECT intensity FROM clean_audio WHERE song_id=' + str(curr_song_id)
    
    # audiodata.execute(qstring)
    # ad = np.array(audiodata.fetchall())
    
    # audio_df = pd.DataFrame(data=ad, columns=['I'])
    # audio_fig = px.line(audio_df, x=audio_df.index, y='I', title='audio data', render_mode='webgl')
    # audio_fig.update_layout(
    #     height=250,
    #     margin_r=0,
    #     margin_l=0,
    #     margin_t=0,
    #     yaxis_title='',
    #     yaxis_fixedrange=True)
    

    s3_key = tag_df.iloc[curr_song_id]['s3_key']
    
    #this_row = tag_df.loc[tag_df['song_id'] == curr_song_id]
    #s3_key = tag_df.iloc[this_row]['s3_key']
    
    ext = s3_key[-4:]
    audio_sd_file = '/home/dylanroyston/Documents/GIT/spectralize/app/audio_file' + ext
    
    bucket.download_file(s3_key, audio_sd_file)     
    
    #audio_rawfile = librosa.load(audio_sd_file)
    
    
    
    return audio_sd_file#, audio_fig

def load_spec_data(selected_row):
    
    curr_song_id = selected_row
    
    specdata = conn.cursor()
    qstring = 'SELECT * FROM clean_spec WHERE song_id=' + str(curr_song_id)
    specdata.execute(qstring)
    sd = np.array(specdata.fetchall())
    
    spec_df = pd.DataFrame(data=sd)
    
    currtitle = tag_df.iloc[curr_song_id]['s3_key']
    currdur = tag_df.iloc[curr_song_id]['duration']

    numpts = len(sd)
    
    interval = float(currdur) / numpts
    
    timeline = np.linspace(0,float(currdur),numpts)
    
    rt = timeline.round(0)

    trim_sd = spec_df.iloc[:,2:]
    spec_fig = px.imshow(np.flipud(trim_sd.transpose()))
    spec_fig.update_layout(
        height=250,
        margin_r=0,
        margin_l=0,
        margin_t=0,
        yaxis_title='frequency',
        #colorbar.title='power',
        yaxis_fixedrange=True,
        x=str(rt)
        #title=currtitle
        )
    
    return spec_fig


#####
# initialize Dash app    
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    
    # header
    html.H1(children='Metadata'),
    
    # metadata table
    dash_table.DataTable(
        id = 'metadata_table',
        data=tag_df.to_dict('rows'),
        columns=[{'id': c, 'name': c} for c in tag_df.columns],
        style_cell={
            'overflowX': 'auto',
            'textOverflow': 'ellipsis',
            'maxWidth': 0,
            'row_selectable': 'single',
            'font_family': 'Arial',
            'font_size': '1.5rem',
            'padding': '.1rem',
            'backgroundColor': '#f4f4f2'
            },
        style_cell_conditional=[
            {'textAlign': 'center'}
            ],
        style_header={
            'backgroundColor':'#f4f4f2',
            'fontWeight': 'bold'
            },
        style_table={
            'maxHeight':'500px',
            'overflowX': 'scroll'
            },
        tooltip_data=[
            {
                column: {'value': str(value), 'type': 'markdown'}
                for column, value in row.items()
            } for row in tag_df.to_dict('rows')
        ],
        tooltip_duration=None
    ),# end table
    
    
    # load audio button
    html.Br(),
    
    html.Div(
        [
            dcc.Input(id='input_songnum', value='input song number', type='text'),
            html.Button('Load audio', 
                    id='submit-val',
                    style={'display': 'inline-block'},
                    n_clicks=0),
            html.Div(id='song_input')
         ],
    ),
    
    html.Br(),
    
    
    # html.Audio(id="player", src=audio_sd_file, controls=True, style={
    #     "width": "100%"
    # }),    
    # dash_player.DashPlayer(
    #     id='player',
    #     url='audio_sd_file',
    #     controls=True
    # ),
    
    html.Br(),
    
    #dcc.Graph(id='waveform', figure=audio_fig),
    
    html.Br(),

    dcc.Graph(id='spect', figure=spec_fig)
            
])
##### finish Dash layout



##### callbacks
# load-audio button control
# @app.callback(
#     Output('input_songnum', 'value'),
#     [Input('submit-val', 'n_clicks')]
#     )
# def retrieve_audio(value):
#     return load_audio_data(value)


# @app.callback(
#     Output('waveform', 'figure'),
#     [Input('submit-val', 'n_clicks')]
#     )
# def update_A_figure(submit_val):
#     audio_fig = load_audio_data(submit_val)
#     return audio_fig
    

## update audio player
# @app.callback(
#     Output('player', 'src'),
#     [Input('submit-val', 'n_clicks')]
#     )
# def update_player(submit_val):
#     audio_sd_file = load_audio_data(submit_val)
#     return audio_sd_file



## update spect figure on button click
@app.callback(
    Output('spect', 'figure'),
    [Input('submit-val', 'n_clicks'),
     Input('input_songnum', 'value')]
    )
def update_S_figure(n_clicks, value):
    
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    
    if 'submit-val' in changed_id:
        spec_fig = load_spec_data(value)
    
    return spec_fig





## combined audiofile/spec update
# @app.callback(
#     [Output('player', 'src'),
#      Output('spect', 'figure')],
#     [Input('submit-val', 'n_clicks')]
#     )
# def update_figures(submit_val):
#     audio_sd_file = load_audio_data(submit_val)
#     spec_fig = load_spec_data(submit_val)
#     return audio_sd_file, spec_fig
    

# @app.callback(
#     Output('metadata_table', 'derived_virtual_selected_rows'),
#     [Input('submit-val', 'n_clicks'),
#      State('metadata_table', 'derived_virtual_selected_rows')]
#     )
# def update_audio(n_clicks, derived_virtual_selected_rows):
#     if derived_virtual_selected_rows is None:
#         derived_virtual_selected_rows = []
        
    
#     return load_audio_data(derived_virtual_selected_rows)


if __name__ == '__main__':
    #app.run_server(debug=True, port=8050, host='127.0.0.1')
    app.run_server(debug=True, port=8050, host='127.0.0.1')
    #app.run_server(debug=True, port=80, host='ec2-18-224-114-72.us-east-2.compute.amazonaws.com')
