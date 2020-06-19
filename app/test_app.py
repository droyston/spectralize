#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 18:54:48 2020

@author: dylanroyston
"""


# -*- coding: utf-8 -*-
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import psycopg2
import os
import pandas as pd
import numpy as np
import plotly.express as px
import librosa
import librosa.display as ld
import IPython.display as ipd


######


# connect to PSQL and retrieve 
psql_usr = os.environ.get('PSQL_USR')
psql_pw = os.environ.get('PSQL_PW')

conn = psycopg2.connect(host = 'ec2-13-58-251-142.us-east-2.compute.amazonaws.com',
                        dbname = 'spectralize',
                        user='postgres',
                        password=psql_pw)


# read out metadata
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

audio_fig = px.scatter(x=[0], y=[0])


# read out audio data
audiodata = conn.cursor()

qstring = 'SELECT timeseries,intensity FROM clean_audio WHERE song_id=' + str(1)

audiodata.execute(qstring)
ad = np.array(audiodata.fetchall())
audio_df = pd.DataFrame(data=ad, columns=['time', 'I'])
audio_fig = px.scatter(audio_df, x='time', y='I', title='audio data')

# def load_audio_data(curr_song_id):
# # read out audio data
#     audiodata = conn.cursor()
    
#     qstring = 'SELECT timeseries,intensity FROM clean_audio WHERE song_id=' + str(curr_song_id)
    
#     audiodata.execute(qstring)
#     ad = np.array(audiodata.fetchall())
#     audio_df = pd.DataFrame(data=ad, columns=['time', 'I'])
#     audio_fig = px.scatter(audio_df, x='time', y='I', title='audio data')
#     return audio_fig

#audiodata.execute("SELECT intensity FROM clean_audio WHERE song_id=1")
#ad = np.array(audiodata.fetchall())
#audio_df = pd.DataFrame(data=ad, columns=['I'])


#audio_fig = ld.waveplot(audio_df)



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div(children=[
    html.H1(children='Metadata'),
    
    dash_table.DataTable(
    data=tag_df.to_dict('records'),
    columns=[{'id': c, 'name': c} for c in tag_df.columns],
    style_cell={
        'overflowX': 'auto',
        'textOverflow': 'ellipsis',
        'maxWidth': 0,
        'row_selectable': 'single'
    },
    tooltip_data=[
        {
            column: {'value': str(value), 'type': 'markdown'}
            for column, value in row.items()
        } for row in tag_df.to_dict('rows')
    ],
    tooltip_duration=None
    ),
    
    html.Div(children='''
        Audio Data
        '''),
    
    html.Div(dcc.Input(id='input-on-submit', type='text')),
    html.Button('Submit', id='submit-val', n_clicks=0),
    html.Div(id='container-button-basic',
             children='Enter a value and press submit'),
    
    
    dcc.Graph(
        id='audio-intensity',
        figure=audio_fig
        )
])




@app.callback(
    Output('div-out','children'),
    [Input('datatable', 'rows'),
     Input('datatable', 'selected_row_indices')])
def f(rows,selected_row_indices):
    #either:
    selected_rows=[rows[i] for i in selected_row_indices]
    #or
    #selected_rows=pd.DataFrame(rows).iloc[i] 
    return selected_rows

@app.callback(
    dash.dependencies.Output('container-button-basic', 'children'),
    [dash.dependencies.Input('submit-val', 'n_clicks')],
    [dash.dependencies.State('input-on-submit', 'value')])
def update_output(value):
    return load_audio_data(value)



#########

# def generate_table(dataframe, max_rows=10):
#     return html.Table([
#         html.Thead(
#             html.Tr([html.Th(col) for col in dataframe.columns])
#         ),
#         html.Tbody([
#             html.Tr([
#                 html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
#             ]) for i in range(min(len(dataframe), max_rows))
#         ])
#     ])


# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# app.layout = html.Div(children=[
#     html.H4(children='File Metadata'),
#     generate_table(tag_df)
# ])



##############

# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# app.layout = dash_table.DataTable(
#     id='table',
#     columns=[{"name": i, "id": i} for i in tag_df.columns],
#     data=tag_df.to_dict('records'),
# )

if __name__ == '__main__':
    app.run_server(debug=True, port=8050, host='127.0.0.1')
    #app.run_server(debug=True, port=80, host='ec2-18-224-114-72.us-east-2.compute.amazonaws.com')