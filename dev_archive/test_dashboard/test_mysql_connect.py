import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import mysql.connector
from dash.dependencies import Event, Output
import plotly
import requests
import dash_table_experiments as dt
import pandas as pd  

import os

mysql_username = os.environ.get("MYSQL_USR")
mysql_password = os.environ.get("MYSQL_PWD")
mysql_host = '13.58.251.142'


def generate_table():
    cnx = mysql.connector.connect(
    	host=mysql_host,
    	user=mysql_username, 
    	password=mysql_password, 
    	database='spectralize')
    query = "SELECT * FROM metadata;"
    cursor = cnx.cursor()
    cursor.execute(query)
    for data in cursor:
        data_name.append(data[0])
        data_port.append(data[1])
    DF_SIMPLE = pd.DataFrame({
        "name" : data_name,
        "port" : data_port
    })
    table = dt.DataTable(
        rows = DF_SIMPLE.to_dict('records'),
        columns = DF_SIMPLE.columns,
        id = 'sample-table'
    )
    return table
app = dash.Dash()
app.layout = html.Div([generate_table(),
    dcc.Interval(
        id='interval-component',
        interval=5 * 1000,  # in milliseconds
    ),])


@app.callback(Output('sample-table', 'rows'), events=[Event('interval-component', 'interval')])
def generate_table():
    cnx = mysql.connector.connect(
    	host=mysql_host,
    	user=mysql_username, 
    	password=mysql_password, 
    	database='spectralize')
    query = "SELECT * FROM metadata;"
    cursor = cnx.cursor()
    cursor.execute(query)
    for data in cursor:
        data_name.append(data[0])
        data_port.append(data[1])
    DF_SIMPLE = pd.DataFrame({
        "name" : data_name,
        "port" : data_port
    })
    rows = DF_SIMPLE.to_dict('records')
    return rows
