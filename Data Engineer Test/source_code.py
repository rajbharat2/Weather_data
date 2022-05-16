# This module is use for Reading the csv and writing to Parquet by having each day as a row group
#
import os,re,glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

#Max days in a month
days = range(1,32)

#Definintion to read csv file to table
def read_csv(filename):
    global dfs
    global table1
    dfs = []
    df = pd.read_csv(filename)
    df['year'] = pd.to_datetime(df['ObservationDate']).dt.year
    df['month'] = pd.to_datetime(df['ObservationDate']).dt.month
    df['day'] = pd.to_datetime(df['ObservationDate']).dt.day
    #Constructing a table object
    table1 = pa.Table.from_pandas(df, preserve_index=False)
    
    for i in days:
        if not df[(df['day']==i)].empty:
            dfs.append(df[(df['day']==i)])

#to list all file with key word weather
files = glob.glob("weather*.csv")

for file_name in files:    
    match_str = re.search(r'\d{8}', file_name)
    res = datetime.strptime(match_str.group(), '%Y%m%d').date()
    year = str(res.year)
    month = str(res.month)

    #To create data in partition manner
    location = f"mydata/year={year}/month={month}/"
    os.makedirs(location,exist_ok=True )
    write_path = location+"delta_data.parquet"
    # To load the each day data datframe to a list
    read_csv(file_name)
    #creating an writer object to write to file
    writer = pq.ParquetWriter(write_path, table1.schema)

    for df in dfs:
        
        table = pa.Table.from_pandas(df, preserve_index=False, schema=table1.schema)
        writer.write_table(table)
    writer.close()