#!/usr/bin/env python
# coding: utf-8

import pandas as pd

from time import time

from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df_iter = pd.read_csv("yellow_tripdata_2021-07.csv", iterator = True, chunksize= 100000)

df = next(df_iter)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql(name = 'yellow_taxi_data', con = engine, if_exists= 'replace')

df.to_sql(name = 'yellow_taxi_data', con = engine, if_exists= 'append')

while True :
    
    start_time = time()
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.to_sql(name = 'yellow_taxi_data', con = engine, if_exists= 'append')
    
    end_time = time()
    
    print("Time Used : %.3f second" %(end_time-start_time))
    
    print("Inserted another chunk...")




