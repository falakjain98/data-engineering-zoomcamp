# import modules
from sqlalchemy import create_engine
import pandas as pd

# read input file
df = pd.read_parquet('yellow_tripdata_2023-01.parquet')

# convert datetime columns to datetime
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

# create database engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# get table schema
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

# save headers to table
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# append data to table
df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

# get look up data
get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv')
df_zones = pd.read_csv('taxi_zone_lookup.csv')

# save zones data to sql table
df_zones.to_sql(name='zones', con=engine, if_exists='replace')