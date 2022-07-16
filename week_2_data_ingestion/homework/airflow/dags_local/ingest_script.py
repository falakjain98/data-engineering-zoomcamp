import os

from time import time

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq


def ingest_callable(user, password, host, port, db, table_name, parquet_file, execution_date):
    print(table_name, parquet_file, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    t_start = time()
    parquetFile = pq.ParquetFile(parquet_file)
    count = 1

    for indx, batch in enumerate(parquetFile.iter_batches(batch_size=100000)):
        batch_df = batch.to_pandas()
        batch_df.tpep_pickup_datetime = pd.to_datetime(batch_df.tpep_pickup_datetime)
        batch_df.tpep_dropoff_datetime = pd.to_datetime(batch_df.tpep_dropoff_datetime)

        if indx == 0:
            batch_df.head(n=0).to_sql(name=table_name,con = engine, if_exists = 'replace')
            batch_df.to_sql(name = table_name, con=engine, if_exists='append')
        else:
            batch_df.to_sql(name = table_name, con=engine, if_exists='append')

    t_end = time()
    print('inserted all the data, took %.3f second' % (t_end - t_start))

    # df = pq.read_table(parquet_file).to_pandas()

    # # df = next(df_iter)

    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # df.to_sql(name=table_name, con=engine, if_exists='append')

    # t_end = time()
    # print('inserted all the data, took %.3f second' % (t_end - t_start))

    # while True: 
    #     t_start = time()

    #     try:
    #         df = next(df_iter)
    #     except StopIteration:
    #         print("completed")
    #         break

    #     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #     df.to_sql(name=table_name, con=engine, if_exists='append')

    #     t_end = time()

    #     print('inserted another chunk, took %.3f second' % (t_end - t_start))
