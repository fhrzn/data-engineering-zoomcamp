import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time
import os

def ingest_data(user, password, host, port, db, table_name, url):    

    csv_name = 'output.csv'
    os.system(f'wget {url} -O {csv_name}.gz')
    os.system(f'gunzip {csv_name}.gz')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)
    df = next(df_iter)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        try:
            df = next(df_iter)
            
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

            df.to_sql(name=table_name, con=engine, if_exists='append')
        except:
            break

        t_end = time()

        print('Inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == '__main__':    

    user = 'postgres'
    password = 'admin'
    host = 'localhost'
    port = '5433'
    db = 'ny_taxi'
    table_name = 'green_taxi_trips'
    csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'

    ingest_data(user, password, host, port, db, table_name, csv_url)