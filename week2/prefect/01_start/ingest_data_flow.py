import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time
import os
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, tags=['extract'], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    # downloading the data
    csv_name = 'output.csv'
    os.system(f'wget {url} -O {csv_name}.gz')
    os.system(f'gunzip {csv_name}.gz')

    # read the downloaded data
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)
    df = next(df_iter)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    return df, csv_name

@task(log_prints=True)
def transform_data(data):
    print(f'pre: missing passenger count: {data["passenger_count"].isin([0]).sum()}')
    
    data = data[data['passenger_count'] != 0]
    
    print(f'post: missing passenger count: {data["passenger_count"].isin([0]).sum()}')
    
    return data


@task(log_prints=True, retries=3)
def load_data(table_name, data, csv_name):
    # # init connection
    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # # start ingest
    # data.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    # data.to_sql(name=table_name, con=engine, if_exists='append')
    
    # ingest using prefect block
    connection_block = SqlAlchemyConnector.load('postgres-connector')
    with connection_block.get_connection(begin=False) as engine:
        data.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        data.to_sql(name=table_name, con=engine, if_exists='append')

    # cleanup workdir
    os.system(f'rm {csv_name}')


@flow(name='Subflow', log_prints=True)
def log_subflow(table_name):
    print(f'Logging Subflow for: {table_name}')


@flow(name='Ingest Data')
def main_flow(table_name='green_taxi_data'):  
    csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'

    log_subflow(table_name)

    raw_data, output_file = extract_data(csv_url)
    data = transform_data(raw_data)
    load_data(table_name, data, output_file)



if __name__ == '__main__':    
    
    main_flow(table_name='green_taxi_trips')