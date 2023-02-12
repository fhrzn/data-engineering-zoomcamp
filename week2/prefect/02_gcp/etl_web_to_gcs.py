from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url):
    
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df):
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')

    return df

@task()
def write_local(df, color, dataset_file):
    path = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')

    return path


@task()
def write_to_gcs(path):
    
    gcs_block = GcsBucket.load("gcs-bucket-connector")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )    

@flow()
def etl_web_to_gcs():
    """The main ETL function"""
    
    color = 'green'
    year = 2020
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_to_gcs(path)


if __name__ == '__main__':
    etl_web_to_gcs()