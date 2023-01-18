import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time
import os

def main(args):    

    csv_name = 'output.csv'
    os.system(f'wget {args.url} -O {csv_name}.gz')
    os.system(f'gunzip {csv_name}.gz')

    engine = create_engine(f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)
    df = next(df_iter)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    df.head(n=0).to_sql(name=args.table_name, con=engine, if_exists='replace')
    df.to_sql(name=args.table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        try:
            df = next(df_iter)
            
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

            df.to_sql(name=args.table_name, con=engine, if_exists='append')
        except:
            break

        t_end = time()

        print('Inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='user name for postgres')
    parser.add_argument('--host', help='user name for postgres')
    parser.add_argument('--port', help='user name for postgres')
    parser.add_argument('--db', help='user name for postgres')
    parser.add_argument('--table_name', help='user name for postgres')
    parser.add_argument('--url', help='user name for postgres')

    args = parser.parse_args()
    main(args)