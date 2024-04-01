from time import time
import pandas as pd
from sqlalchemy import create_engine
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    db = params.db
    port = params.port
    table_name = params.table_name

    csv_name = 'output.csv'

    # download the data
    # os.system(f"wget {url} -O {csv_name}")

    # create postgres engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # add dataframe iterator to split data into chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')

    # df variable will contain each chunk in the dataframe using next builtin pointer
    df = next(df_iter)

    # some preprocessing to convert text date into timestamps
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create a table in the postgres database with the headers of the dataframe
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # while loop to insert data into pg DB
    while True:
        df = next(df_iter)
        t_begin = time()

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()

        print(f'inserted another chunk...., took {t_end - t_begin:.3f}')


if __name__ == '__main__':
    # init argument parser to get engine args
    parser = argparse.ArgumentParser(description='Ingest data from csv to postgres.')

    # Args => user, password, host, port, DB name, table name, csv url
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')

    args = parser.parse_args()
    main(args)
