#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    parquet_name = 'output.parquet'

    # download the PARQUET file
    os.system(f"wget {url} -O {parquet_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    trips = pq.read_table(parquet_name)

    df= trips.to_pandas()

    #df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    
    t_start = time()

    df.to_sql(name=table_name, con=engine, if_exists='replace', chunksize=100000)

    t_end = time()

    print('inserted, took %.3f second' % (t_end - t_start))
        


if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)