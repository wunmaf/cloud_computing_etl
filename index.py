import boto3
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from utils.heper import create_bucket
from configparser import ConfigParser
from utils.constants import db_tables

config = ConfigParser()
config.read('.env')

region = config['aws']['region']
bucket_name = config['aws']['bucket_name']
access_key =config ['aws']['access_key']
secret_key = config['aws']['secret_key']

host = config['DB_CRED']['host']
user = config ['DB_CRED']['user']
password = config['DB_CRED']['password']
database = config['DB_CRED']['database']

#step1 : create bucket using boto3
create_bucket(access_key,secret_key,bucket_name)



#step 2: Extract from the database to Datalake (s3)

conn = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database}')
s3_path = 's3://{}/{}.csv'

#create dataframe

for table in db_tables:
    query = f'SELECT * FROM {table}'
    df = pd.read_sql_query(query,conn)


#saving the content of the table into csv
    df.to_csv(
        s3_path.format(bucket_name, table)
        , index=False
        , storage_options={
            'key': access_key
            , 'secret': secret_key}
)