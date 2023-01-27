import configparser
import json

import boto3
from datetime import date
import pandas as pd 
from sqlalchemy import create_engine

# Instanciate configuration items for the db connection
config = configparser.ConfigParser()
config.read('.config.ini')

# importing dataset to upload into S3 and Postgress
df = pd.read_json('output\enriched_dataset_booking.json')
csv_file = df.explode(['days','temp_max', 'temp_min','pre_sum'])


def S3_loader(file_name, your_bucket_name):
        
    # initiate the session 
    session = boto3.Session(aws_access_key_id=config['AWS']['ACCESSKEY'], 
                        aws_secret_access_key=config['AWS']['SECRETACCESSKEY'])
    s3 = boto3.client('s3')

    # upload csv into s3 Bucket
    today = date.today()
    s3.put_object(Key=f'{today}_{file_name}.csv',Bucket= your_bucket_name, Body=csv_file.to_csv(index=False))
    print(f'Uploaded {today}_{file_name}.csv to S3 bucket: {your_bucket_name}')
        
def postgres_loader():
    DBHOST = config['DATABASE']['DBHOST']
    DBUSER = config['DATABASE']['DBUSER']
    DBPASS = config['DATABASE']['DBPASS']
    DBNAME = config['DATABASE']['DBNAME']
    PORT = config['DATABASE']['PORT']
    engine =  create_engine(f"postgresql+psycopg2://{DBUSER}:{DBPASS}@{DBHOST}:{PORT}/{DBNAME}", echo=True)
    
    # Push the final dataset in to postgresql database
    csv_file.to_sql("booking_data", engine,if_exists='append')
    print('Uploaded dataset to booking_data table') 


def main():
    S3_loader('final_dataset_booking', 'jedha-block1-booking')
    postgres_loader()


if __name__ == "__main__":
    main()

