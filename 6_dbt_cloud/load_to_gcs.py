import pandas as pd
import pyarrow
from google.cloud import storage
import os

init_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
CREDENTIALS_FILE = './gcs.json'
BUCKET_NAME = 'nyc-tl-data-01'

def fetch(dataset_url: str, year:str, color: str) -> pd.DataFrame: 
    if color == 'fhv':
        dtypes = {'dispatching_base_num':'str', 'PUlocationID':'Int64', 'DOlocationID':'Int64', 'SR_Flag':'Int64'}
        date_cols = ['pickup_datetime', 'dropOff_datetime']
    elif color == 'green':
        dtypes = {'VendorID':'Int64', 'store_and_fwd_flag':'str', 'RatecodeID':'Int64', 'PULocationID':'Int64', \
                  'DOLocationID':'Int64', 'passenger_count':'Int64', 'ehail_fee':'Float64', 'payment_type':'Int64', 'trip_type':'Int64'}
        date_cols = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    else:
        dtypes = {'VendorID':'Int64', 'store_and_fwd_flag':'str', 'RatecodeID':'Int64', 'PULocationID':'Int64', 'DOLocationID':'Int64', \
                  'passenger_count':'Int64', 'payment_type':'Int64'}
        date_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        
    df = pd.read_csv(dataset_url, compression='gzip', parse_dates=date_cols, dtype=dtypes)
    return df

def upload_to_gcs(object_name: str, local_file: os.path):
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(color: str, year: str):
    for i in range(1,13):
        month = f'{i:02d}'
        filename = f'{color}_tripdata_{year}-{month}'
        dataset_url = f'{init_url}/{color}/{filename}.csv.gz'
        df = fetch(dataset_url, year, color)

        cwd = os.getcwd()
        file_path = os.path.join(cwd, f'{filename}.parquet')
        df.to_parquet(file_path, engine='pyarrow')

        upload_to_gcs(f'{color}/{filename}.parquet', file_path)
        print(f'GCS: {color}/{filename}.parquet')

web_to_gcs('green', '2019')
# web_to_gcs('green', '2020')
# web_to_gcs('yellow', '2019')
# web_to_gcs('yellow', '2020')
# web_to_gcs('fhv', '2019')


