import requests
import csv
import json
import os
from google.cloud import storage
import pandas as pd
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'manindejit',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(1),
    'retry_delay': timedelta(minutes=5),
}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Maninderjit Singh\Source\Repos\maninderjitsingh07\db_test\Airflow\cric_icc_stats\GCP_credentials.json'
url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"
querystring = {"formatType":"test"}
headers = {
	"X-RapidAPI-Key": "8dfee58215msh8fcfd92cc1a48f2p1e5c92jsn6169bab1275c",
	"X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}
field_names = ['rank', 'name', 'country','rating','points','lastUpdatedOn']
bucket_name = 'raw_bucket_cric_icc_ranking_data'
FILE_NAME="cricbuzz_rank_info.csv"
STAGING_DATASET="cric_info"


# Get project information 
def get_project_info():
    with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'r') as fp:
        credentials = json.load(fp)
        project_id = credentials['project_id']
    return project_id

def get_data():
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = response.json().get('rank', []) # get rank data 
        df=pd.DataFrame(data)[field_names] # convert ot dataframe 
        df=df.to_csv(index=False).encode() # convert to csv 
        # Create a storage client
        storage_client = storage.Client()  
        bucket=storage_client.bucket(bucket_name)
        # Upload the data to the selected bucket
        blob = bucket.blob(FILE_NAME)
        blob.upload_from_string(df)
        print(f"data sucessfully uploadesd to {bucket_name}")

with DAG('cric_rank_info',
         start_date=days_ago(1), 
         schedule_interval="@once",
         catchup=False, 
         default_args=default_args, 
         tags=["gcs", "bq"]
) as dag:

        create_bucket = GCSCreateBucketOperator(
            task_id="create_bucket",
            bucket_name=bucket_name,
            project_id=get_project_info(),
        )

        load_data_to_bucket = PythonOperator(
             task_id = 'load_data_to_bucket',
             python_callable = get_data,
        )

        load_to_bq = GCSToBigQueryOperator(
             task_id = 'load_to_bq',
             bucket = bucket_name,
             source_objects = ['stock_data.csv'],
             destination_project_dataset_table = f'{get_project_info()}:{STAGING_DATASET}.rank',
             write_disposition='WRITE_TRUNCATE',
             source_format = 'csv',
             allow_quoted_newlines = 'true',
             skip_leading_rows = 1,
             schema_fields=[
                  {'name': 'rank', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                  {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                  {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
                  {'name': 'rating', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                  {'name': 'points', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                  {'name': 'lastUpdatedOn', 'type': 'DATE', 'mode': 'NULLABLE'},
        ],
        )

        (
        create_bucket
        >> pull_stock_data_to_gcs
        >> load_to_bq
        )

        
