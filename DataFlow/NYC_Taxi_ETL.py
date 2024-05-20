import apache_beam as beam
from datetime import date
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

custom_gcs_temp_location = "gs://dataflow_temp_storage_raw"
input_data_location = "gs://data_lake_raw/yellow_tripdata_2024-01.parquet"
output_table = "natural-element-423604-f9:nyctaxi.yellow_tripdata"
schema = """vendor_name:STRING,
pickup_datetime:DATETIME,
pickup_datetime_day:INTEGER,
pickup_datetime_month:INTEGER,
pickup_datetime_year:INTEGER,
dropoff_datetime:DATETIME,
dropoff_datetime_day:INTEGER,
dropoff_datetime_month:INTEGER,
dropoff_datetime_year:INTEGER,
passenger_count:FLOAT64,
trip_distance:FLOAT64,
RatecodeID:STRING,
store_and_fwd_flag:STRING,
PULocationID:INTEGER,
DOLocationID:INTEGER,
payment_type:STRING,
fare_amount:FLOAT64,
extra:FLOAT64,
mta_tax:FLOAT64,
tip_amount:FLOAT64,
tolls_amount:FLOAT64,
improvement_surcharge:FLOAT64,
total_amount:FLOAT64,
congestion_surcharge:FLOAT64,
airport_fee:FLOAT64"""


def process_columns(element):
    data = {}
    data['vendor_name'] = "Creative Mobile Technologies, LLC" if element['VendorID'] == 1 else "VeriFone Inc." if element['VendorID'] == 2 else None
    data['pickup_datetime'] = element['tpep_pickup_datetime']
    data['pickup_datetime_day'] = element['tpep_pickup_datetime'].day
    data['pickup_datetime_month'] = element['tpep_pickup_datetime'].month
    data['pickup_datetime_year'] = element['tpep_pickup_datetime'].year
    data['dropoff_datetime'] = element['tpep_dropoff_datetime']
    data['dropoff_datetime_day'] = element['tpep_pickup_datetime'].day
    data['dropoff_datetime_month'] = element['tpep_pickup_datetime'].month
    data['dropoff_datetime_year'] = element['tpep_pickup_datetime'].year
    data['passenger_count'] = element['passenger_count']
    data['trip_distance'] = element['trip_distance']
    data['RatecodeID'] = "Standard rate" if element['RatecodeID'] == 1.0 else "JFK" if element['RatecodeID'] == 2.0 else "Newark" if element['RatecodeID'] == 3.0 else "Nassau or Westchester" if element['RatecodeID'] == 4.0 else "Negotiated fare" if element['RatecodeID'] == 5.0 else "Group ride" if element['RatecodeID'] == 6.0 else None
    data['store_and_fwd_flag'] = "Store and forward trip" if element['store_and_fwd_flag'] == "Y" else "Not a store and forward trip" if element['store_and_fwd_flag'] == "N" else None
    data['PULocationID'] = element['PULocationID']
    data['DOLocationID'] = element['DOLocationID']
    data['payment_type'] = "Credit card" if element['payment_type'] == 1.0 else "Cash" if element['payment_type'] == 2.0 else "No charge" if element['payment_type'] == 3.0 else "Dispute" if element['payment_type'] == 4.0 else "Unknown" if element['payment_type'] == 5.0 else "Voided trip" if element['payment_type'] == 6.0 else None
    data['fare_amount'] = element['fare_amount']
    data['extra'] = element['extra']
    data['mta_tax'] = element['mta_tax']
    data['tip_amount'] = element['tip_amount']
    data['tolls_amount'] = element['tolls_amount']
    data['improvement_surcharge'] = element['improvement_surcharge']
    data['total_amount'] = element['total_amount']
    data['congestion_surcharge'] = element['congestion_surcharge']
    data['airport_fee'] = element['Airport_fee']
    return data


beam_options = PipelineOptions()
with beam.Pipeline() as pipeline:

    input_data = pipeline | 'Read' >> ReadFromParquet(input_data_location)

    processed_data = input_data | 'ProcessColumns' >> beam.Map(process_columns)

    processed_data  | 'WriteToBigQuery' >> WriteToBigQuery(
         table=output_table,
         schema=schema,
         custom_gcs_temp_location=custom_gcs_temp_location,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

