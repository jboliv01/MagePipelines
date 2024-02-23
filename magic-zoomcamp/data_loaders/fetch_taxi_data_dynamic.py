import io
from mage_ai.io.file import FileIO
import pandas as pd
from pandas import DataFrame
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, List

from mage_ai.data_preparation.variable_manager import set_global_variable

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
   



@data_loader
def load_parquet_from_url(download_info, *args, **kwargs) -> List[List[Dict]]:
    #print(download_info)
    metadata = []
    request_url = download_info['request_url']
    file_name = download_info['file_name']
    year = download_info['year']
    month = download_info['month']
    service = download_info['service']
    metadata.append(dict(object_key=f'{file_name}', year=f'{year}', month=f'{month}', service=f'{service}'))
    metadata_dict = dict(object_key=f'{file_name}', year=f'{year}', month=f'{month}', service=f'{service}')

    # taxi_dtypes = {
        # 'VendorID': 'Int64',
        # 'store_and_fwd_flag': 'str',
        # 'RatecodeID': 'Int64',
        # 'PULocationID': 'Int64',
        # 'DOLocationID': 'Int64',
        # 'passenger_count': 'Int64',
        # 'trip_distance': 'float64',
        # 'fare_amount': 'float64',
        # 'extra': 'float64',
        # 'mta_tax': 'float64',
        # 'tip_amount': 'float64',
        # 'tolls_amount': 'float64',
        # 'ehail_fee': 'float64',
        # 'improvement_surcharge': 'float64',
        # 'total_amount': 'float64',
        # 'payment_type': 'float64',
        # 'trip_type': 'float64',
        # 'congestion_surcharge': 'float64'
    # }

   
    taxi_schema = pa.schema([
        ('dispatching_base_num', pa.string()),
        ('pickup_datetime', pa.string()),
        ('dropOff_datetime', pa.string()),
        ('PUlocationID', pa.float64()),
        ('DOlocationID', pa.float64()),
        ('SR_Flag', pa.float64()),
        ('Affiliated_base_number', pa.string()),

        # Add timestamp fields with their specific precision if they are present in your data
    ])

    # Function to safely convert string to timestamp and handle out-of-bounds values
    def safe_convert_to_timestamp(column, max_year=9999):
        return pd.to_datetime(column, errors='coerce').apply(
            lambda x: x if pd.isnull(x) or x.year < max_year else x // 1000
        )

    try:
        response = requests.get(request_url)
        response.raise_for_status()
        data = io.BytesIO(response.content)
        print(f"Parquet loaded: {file_name}")
        
        # Read the Parquet file into a PyArrow table with modified schema
        table = pq.read_table(data, schema=taxi_schema, buffer_size=4194304)
        # table = pq.read_table(data)

        # Convert to Pandas DataFrame
        df = table.to_pandas()
            
        # # Convert and clean the timestamp columns
        df['pickup_datetime'] = safe_convert_to_timestamp(df['pickup_datetime'])
        df['dropOff_datetime'] = safe_convert_to_timestamp(df['dropOff_datetime'])

        # # nat_count_pickup = df['pickup_datetime'].isna().sum()
        # # nat_count_dropoff = df['dropOff_datetime'].isna().sum()
        # # nat_records = df[df['pickup_datetime'].isna() | df['dropOff_datetime'].isna()]
        # # print(f"Number of NaT values in 'pickup_datetime': {nat_count_pickup}")
        # # print(f"Number of NaT values in 'dropOff_datetime': {nat_count_dropoff}")
        # print(df['dropOff_datetime'].max())

        print(df.dtypes)

        return [df, metadata]

    except requests.HTTPError as e:
        print(f"HTTP Error: {e}")

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
