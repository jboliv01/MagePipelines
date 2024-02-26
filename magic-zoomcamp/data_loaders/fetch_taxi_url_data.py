if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.io.file import FileIO
import os
import pandas as pd
import pyarrow as pa
import requests
import tempfile


@data_loader
def load_parquet_from_url(download_info, *args, **kwargs) -> pd.DataFrame:
    file_io = FileIO(verbose=True)  # Enables verbose output
    metadata = []
    request_url = download_info['request_url']
    file_name = download_info['file_name']
    year = download_info['year']
    month = download_info['month']
    service = download_info['service']
    metadata.append(dict(object_key=f'{file_name}', year=f'{year}', month=f'{month}', service=f'{service}'))
    metadata_dict = dict(object_key=f'{file_name}', year=f'{year}', month=f'{month}', service=f'{service}')
    
    if service == 'fhv':
        taxi_schema = pa.schema([
            ('dispatching_base_num', pa.string()),
            ('pickup_datetime', pa.string()),
            ('dropOff_datetime', pa.string()),
            ('PUlocationID', pa.float64()),
            ('DOlocationID', pa.float64()),
            ('SR_Flag', pa.float64()),
            ('Affiliated_base_number', pa.string()),
        ])
    elif service == 'green':
        taxi_schema = pa.schema([
            ('lpep_pickup_datetime', pa.string()),
            ('lpep_dropoff_datetime', pa.string()),
            ('VendorID', pa.int64()),
            ('store_and_fwd_flag', pa.string()),
            ('RatecodeID', pa.int64()),
            ('PULocationID', pa.int64()),
            ('DOLocationID', pa.int64()),
            ('passenger_count', pa.int64()),
            ('trip_distance', pa.float64()),
            ('fare_amount', pa.float64()),
            ('extra', pa.float64()),
            ('mta_tax', pa.float64()),
            ('tip_amount', pa.float64()),
            ('tolls_amount', pa.float64()),
            ('ehail_fee', pa.float64()),
            ('improvement_surcharge', pa.float64()),
            ('total_amount', pa.float64()),
            ('payment_type', pa.float64()),
            ('trip_type', pa.float64()),
            ('congestion_surcharge', pa.float64())
        ])
    else:
        taxi_schema = pa.schema([
            ('tpep_pickup_datetime', pa.string()),
            ('tpep_dropoff_datetime', pa.string()),
            ('VendorID', pa.int64()),
            ('store_and_fwd_flag', pa.string()),
            ('RatecodeID', pa.int64()),
            ('PULocationID', pa.int64()),
            ('DOLocationID', pa.int64()),
            ('passenger_count', pa.int64()),
            ('trip_distance', pa.float64()),
            ('fare_amount', pa.float64()),
            ('extra', pa.float64()),
            ('mta_tax', pa.float64()),
            ('tip_amount', pa.float64()),
            ('tolls_amount', pa.float64()),
            ('ehail_fee', pa.float64()),
            ('improvement_surcharge', pa.float64()),
            ('total_amount', pa.float64()),
            ('payment_type', pa.float64()),
            ('trip_type', pa.float64()),
            ('congestion_surcharge', pa.float64())
        ])


    def safe_convert_to_timestamp(column):
        # Convert to datetime, coerce errors
        dt_column = pd.to_datetime(column, errors='coerce')
        # Maximum representable timestamp in nanoseconds since 1970-01-01
        max_timestamp_ns = 2**63 - 1
        # Convert to nanoseconds for comparison
        dt_column_ns = dt_column.astype('int64')
        # Create a boolean mask for valid dates (both year and nanosecond range)
        mask = (dt_column_ns < max_timestamp_ns)
        # For dates outside the valid range, set to NaT (not a time)
        dt_column[~mask] = pd.NaT

        return dt_column


    try:
        response = requests.get(request_url)
        response.raise_for_status()
        # Write the content to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
            tmp.write(response.content)
            tmp.flush()
    
            # Use FileIO to load the Parquet file into a DataFrame
            df = FileIO(verbose=True).load(tmp.name, format='parquet', schema=taxi_schema)

            if service == 'fhv':
                df['pickup_datetime'] = safe_convert_to_timestamp(df['pickup_datetime'])
                df['dropOff_datetime'] = safe_convert_to_timestamp(df['dropOff_datetime'])
            elif service == 'green':
                df['lpep_pickup_datetime'] = safe_convert_to_timestamp(df['lpep_pickup_datetime'])
                df['lpep_dropoff_datetime'] = safe_convert_to_timestamp(df['lpep_dropoff_datetime'])
            else: 
                df['tpep_pickup_datetime'] = safe_convert_to_timestamp(df['tpep_pickup_datetime'])
                df['tpep_dropoff_datetime'] = safe_convert_to_timestamp(df['tpep_dropoff_datetime'])

        # Now df contains the DataFrame loaded from the Parquet file
        return [df, metadata]

    except requests.HTTPError as e:
        print(f"HTTP Error: {e}")
    
    

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'