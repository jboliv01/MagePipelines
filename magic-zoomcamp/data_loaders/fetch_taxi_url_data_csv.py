import io
import pandas as pd
import requests
import gzip
from mage_ai.io.file import FileIO
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(df,*args, **kwargs):
    metadata = []
    request_url = df['request_url']
    file_name_csv = df['file_name']
    year = df['year']
    month = df['month']
    service = df['service']
    metadata.append(dict(object_key=f'{file_name_csv}', year=f'{year}', month=f'{month}', service=f'{service}'))

    print(service)

    if service == 'fhv':
        parse_taxi_dates = ['pickup_datetime', 'dropOff_datetime']
        taxi_schema = {
            'dispatching_base_num': 'string',
            'PUlocationID': 'Int64',
            'DOlocationID': 'Int64',
            'SR_Flag': 'string',
            'Affiliated_base_number': 'string'
        }
    elif service == 'green':
        parse_taxi_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        taxi_schema = {
            'VendorID': 'Int64',
            'store_and_fwd_flag': 'string',
            'RatecodeID': 'Int64',
            'PULocationID': 'Int64',
            'DOLocationID': 'Int64',
            'passenger_count': 'Int64',
            'trip_distance': 'float64',
            'fare_amount': 'float64',
            'extra': 'float64',
            'mta_tax': 'float64',
            'tip_amount': 'float64',
            'tolls_amount': 'float64',
            'ehail_fee': 'float64',
            'improvement_surcharge': 'float64',
            'total_amount': 'float64',
            'payment_type': 'Int64',
            'trip_type': 'Int64',
            'congestion_surcharge': 'float64'
        }
    else:
        parse_taxi_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        taxi_schema = {
            'VendorID': 'Int64',
            'store_and_fwd_flag': 'string',
            'RatecodeID': 'Int64',
            'PULocationID': 'Int64',
            'DOLocationID': 'Int64',
            'passenger_count': 'Int64',
            'trip_distance': 'float64',
            'fare_amount': 'float64',
            'extra': 'float64',
            'mta_tax': 'float64',
            'tip_amount': 'float64',
            'tolls_amount': 'float64',
            'ehail_fee': 'float64',
            'improvement_surcharge': 'float64',
            'total_amount': 'float64',
            'payment_type': 'Int64',
            'trip_type': 'Int64',
            'congestion_surcharge': 'float64'
        }
    
        
    df = pd.read_csv(request_url, sep=',', compression='gzip', dtype=taxi_schema, parse_dates=parse_taxi_dates)
        
    return [df, metadata]