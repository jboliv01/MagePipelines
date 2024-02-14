import io
import pandas as pd
import requests
import gzip
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(df,*args, **kwargs):
    metadata = []
    request_url = df['request_url']
    file_name_csv = df['file_name']
    year= df['year']
    service = df['service']
    metadata.append(dict(object_key=f'{file_name_csv}', year=f'{year}', service=f'{service}'))
    
    df = pd.read_csv(request_url,compression='gzip')
    
    return [df, metadata]