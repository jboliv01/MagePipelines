import io
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

    try:
        response = requests.get(request_url)
        response.raise_for_status()  # Raises HTTPError for bad requests
        data = io.BytesIO(response.content)
        df = pq.read_table(data).to_pandas()
        print(f"Parquet loaded: {file_name}")
    except requests.HTTPError as e:
        print(f"HTTP Error: {e}")

    return [df, metadata]

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
