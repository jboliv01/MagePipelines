import io
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    service = kwargs['service']
    year = kwargs['year']
    print(f'loading {service} taxi data for the year {year}\n')

    data_frames = []

    for i in range(12):
        month = f"{i+1:02d}"
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        request_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}'
        print(f'request url: {request_url}')
        try:
            response = requests.get(request_url)
            response.raise_for_status()  # Raises HTTPError for bad requests
            data = io.BytesIO(response.content)
            df = pq.read_table(data).to_pandas()
            data_frames.append(df)
            print(f"Parquet loaded: {file_name}")
        except requests.HTTPError as e:
            print(f"HTTP Error: {e}")
            # Optionally, handle the error (e.g., by breaking the loop or re-trying)

    # Concatenate all dataframes
    combined_df = pd.concat(data_frames, ignore_index=True)
    return combined_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
