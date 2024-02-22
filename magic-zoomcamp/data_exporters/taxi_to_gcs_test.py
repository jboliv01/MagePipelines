from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import io
import pyarrow as pa
import pandas as pd
from pyarrow import parquet as pq
import os
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    year = kwargs['year']
    month = kwargs['month']

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'dtc-de-zoomcamp-410523-de26a07c6818.json'

    # Bucket name
    bucket_name = 'mage-zoomcamp-jonah-oliver'

    # Service name (if applicable)
    service = kwargs.get('service', 'default_service')

    object_key = f'ny_taxi_data/service={service}/year={year}/month={month}/daily_trips.parquet'

    from google.cloud import storage

    # Google Cloud Storage setup
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)

    # Convert DataFrame to Parquet format in-memory using pyarrow
    table = pa.Table.from_pandas(df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer, coerce_timestamps='us')

    schema = pq.read_schema(parquet_buffer)
    print("Schema of the Parquet file:")
    print(schema)

    # Upload Parquet from in-memory buffer
    blob.upload_from_string(parquet_buffer.getvalue(), content_type='application/octet-stream')

    # # Convert DataFrame to CSV in-memory
    # csv_buffer = io.BytesIO()
    # df.to_csv(csv_buffer, index=False)
    # csv_buffer.seek(0)  # Rewind the buffer

    # Upload CSV from in-memory buffer
    # blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
