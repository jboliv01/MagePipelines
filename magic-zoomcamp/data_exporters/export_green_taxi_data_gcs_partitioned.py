import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'dtc-de-zoomcamp-410523-de26a07c6818.json'
project_id = 'dtc-de-zoomcamp-410523'
bucket_name = 'mage-zoomcamp-jonah-oliver'
object_key = 'ny_taxi_data.parquet'
table_name = 'yellow_taxi'
root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    # creating a new date column from the existing datetime column
    
    table = pa.Table.from_pandas(df)
    print(table)
    gcs = pa.fs.GcsFileSystem()

    # pq.write_to_dataset(
    #     table,
    #     root_path=root_path,
    #     partition_cols=['tpep_pickup_date'],
    #     filesystem=gcs
    # )