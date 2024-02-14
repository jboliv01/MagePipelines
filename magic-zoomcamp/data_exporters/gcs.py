from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import pandas as pd

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

    bucket_name = 'mage-zoomcamp-jonah-oliver'
    object_key = kwargs['object_key']
    year = kwargs['year']
    service = kwargs['service']
    print(object_key)
    # chunk_size = 1000000 # Adjust chunk size according to ur memory/ network speed

    # num_chunks = -(-len(df) // chunk_size)

    # for i in range(num_chunks):
    #     start_idx = i * chunk_size
    #     end_idx = min((i + 1) * chunk_size, len(df))
    #     chunk_df = df.iloc[start_idx:end_idx]

    #     GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
    #         chunk_df,
    #         bucket_name,
    #         f"mage-slack/{service}/{year}/part_{i}_{object_key}"
    #     )
