import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from typing import Dict, List


@data_loader
def load_data(*args, **kwargs) -> List[List[Dict]]:
    users = []
    metadata = []

    for i in range(3):
        i += 1
        users.append(dict(id=i, name=f'user_{i}'))
        metadata.append(dict(block_uuid=f'for_user_{i}'))

    return [
        users,
        metadata,
    ]
