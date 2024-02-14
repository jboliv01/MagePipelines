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
