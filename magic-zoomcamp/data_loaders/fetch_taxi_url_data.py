if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from typing import Dict, List

@data_loader
def load_parquet_url(*args, **kwargs) -> List[List[Dict]]:
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    download_info = []
    
    year = kwargs.get('year', '2019')
    service = kwargs.get('service', 'fhv')
    
    for i in range(3):
        month = '0'+str(i+1)
        month = month[-2:]
    
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        request_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}'
        print(f'request url: {request_url}')

        download_info.append(dict(file_name=f'{file_name}', request_url=f'{request_url}', year=f'{year}', month=f'{month}', service=f'{service}'))


    return [download_info]
