if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here

    init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

    download_info = []
    
    year = kwargs.get('year')
    service = kwargs.get('service')
    

    for i in range(12):
        month = '0'+str(i+1)
        month = month[-2:]

        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        file_name_csv = f"{service}_tripdata_{year}-{month}.csv"

        request_url = f"{init_url}{service}/{file_name}"

        download_info.append(dict(file_name=f'{file_name_csv}', request_url=f'{request_url}', year=f'{year}', service=f'{service}'))


    return [download_info]
