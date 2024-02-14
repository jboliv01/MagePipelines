if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)

    """
    # Specify your transformation logic here

    zero_passengers_df = data[data['passenger_count'].isin([0])]
    zero_passengers_count = zero_passengers_df['passenger_count'].count()
    non_zero_passengers_df = data[data['passenger_count'] > 0]
    non_zero_passengers_count = non_zero_passengers_df['passenger_count'].count()
    print(f'Preprocessing: records with zero passengers: {zero_passengers_count}')
    print(f'Preprocessing: records with 1 passenger or more: {non_zero_passengers_count}')

    df = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    df_2 = data[(data['passenger_count'] == 0) | (data['trip_distance'] == 0)]
    print(df.shape)
    print(df_2.shape)

    df['tpep_pickup_date'] = df['tpep_pickup_datetime'].dt.date

    df.columns = (df.columns
        .str.replace(' ', '_')
        .str.lower()      
        )
   
    print(df['vendorid'].value_counts())

    non_snake_case_count = sum(
        original != normalized for original, normalized in zip(data.columns, df.columns))
    print(f'non snake case columns: {non_snake_case_count})')
 
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendorid' in output.columns, 'vendorid column is missing'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'

