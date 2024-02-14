if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
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
    import pandas as pd

    # metadata_dict = kwargs['metadata']

    # Define all potential datetime columns
    datetime_columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']

    # Filter out the columns that actually exist in the DataFrame
    existing_datetime_columns = [col for col in datetime_columns if col in df.columns]

    # Apply the formatting only to existing columns
    if existing_datetime_columns:
        df[existing_datetime_columns] = df[existing_datetime_columns].apply(lambda x: pd.to_datetime(x).dt.strftime('%Y-%m-%d %H:%M:%S'))

    return df


# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'
