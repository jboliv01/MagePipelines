if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from typing import Dict, List
import uuid


@transformer
def transform(data: Dict, *args, **kwargs) -> List[Dict]:
    data['token'] = uuid.uuid4().hex
    return [data]



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
