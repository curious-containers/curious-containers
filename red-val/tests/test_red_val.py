from red_val import __version__
from ruamel.yaml import YAML
yaml = YAML(typ='safe')
from red_val.red_validation import get_variable_keys

def test_version():
    assert __version__ == '9.1.1'

test_dict = {
    "auth":{
        "username": "root",
        "privateKey": "{{private_key}}"
    }
}
actual_result = "private_key"

def test_get_variable_keys():
    """
    Test the `get_variable_keys` function with a sample dictionary that has a template key.
    The function asserts that the first element in the resulting list is the expected template key.
    Functions tested:
    - get_variable_keys_impl: called by get_variable_keys to iterate over  
        the data and collect variable keys.
    - unique_variable_keys: called by get_variable_keys to return a list of unique variable keys.
    Input:
    - test_dict: a dictionary with a template key in the "privateKey" field.
    Assert:
    - The first element in the resulting list of variable keys matches the 
        expected key "private_key".
    """
    function_result = get_variable_keys(test_dict)
    function_result = str(function_result[0])
    assert function_result == actual_result
