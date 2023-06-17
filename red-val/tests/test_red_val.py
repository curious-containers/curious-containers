from red_val.exceptions import RedValidationError
from red_val.red_variables import _extract_variable_keys, complete_variables
from red_val.red_validation import get_variable_keys, red_validation
from red_val import __version__
from ruamel.yaml import YAML
import pytest
yaml = YAML(typ='safe')


def test_version():
    assert __version__ == '9.1.1'


test_dict = {
    "auth": {
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


def test_extract_variable_keys():
    """
    The test_extract_variable_keys function tests the _extract_variable_keys function by passing 
    in a variable_string, key_string, and protected argument. 
    :param None:
    :return None:
    """
    variable_string = "The {{quick}} brown fox jumps over the lazy dog"
    key_string = "test_dict"
    protected = True
    function_result = (_extract_variable_keys(
        variable_string, key_string, protected))
    for element in function_result:
        function_result = element
        function_result = str(function_result)
    expected_result = "quick"
    assert function_result == expected_result


data = {
    "name": "The {{adjective}} {{noun}}",
    "age": "{{age}}"
}
variables = {
    "adjective": "quick",
    "noun": "brown fox",
    "age": "5"
}


def test_complete_variables():
    """
    Test for the complete_variables function.
    Checks if the function correctly completes variables in the data.
    Test case:
    - Test a dictionary with a string value containing a variable.
    Expected result: The variable is replaced with the corresponding value.
    """
    expected_result = {"name": "The quick brown fox", "age": "5"}
    complete_variables(data, variables)
    assert data == expected_result


def test_red_validation():
    """
    Test for the red_validation function.
    Checks if the function raises error if the data is not in the json format.
    Test case:
    - Test 1: Tests if the input_output.red is in json format.
    Expected result: No error.
    - Test 2: Tests if the data is in json format else raises error.
    Expected result: raises RedValidationError error.
    """
    with open("red_files/input_output.red", 'r') as file:
        input_output = yaml.load(file)
    assert red_validation(red_data=input_output, ignore_outputs=True,
                            container_requirement=False, allow_variables=True) is None
    with pytest.raises(RedValidationError):
        red_validation(red_data=data, ignore_outputs=True,
                            container_requirement=False, allow_variables=True)
