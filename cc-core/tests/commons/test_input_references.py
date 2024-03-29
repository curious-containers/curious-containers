import pytest

from cc_core.commons.exceptions import InvalidInputReference
from cc_core.commons.input_references import resolve_input_references
from cc_core.commons.command_builder import generate_command

INPUT_LIST_TO_REFERENCE = {
    'a_file': [
        {
            'basename': 'a_file',
            'class': 'File',
            'connector': {
                'access': {
                    'method': 'GET',
                    'url': 'https://raw.githubusercontent.com/curious-containers/vagrant-quickstart/master/in.txt'
                },
                'command': 'red-connector-http'
            },
            'dirname': '/tmp/red/inputs',
            'nameext': '',
            'nameroot': 'a_file',
            'path': '/tmp/red/inputs/a_file'
        }
    ]
}

INPUT_TO_REFERENCE = {
    'a_file': {
        'basename': 'a_file',
        'class': 'File',
        'connector': {
            'access': {
                'method': 'GET',
                'url': 'https://raw.githubusercontent.com/curious-containers/vagrant-quickstart/master/in.txt'
            },
            'command': 'red-connector-http'
        },
        'dirname': '/tmp/red/inputs/146dbc18-940d-4384-aaa7-073eb4402b51',
        'nameext': '',
        'nameroot': 'a_file',
        'path': '/tmp/red/inputs/146dbc18-940d-4384-aaa7-073eb4402b51/a_file',
        'size': 1000
    }
}


def test_bracket_double_quote():
    glob = 'PRE-$(inputs["a_file"]["basename"])-POST'
    result = resolve_input_references(glob, INPUT_TO_REFERENCE)

    assert result == 'PRE-a_file-POST'


def test_bracket_single_quote():
    glob = 'PRE-$(inputs[\'a_file\'][\'basename\'])-POST'
    result = resolve_input_references(glob, INPUT_TO_REFERENCE)

    assert result == 'PRE-a_file-POST'


def test_bracket_dots():
    glob = 'PRE-$(inputs.a_file.basename)-POST'
    result = resolve_input_references(glob, INPUT_TO_REFERENCE)

    assert result == 'PRE-a_file-POST'


def test_file_list():
    glob = 'PRE-$(inputs.a_file[0].basename)-POST'
    result = resolve_input_references(glob, INPUT_LIST_TO_REFERENCE)

    assert result == 'PRE-a_file-POST'


def test_could_not_resolve_attribute():
    glob = '$(inputs.a_file.invalid)'
    with pytest.raises(InvalidInputReference):
        resolve_input_references(glob, INPUT_TO_REFERENCE)


def test_not_closed_bracket():
    glob = '$(inputs["a_file.basename)'
    with pytest.raises(InvalidInputReference):
        resolve_input_references(glob, INPUT_TO_REFERENCE)


def test_multiple_references():
    glob = '$(inputs.a_file.basename) - $(inputs.a_file.class)'
    result = resolve_input_references(glob, INPUT_TO_REFERENCE)

    assert result == 'a_file - File'


def test_recursive_references():
    glob = '$(inputs.$(inputs.a_file.basename).basename'
    with pytest.raises(InvalidInputReference):
        resolve_input_references(glob, INPUT_TO_REFERENCE)


def test_recursive_attributes():
    glob = '$(inputs["inputs["a_file"]"].basename)'
    with pytest.raises(InvalidInputReference):
        resolve_input_references(glob, INPUT_TO_REFERENCE)


def test_string_index_and_dot():
    glob = '$(inputs["a_file"].basename)'
    result = resolve_input_references(glob, INPUT_TO_REFERENCE)

    assert result == 'a_file'


def test_string_index_in_list():
    glob = '$(inputs.a_file[invalid].basename)'
    with pytest.raises(InvalidInputReference):
        resolve_input_references(glob, INPUT_LIST_TO_REFERENCE)


def test_could_not_resolve_identifier():
    glob = '$(inputs["invalid"])'
    with pytest.raises(InvalidInputReference):
        resolve_input_references(glob, INPUT_TO_REFERENCE)


def test_missing_inputs():
    glob = '$(invalid.a_file.basename)'
    with pytest.raises(InvalidInputReference):
        resolve_input_references(glob, INPUT_TO_REFERENCE)


def test_wrong_order():
    glob = '$(inputs.a_file["basename)"]'
    with pytest.raises(InvalidInputReference):
        resolve_input_references(glob, INPUT_TO_REFERENCE)


def test_int_value():
    glob = '$(inputs.a_file.size)'
    result = resolve_input_references(glob, INPUT_TO_REFERENCE)

    assert result == '1000'


def test_generate_command():
    """
    This test case verifies that the generate_command function produces the expected command when
    provided with a base command, an empty list of CLI arguments, and a batch of inputs.
    The expected command in this test case is the same as the base command since no CLI arguments
    are specified.
    If the generated command matches the expected command, the test case passes. Otherwise, it fails
    Note: Adjust the test data and assertions as needed for different scenarios with different
    CLI arguments and input batches.
    """
    base_command = ["python", "script.py"]
    cli_arguments = []
    batch = {
        "inputs": {
            "arg1": "value1",
            "arg2": "option2",
        }
    }
    expected_command = ["python", "script.py"]
    result = generate_command(base_command, cli_arguments, batch)
    assert result == expected_command