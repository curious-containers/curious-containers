from ruamel.yaml import YAML, YAMLError
import pytest
from cc_core.commons.exceptions import InvalidExecutionEngineArgumentException
from cc_faice.exec.main import _has_outputs, _check_execution_arguments
from cc_core.commons.files import load_and_read
from cc_core.commons.red_secrets import get_secret_values

yaml = YAML(typ='safe')

# Reading all the Red files
def red_create(batch: bool, output: bool):
    """
    This function creates a red file for testing purpose.
    :param batch: If the batch is required
    :type batch: bool
    :param output: If the output is required
    :type output: bool
    :return: A red file consisting the above requirements
    :rtype: Dict[str, Any]
    """
    with open("tests/red_files/initial.red",'r') as file:
        initial_red = yaml.load(file)
    with open("tests/red_files/container.red",'r') as file:
        container_red = yaml.load(file)
    with open("tests/red_files/execution.red",'r') as file:
        execution_red = yaml.load(file)
    if output:
        with open("tests/red_files/outputs.red",'r') as file:
            output_red = yaml.load(file)
    else:
        with open("tests/red_files/no_outputs.red",'r') as file:
            output_red = yaml.load(file)
    if output and batch:
        initial_red['batches'] = output_red
    elif not output and batch:
        initial_red['batches'] = output_red
    elif output and not batch:
        initial_red.update(output_red)
    else:
        initial_red.update(output_red)
    initial_red.update(container_red)
    initial_red.update(execution_red)
    return initial_red


def test_has_outputs():
    """
    Tests the _has_outputs function from cc_faice.exec.main for two red files.
    """
    assert not _has_outputs(red_create(batch=False, output=False)), 'Expected no outputs but found one output in hello_world.red'
    assert  _has_outputs(red_create(batch=False, output=True)), 'Expected outputs but not found output in input_output.red'


def test_check_execution_arguments():
    """
    Tests the _check_execution_arguments function from cc_faice.exec.main.
    Here the function expects the return values as per "ALLOWED_EXECUTION_ENGINE_ARGUMENTS" mentioned
    in cc-faice.cc_faice.exec.main on line no: 161 else raises "InvalidExecutionEngineArgumentException"
    """
    _check_execution_arguments('ccagency',disable_retry=True, disable_connector_validation=True)
    _check_execution_arguments('ccfaice',leave_container=True)
    _check_execution_arguments('ccfaice',preserve_environment=True)
    _check_execution_arguments('ccfaice',gpu_ids=True)
    _check_execution_arguments('ccfaice',disable_pull=True)
    with pytest.raises(InvalidExecutionEngineArgumentException):
        _check_execution_arguments('ccfaice',disable_retry=True, disable_connector_validation=True)
    with pytest.raises(InvalidExecutionEngineArgumentException):
        _check_execution_arguments('ccagency',leave_container=True)
    with pytest.raises(InvalidExecutionEngineArgumentException):
        _check_execution_arguments('ccagency',preserve_environment=True)
    with pytest.raises(InvalidExecutionEngineArgumentException):
        _check_execution_arguments('ccagency',gpu_ids=True)
    with pytest.raises(InvalidExecutionEngineArgumentException):
        _check_execution_arguments('ccagency',insecure=True)
    with pytest.raises(InvalidExecutionEngineArgumentException):
        _check_execution_arguments('ccagency',disable_pull=True)

def test_load_and_read():
    """
    Tests the _load_and_read function from cc_core.commons.files
    In the first section function expects no error in return.
    In the second function expects a YAMLError if the file is not in the form of json or yaml.
    In the third section function expects a OSError in return if the directory is unknown.
    """
    load_and_read(location='tests/red_files/initial.red', var_name="initial")
    with pytest.raises(YAMLError):
        load_and_read(location='tests/red_files/for_test_load_and_read.txt', var_name="initial")
    with pytest.raises(OSError):
        load_and_read(location='test/red_files/initial.red', var_name="initial")

red_message = [
    {
        'inputs':{
            'auth' : {
                'privateKey': "{{avocado_private_key01}}"
            }
        },
        'outputs' :{
            'auth' : {
                'privateKey': "{{avocado_private_key02}}"
            }
        },
        'execution' :{
            'auth' : {
                'privateKey': "{{avocado_private_key03}}"
            }
        }
    }
]
get_secret_values_expected_value = ['{{avocado_private_key01}}',
                                    '{{avocado_private_key02}}',
                                    '{{avocado_private_key03}}']
def test_get_secret_values():
    """
    Tests the get_secret_values function from cc_core.commons.
    Here the function expects the "get_secret_values_expected_value" in return as 
    the secret values are appended by the function "_append_secret_values".
    """
    assert get_secret_values(red_message) == get_secret_values_expected_value
