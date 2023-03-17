from ruamel.yaml import YAML, YAMLError
import pytest
from cc_core.commons.exceptions import InvalidExecutionEngineArgumentException
from cc_faice.exec.main import _has_outputs, _check_execution_arguments
from cc_core.commons.files import load_and_read

yaml = YAML(typ='safe')

# Reading all the Red files
def red_create(batch: bool, output: bool):
    """
    This fucntion creates a red file for testing pupose.
    :param batch: If the batch is required
    :type batch: bool
    :param output: If the output is required
    :type output: bool
    :return: A red file consisting the above requirments
    :rtype: Dict[str, Any]
    """
    with open("tests/red_files/initial.red",'r') as f:
        initial_red = yaml.load(f)
    with open("tests/red_files/container.red",'r') as f:
        container_red = yaml.load(f)
    with open("tests/red_files/execution.red",'r') as f:
        execution_red = yaml.load(f)
    if output:
        with open("tests/red_files/outputs.red",'r') as f:
            output_red = yaml.load(f)
    else:
        with open("tests/red_files/no_outputs.red",'r') as f:
            output_red = yaml.load(f)        
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
    assert not _has_outputs(red_create(batch= False, output= False)), 'Expected no outputs but found one output in hello_world.red'
    assert  _has_outputs(red_create(batch= False, output= True)), 'Expected outputs but not found output in input_output.red'


def test_check_execution_arguments():
    """
    Tests the _check_execution_arguments function from cc_faice.exec.main. 
    """
    _check_execution_arguments('ccagency',disable_retry= True, disable_connector_validation= True)
    _check_execution_arguments('ccfaice',leave_container= True)
    _check_execution_arguments('ccfaice',preserve_environment= True)
    _check_execution_arguments('ccfaice',gpu_ids= True)
    _check_execution_arguments('ccfaice',disable_pull= True)
    with pytest.raises(InvalidExecutionEngineArgumentException):
        _check_execution_arguments('ccfaice',disable_retry= True, disable_connector_validation= True)
    with pytest.raises(InvalidExecutionEngineArgumentException):
        _check_execution_arguments('ccagency',leave_container= True)
    with pytest.raises(InvalidExecutionEngineArgumentException): 
        _check_execution_arguments('ccagency',preserve_environment= True)
    with pytest.raises(InvalidExecutionEngineArgumentException): 
        _check_execution_arguments('ccagency',gpu_ids= True)
    with pytest.raises(InvalidExecutionEngineArgumentException): 
        _check_execution_arguments('ccagency',insecure= True)    
    with pytest.raises(InvalidExecutionEngineArgumentException): 
        _check_execution_arguments('ccagency',disable_pull= True)    


def test_load_and_read():
    """
    Tests the _load_and_read function from cc_core.commons.files  
    """
    load_and_read(location='tests/red_files/initial.red', var_name= "initial")
    with pytest.raises(YAMLError):
        load_and_read(location='tests/red_files/for_test_load_and_read.txt', var_name= "initial")
    with pytest.raises(OSError):
        load_and_read(location='test/red_files/initial.red', var_name= "initial")