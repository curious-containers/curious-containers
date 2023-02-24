from ruamel.yaml import YAML
import pytest
from cc_faice.exec.main import _has_outputs , _check_execution_arguments

yaml = YAML(typ='safe')

# Reading all the Red files
with open("tests/red_files/input_output.red",'r') as f:
    input_output_red = yaml.load(f)

with open("tests/red_files/hello_world.red",'r') as f:
    hello_world_red = yaml.load(f)

def test_has_outputs():
    """
    Tests the _has_outputs function from cc_faice.exec.main for two red files. 
    """
    assert not _has_outputs(hello_world_red), 'Expected no outputs but found one output in hello_world.red'
    assert  _has_outputs(input_output_red), 'Expected outputs but not found output in input_output.red'


