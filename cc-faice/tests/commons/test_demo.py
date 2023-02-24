from ruamel.yaml import YAML
import pytest
from cc_faice.exec.main import _has_outputs 


yaml = YAML(typ='safe')



# Reading all the Red files
with open("tests/commons/input_output.red",'r') as f:
    inputOutputRed = yaml.load(f)

with open("tests/commons/hello_world.red",'r') as f:
    helloWorldRed = yaml.load(f)



def test_has_outputs():
    """
    Tests the _has_outputs function form cc_faice.exec.main for two red files. 
    """
    assert _has_outputs(helloWorldRed) == False
    assert _has_outputs(inputOutputRed)
