from ruamel.yaml import YAML
from cc_faice.exec.main import _has_outputs

yaml = YAML(typ='safe')

# Reading all the Red files
def read_file(batch:bool,output:bool):
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
    if output == True:
        with open("tests/red_files/outputs.red",'r') as f:
            output_red = yaml.load(f)
    else:
        with open("tests/red_files/no_outputs.red",'r') as f:
            output_red = yaml.load(f)        
    if (output == True) & (batch == True):
        initial_red['batches'] = output_red
    elif (output == False) & (batch == True):
        initial_red['batches'] = output_red
    elif (output == True) & (batch == False):
        initial_red.update(output_red)
    else:
        initial_red.update(output_red)
    return initial_red


def test_has_outputs():
    """
    Tests the _has_outputs function from cc_faice.exec.main for two red files. 
    """
    assert not _has_outputs(read_file(batch=False,output=False)), 'Expected no outputs but found one output in hello_world.red'
    assert  _has_outputs(read_file(batch=False,output=True)), 'Expected outputs but not found output in input_output.red'