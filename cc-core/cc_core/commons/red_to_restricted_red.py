"""
This module defines functionality to transform red data into restricted red data:
  - Split in batches
  - Complete input attributes
  - Normalize base command
  - Resolve input references

The following steps were performed for blue, but are NOT longer supported for restricted red:
  - Define a command from a red file
    - Define the base command
    - Define the arguments for the command
  - Filter primitive data from job inputs

These steps are now performed by the agent
"""
from copy import deepcopy
import os.path
import uuid

from cc_core.commons.exceptions import InvalidInputReference
from cc_core.commons.input_references import resolve_input_references
from red_val.red_types import InputType

CONTAINER_OUTPUT_DIR = '/cc/outputs'
CONTAINER_INPUT_DIR = '/cc/inputs'
CONTAINER_AGENT_PATH = '/cc/blue_agent.py'
CONTAINER_BLUE_FILE_PATH = '/cc/blue_file.json'


def convert_red_to_restricted_red(red_data):
    """
    Converts the given red data into a list of restricted red data dictionaries.

    - The restricted red data is always given as list and each list entry represents one batch in the red data.
    - The command is always a list of strings
    - TODO: process as defined in CWL standard
    - For every input file/input directory additional attributes are defined ('path', 'nameroot', 'nameext')
    - Input references are resolved

    :param red_data: The red data to convert
    :type red_data: dict
    :return: A list of restricted red data
    :rtype: list[dict]
    """
    restricted_red_batches = []

    # Split in batches
    batches = extract_batches(red_data)

    cli_description = red_data['cli']
    cli_inputs = cli_description['inputs']
    cli_outputs = cli_description.get('outputs')
    cli_stdout = cli_description.get('stdout')
    cli_stderr = cli_description.get('stderr')

    command = normalize_base_command(cli_description.get('baseCommand'))

    for batch in batches:
        # complete input attributes
        batch_inputs = batch['inputs']
        complete_batch_inputs(batch_inputs, cli_inputs)

        # resolve input references
        resolved_cli_outputs = complete_input_references_in_outputs(cli_outputs, batch_inputs)

        # create restricted red batch
        restricted_red_batch = create_restricted_red_batch(
            command=command,
            batch=batch,
            cli_inputs=cli_inputs,
            cli_outputs=resolved_cli_outputs,
            cli_stdout=cli_stdout,
            cli_stderr=cli_stderr
        )
        restricted_red_batches.append(restricted_red_batch)

    return restricted_red_batches


def create_restricted_red_batch(command, batch, cli_inputs, cli_outputs, cli_stdout=None, cli_stderr=None):
    """
    Defines a dictionary containing a blue batch

    :param command: The command of the blue data, given as list of strings
    :param batch: The Job data of the blue data
    :param cli_inputs: The input section of cli description
    :param cli_outputs: The outputs section of cli description
    :param cli_stdout: The path where the stdout file should be created. If None cli.stdout is not added to the blue
                       batch
    :param cli_stderr: The path where the stderr file should be created. If None cli.stderr it is not added to the blue
                       batch
    :return: A dictionary containing the blue data
    """
    batch_inputs = batch['inputs']
    batch_outputs = batch['outputs']
    data = {
        'command': command,
        'cli': {
            'inputs': cli_inputs,
            'outputs': cli_outputs
        },
        'inputs': batch_inputs,
        'outputs': batch_outputs
    }

    if _outputs_contain_output_type(batch_outputs, 'stdout') and cli_stdout is None:
        cli_stdout = str(uuid.uuid4())

    if _outputs_contain_output_type(batch_outputs, 'stderr') and cli_stderr is None:
        cli_stderr = str(uuid.uuid4())

    # add stdout/stderr file specification
    if cli_stdout is not None:
        data['cli']['stdout'] = cli_stdout

    if cli_stderr is not None:
        data['cli']['stderr'] = cli_stderr

    return data


def _outputs_contain_output_type(blue_batch_outputs, output_type):
    """
    Returns whether the given blue batch outputs contain an output with the given output type.
    This function is used to determine, if this batch output contain stdout or stderr specifications.

    :param blue_batch_outputs: The blue batch outputs to search in. The keys are the output keys of the converted red
                               file and the values are the output values corresponding to this key.
    :type blue_batch_outputs: dict
    :param output_type: The OutputType to search given as string
    :type output_type: str
    :return: True, if an output of type output_type could be found, otherwise False
    """
    for output_value in blue_batch_outputs.values():
        if output_value.get('class') == output_type:
            return True
    return False


def complete_input_references_in_outputs(cli_outputs, inputs_to_reference):
    """
    Takes the cli outputs and inputs to reference and returns the cli outputs, but with resolved input references

    :param cli_outputs: The cli outputs to resolve input references for
    :param inputs_to_reference: The inputs to reference
    """
    resolved_outputs = deepcopy(cli_outputs)

    for output_key, output_value in resolved_outputs.items():
        if output_value['type'] == 'stdout' or output_value['type'] == 'stderr':
            continue
        output_binding = output_value['outputBinding']

        try:
            resolved_glob = resolve_input_references(output_binding['glob'], inputs_to_reference)
        except InvalidInputReference as e:
            raise InvalidInputReference('Invalid Input Reference for output key "{}":\n{}'.format(output_key, str(e)))

        output_binding['glob'] = resolved_glob

    return resolved_outputs


def complete_batch_inputs(batch_inputs, cli_inputs):
    """
    Completes the input attributes of the input files/directories, by adding the attributes:
    path, basename, dirname, nameroot, nameext

    :param batch_inputs: a dictionary containing job input information
    :param cli_inputs: a dictionary that contains the cli description
    """
    for input_key, batch_value in batch_inputs.items():
        cli_input = cli_inputs[input_key]

        input_type = InputType.from_string(cli_input['type'])

        # complete files
        if input_type.is_file():
            if input_type.is_array():
                for file_element in batch_value:
                    complete_file_input_values(input_key, file_element)
            else:
                complete_file_input_values(input_key, batch_value)

        # complete directories
        elif input_type.is_directory():
            if input_type.is_array():
                for directory_element in batch_value:
                    complete_directory_input_values(input_key, directory_element)
            else:
                complete_directory_input_values(input_key, batch_value)


def default_inputs_dirname():
    """
    Returns the default dirname for an input file.

    :return: The default dirname for an input file.
    """
    return os.path.join(CONTAINER_INPUT_DIR, str(uuid.uuid4()))


def complete_file_input_values(input_key, input_value):
    """
    Completes the information inside a given file input value. Will alter the given input_value.
    Creates the following keys (if not already present): path, basename, dirname, nameroot, nameext

    :param input_key: An input key as string
    :param input_value: An input value with class 'File'
    """
    # define basename
    if 'basename' in input_value:
        basename = input_value['basename']
    else:
        basename = input_key
        input_value['basename'] = basename

    # define dirname
    if 'dirname' in input_value:
        dirname = input_value['dirname']
    else:
        dirname = default_inputs_dirname()
        input_value['dirname'] = dirname

    # define nameroot, nameext
    nameroot, nameext = os.path.splitext(basename)
    input_value['nameroot'] = nameroot
    input_value['nameext'] = nameext

    # define path
    input_value['path'] = os.path.join(dirname, basename)


def complete_directory_input_values(input_key, input_value):
    """
    Completes the information inside a given directory input value. Will alter the given input_value.
    Creates the following keys (if not already present): path, basename

    :param input_key: An input key as string
    :param input_value: An input value with class 'Directory'
    """
    # define basename
    if 'basename' in input_value:
        basename = input_value['basename']
    else:
        basename = input_key
        input_value['basename'] = basename

    # define path
    dirname = default_inputs_dirname()
    input_value['path'] = os.path.join(dirname, basename)


def extract_batches(red_data):
    """
    Extracts a list of batches from the given red data.
    The resulting batches always contain an inputs and an outputs key

    :param red_data: The red data to extract batches from
    :return: A list of Batches
    """
    # in case of batches given
    red_batches = red_data.get('batches')
    if red_batches:
        batches = []
        for batch in red_batches:
            new_batch = {'inputs': batch['inputs'],
                         'outputs': batch.get('outputs', {})}
            batches.append(new_batch)
    else:
        batch = {'inputs': red_data['inputs'],
                 'outputs': red_data.get('outputs', {})}
        batches = [batch]

    for batch in batches:
        remove_null_values(batch['inputs'])
        remove_null_values(batch['outputs'])

    return batches


def remove_null_values(dictionary):
    """
    Removed values that are None

    :param dictionary: The dictionary in which to remove None values
    """
    keys = list(dictionary.keys())
    for key in keys:
        if dictionary[key] is None:
            del dictionary[key]


def normalize_base_command(cwl_base_command):
    """
    Normalizes the command given in a red file. The command returned by this function is always a list of strings.

    :param cwl_base_command: The cwl base command as string or list of strings in the red file
    :type cwl_base_command: str or list[str]
    :return: A stripped list of strings representing the base command
    :rtype: list[str]
    """
    if isinstance(cwl_base_command, list):
        base_command = [w.strip() for w in cwl_base_command]
    elif isinstance(cwl_base_command, str):
        base_command = [cwl_base_command.strip()]
    else:
        base_command = []

    return base_command
