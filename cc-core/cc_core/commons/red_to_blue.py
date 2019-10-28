"""
This module defines functionality to transform red data into blue data:
- Define a command from a red file
  - Define the base command
  - Define the arguments for the command
- Complete input attributes
- Resolve input references
"""
from copy import deepcopy

from enum import Enum
from functools import total_ordering

import os.path

import uuid

from cc_core.commons.exceptions import JobSpecificationError, InvalidInputReference, RedSpecificationError
from cc_core.commons.input_references import resolve_input_references

CONTAINER_OUTPUT_DIR = '/cc/outputs'
CONTAINER_INPUT_DIR = '/cc/inputs'
CONTAINER_AGENT_PATH = '/cc/blue_agent.py'
CONTAINER_BLUE_FILE_PATH = '/cc/blue_file.json'
BLUE_INPUT_CLASSES = {'File', 'Directory'}


def convert_red_to_blue(red_data):
    """
    Converts the given red data into a list of blue data dictionary. The blue data is always given as list and each list
    entry represents one batch in the red data.

    :param red_data: The red data to convert
    :return: A list of blue data dictionaries
    """
    blue_batches = []

    batches = extract_batches(red_data)

    cli_description = red_data['cli']
    cli_inputs = cli_description['inputs']
    cli_outputs = cli_description.get('outputs')
    cli_stdout = cli_description.get('stdout')
    cli_stderr = cli_description.get('stderr')

    cli_arguments = get_cli_arguments(cli_inputs)
    base_command = produce_base_command(cli_description.get('baseCommand'))

    for batch in batches:
        batch_inputs = batch['inputs']
        complete_batch_inputs(batch_inputs, cli_inputs)
        resolved_cli_outputs = complete_input_references_in_outputs(cli_outputs, batch_inputs)
        command = generate_command(base_command, cli_arguments, batch)
        blue_batch = create_blue_batch(command, batch, resolved_cli_outputs, cli_stdout, cli_stderr)
        blue_batches.append(blue_batch)

    return blue_batches


def _is_blue_input_value(input_value):
    """
    Returns whether the given input value defines a connector.

    :param input_value: The input value as list or value, that may contain a connector
    :return: True, if input value contains a connector definition, otherwise false
    """
    if isinstance(input_value, list):
        if not input_value:
            return False

        for sub_input_value in input_value:
            if not _is_blue_input_value(sub_input_value):
                return False
        return True
    elif isinstance(input_value, dict):
        return input_value.get('class') in BLUE_INPUT_CLASSES

    return False


def _create_blue_batch_inputs(batch_inputs):
    """
    Filters the given inputs and returns only batch inputs, that contain a connector definition

    :param batch_inputs: The batch inputs of a red file
    :return: A dictionary containing all keys of batch_inputs, which contain a connector definition
    """
    blue_batch_inputs = {}
    for input_key, input_value in batch_inputs.items():
        if _is_blue_input_value(input_value):
            blue_batch_inputs[input_key] = input_value
    return blue_batch_inputs


def create_blue_batch(command, batch, cli_outputs, cli_stdout=None, cli_stderr=None):
    """
    Defines a dictionary containing a blue batch

    :param command: The command of the blue data, given as list of strings
    :param batch: The Job data of the blue data
    :param cli_outputs: The outputs section of cli description
    :param cli_stdout: The path where the stdout file should be created. If None cli.stdout is not added to the blue
                       batch
    :param cli_stderr: The path where the stderr file should be created. If None cli.stderr it is not added to the blue
                       batch
    :return: A dictionary containing the blue data
    """
    blue_batch_inputs = _create_blue_batch_inputs(batch['inputs'])
    blue_batch_outputs = batch['outputs']
    blue_data = {
        'command': command,
        'cli': {
            'outputs': cli_outputs
        },
        'inputs': blue_batch_inputs,
        'outputs': blue_batch_outputs
    }

    if _outputs_contain_output_type(blue_batch_outputs, 'stdout') and cli_stdout is None:
        cli_stdout = str(uuid.uuid4())

    if _outputs_contain_output_type(blue_batch_outputs, 'stderr') and cli_stderr is None:
        cli_stderr = str(uuid.uuid4())

    # add stdout/stderr file specification
    if cli_stdout is not None:
        blue_data['cli']['stdout'] = cli_stdout

    if cli_stderr is not None:
        blue_data['cli']['stderr'] = cli_stderr

    return blue_data


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


class InputType:
    class InputCategory(Enum):
        File = 0
        Directory = 1
        string = 2
        int = 3
        long = 4
        float = 5
        double = 6
        boolean = 7

    def __init__(self, input_category, is_array, is_optional):
        self.input_category = input_category
        self._is_array = is_array
        self._is_optional = is_optional

    @staticmethod
    def from_string(s):
        is_optional = s.endswith('?')
        if is_optional:
            s = s[:-1]

        is_array = s.endswith('[]')
        if is_array:
            s = s[:-2]

        input_category = None
        for ic in InputType.InputCategory:
            if s == ic.name:
                input_category = ic

        if input_category is None:
            raise RedSpecificationError('The given input type "{}" is not valid'.format(s))

        return InputType(input_category, is_array, is_optional)

    def to_string(self):
        return '{}{}{}'.format(self.input_category.name,
                               '[]' if self._is_array else '',
                               '?' if self._is_optional else '')

    def __repr__(self):
        return self.to_string()

    def __eq__(self, other):
        return (self.input_category == other.input_category) and\
               (self._is_array == other.is_array()) and\
               (self._is_optional == other.is_optional())

    def is_file(self):
        return self.input_category == InputType.InputCategory.File

    def is_directory(self):
        return self.input_category == InputType.InputCategory.Directory

    def is_array(self):
        return self._is_array

    def is_optional(self):
        return self._is_optional

    def is_primitive(self):
        return (self.input_category != InputType.InputCategory.Directory) and \
               (self.input_category != InputType.InputCategory.File)


class OutputType:
    class OutputCategory(Enum):
        File = 0
        Directory = 1
        stdout = 2
        stderr = 3

    def __init__(self, output_category, is_optional):
        self.output_category = output_category
        self._is_optional = is_optional

    @staticmethod
    def from_string(s):
        is_optional = s.endswith('?')
        if is_optional:
            s = s[:-1]

        output_category = None
        for oc in OutputType.OutputCategory:
            if s == oc.name:
                output_category = oc

        if output_category is None:
            raise RedSpecificationError('The given output type "{}" is not valid'.format(s))

        if output_category == OutputType.OutputCategory.stdout and is_optional:
            raise RedSpecificationError(
                'The given output type is an optional stdout ("{}"), which is not valid'.format(s)
            )
        if output_category == OutputType.OutputCategory.stderr and is_optional:
            raise RedSpecificationError(
                'The given output type is an optional stderr ("{}"), which is not valid'.format(s)
            )

        return OutputType(output_category, is_optional)

    def to_string(self):
        return '{}{}'.format(
            self.output_category.name,
            '?' if self._is_optional else ''
        )

    def __repr__(self):
        return self.to_string()

    def __eq__(self, other):
        return (self.output_category == other.output_category) and \
               (self._is_optional == other.is_optional())

    # noinspection PyMethodMayBeStatic
    def is_array(self):
        return False

    def is_file(self):
        return self.output_category == OutputType.OutputCategory.File

    def is_directory(self):
        return self.output_category == OutputType.OutputCategory.Directory

    def is_stdout(self):
        return self.output_category == OutputType.OutputCategory.stdout

    def is_stderr(self):
        return self.output_category == OutputType.OutputCategory.stderr

    def is_stream(self):
        """
        Returns True, if this OutputType holds a stdout or stderr
        """
        return self.is_stdout() or self.is_stderr()

    def is_optional(self):
        return self._is_optional


def generate_command(base_command, cli_arguments, batch):
    """
    Creates a command from the cli description and a given batch.

    :param base_command: The base command to use
    :param cli_arguments: The arguments of the described tool
    :param batch: The batch to execute
    :return: A list of string representing the created command
    """
    command = base_command.copy()

    for cli_argument in cli_arguments:
        batch_value = batch['inputs'].get(cli_argument.input_key)
        execution_argument = create_execution_argument(cli_argument, batch_value)
        command.extend(execution_argument)

    return command


INPUT_CATEGORY_REPRESENTATION_MAPPER = {
    InputType.InputCategory.File: lambda batch_value: batch_value['path'],
    InputType.InputCategory.Directory: lambda batch_value: batch_value['path'],
    InputType.InputCategory.string: lambda batch_value: batch_value,
    InputType.InputCategory.int: lambda batch_value: str(batch_value),
    InputType.InputCategory.long: lambda batch_value: str(batch_value),
    InputType.InputCategory.float: lambda batch_value: str(batch_value),
    InputType.InputCategory.double: lambda batch_value: str(batch_value),
    InputType.InputCategory.boolean: lambda batch_value: str(batch_value)
}


def _get_boolean_array_argument_list(cli_argument, batch_value):
    """
    Creates a list of strings representing an execution argument for boolean arrays. Like ['true', 'False']

    :param cli_argument: The cli argument. (Should be an boolean array)
    :param batch_value: The batch value corresponding to the cli argument. Should contain any number of booleans.
    :return: A list of strings representing
    """
    argument_list = []
    if cli_argument.item_separator:
        for sub_batch_value in batch_value:
            r = INPUT_CATEGORY_REPRESENTATION_MAPPER[cli_argument.get_type_category()](sub_batch_value)
            argument_list.append(r)
    return argument_list


def _create_argument_list(cli_argument, batch_value):
    """
    Creates the argument list for an execution argument.
    Prefix is not included.
    Elements are not joined with item_separator.

    :param cli_argument: An cli argument
    :param batch_value: The batch value corresponding to the cli argument.
    :return: A list of strings representing the argument list of this cli argument.
    """
    argument_list = []
    if cli_argument.is_array():
        if not isinstance(batch_value, list):
            raise JobSpecificationError('For input key "{}":\nDescription defines an array, '
                                        'but job is not given as list'.format(cli_argument.input_key))

        # handle boolean arrays special
        if cli_argument.is_boolean():
            argument_list = _get_boolean_array_argument_list(cli_argument, batch_value)
        else:
            for sub_batch_value in batch_value:
                r = INPUT_CATEGORY_REPRESENTATION_MAPPER[cli_argument.get_type_category()](sub_batch_value)
                argument_list.append(r)
    else:
        # do not insert anything for boolean
        if not cli_argument.is_boolean():
            argument_list.append(INPUT_CATEGORY_REPRESENTATION_MAPPER[cli_argument.get_type_category()](batch_value))

    return argument_list


def _argument_list_to_execution_argument(argument_list, cli_argument, batch_value):
    """
    Returns a list of strings representing the execution argument for the given cli argument.

    :param argument_list: The list of argument without prefix
    :param cli_argument: The cli argument whose prefix might be added.
    :param batch_value: The batch value corresponding to the cli argument
    """
    execution_argument = []

    if cli_argument.prefix:
        do_separate = cli_argument.separate

        # do separate, if the cli argument is an array and the item separator is not given
        if cli_argument.is_array() and not cli_argument.item_separator:
            do_separate = True

        should_add_prefix = True
        # do not add prefix if input value is an empty list
        if cli_argument.is_array():
            if not batch_value:
                should_add_prefix = False

        # handle prefix special for boolean values and arrays
        # do not add prefix, if boolean value is False or array of booleans is empty
        if cli_argument.is_boolean():
            if not batch_value:
                should_add_prefix = False

        if should_add_prefix:
            if do_separate:
                execution_argument.append(cli_argument.prefix)
                execution_argument.extend(argument_list)
            else:
                # This case only occurs for boolean arrays, if prefix is set, separate is set to false and no
                # itemSeparator is given and the corresponding input list is not empty
                if not argument_list:
                    execution_argument.append(cli_argument.prefix)
                else:
                    assert len(argument_list) == 1
                    joined_argument = '{}{}'.format(cli_argument.prefix, argument_list[0])
                    execution_argument.append(joined_argument)
    else:
        execution_argument.extend(argument_list)

    return execution_argument


def create_execution_argument(cli_argument, batch_value):
    """
    Creates a list of strings representing an execution argument. Like ['--mydir=', '/path/to/file']

    :param cli_argument: The cli argument
    :param batch_value: The batch value corresponding to the cli argument. Can be None
    :return: A list of strings, that can be used to extend the command. Returns an empty list if (cli_argument is
             optional and batch_value is None) or (cli argument is array and len(batch_value) is 0)
    :raise JobSpecificationError: If cli argument is mandatory, but batch value is None
                                  If Cli Description defines an array, but job does not define a list
    """
    # handle optional arguments
    if batch_value is None:
        if cli_argument.is_optional():
            return []
        else:
            raise JobSpecificationError('Required argument "{}" is missing'.format(cli_argument.input_key))

    argument_list = _create_argument_list(cli_argument, batch_value)

    # join argument list, depending on item separator
    if argument_list and cli_argument.item_separator:
        argument_list = [cli_argument.item_separator.join(argument_list)]

    return _argument_list_to_execution_argument(argument_list, cli_argument, batch_value)


@total_ordering
class CliArgumentPosition:
    class CliArgumentPositionType(Enum):
        Positional = 0
        Named = 1

    def __init__(self, argument_position_type, binding_position):
        """
        Creates a new CliArgumentPosition.

        :param argument_position_type: The position type of this argument position
        """
        self.argument_position_type = argument_position_type
        self.binding_position = binding_position

    @staticmethod
    def new_positional_argument(binding_position):
        """
        Creates a new positional argument position.

        :param binding_position: The input position of the argument
        :return: A new CliArgumentPosition with position_type Positional
        """
        return CliArgumentPosition(CliArgumentPosition.CliArgumentPositionType.Positional, binding_position)

    @staticmethod
    def new_named_argument():
        """
        Creates a new named argument position.

        :return: A new CliArgumentPosition with position_type Named.
        """
        return CliArgumentPosition(CliArgumentPosition.CliArgumentPositionType.Named, 0)

    def is_positional(self):
        return self.argument_position_type is CliArgumentPosition.CliArgumentPositionType.Positional

    def is_named(self):
        return self.argument_position_type is CliArgumentPosition.CliArgumentPositionType.Named

    def __eq__(self, other):
        return (self.argument_position_type is other.argument_position_type) \
               and (self.binding_position == other.binding_position)

    def __lt__(self, other):
        if self.argument_position_type is CliArgumentPosition.CliArgumentPositionType.Positional:
            if other.argument_position_type is CliArgumentPosition.CliArgumentPositionType.Positional:
                return self.binding_position < other.binding_position
            else:
                return True
        else:
            return False

    def __repr__(self):
        return 'CliArgumentPosition(argument_position_type={}, binding_position={})'\
               .format(self.argument_position_type, self.binding_position)


class CliArgument:
    def __init__(self, input_key, argument_position, input_type, prefix, separate, item_separator):
        """
        Creates a new CliArgument.

        :param input_key: The input key of the cli argument
        :param argument_position: The type of the cli argument (Positional, Named)
        :param input_type: The type of the input key
        :param prefix: The prefix to prepend to the value
        :param separate: Separate prefix and value
        :param item_separator: The string to join the elements of an array
        """
        self.input_key = input_key
        self.argument_position = argument_position
        self.input_type = input_type
        self.prefix = prefix
        self.separate = separate
        self.item_separator = item_separator

    def __repr__(self):
        return 'CliArgument(\n\t{}\n)'.format('\n\t'.join(['input_key={}'.format(self.input_key),
                                                           'argument_position={}'.format(self.argument_position),
                                                           'input_type={}'.format(self.input_type),
                                                           'prefix={}'.format(self.prefix),
                                                           'separate={}'.format(self.separate),
                                                           'item_separator={}'.format(self.item_separator)]))

    @staticmethod
    def new_positional_argument(input_key, input_type, input_binding_position, item_separator):
        return CliArgument(input_key=input_key,
                           argument_position=CliArgumentPosition.new_positional_argument(input_binding_position),
                           input_type=input_type,
                           prefix=None,
                           separate=False,
                           item_separator=item_separator)

    @staticmethod
    def new_named_argument(input_key, input_type, prefix, separate, item_separator):
        return CliArgument(input_key=input_key,
                           argument_position=CliArgumentPosition.new_named_argument(),
                           input_type=input_type,
                           prefix=prefix,
                           separate=separate,
                           item_separator=item_separator)

    def is_array(self):
        return self.input_type.is_array()

    def is_optional(self):
        return self.input_type.is_optional()

    def is_positional(self):
        return self.argument_position.is_positional()

    def is_named(self):
        return self.argument_position.is_named()

    def get_type_category(self):
        return self.input_type.input_category

    def is_boolean(self):
        return self.input_type.input_category == InputType.InputCategory.boolean

    @staticmethod
    def from_cli_input_description(input_key, cli_input_description):
        """
        Creates a new CliArgument depending of the information given in the cli input description.
        inputBinding keys = 'prefix' 'separate' 'position' 'itemSeparator'

        :param input_key: The input key of the cli input description
        :param cli_input_description: red_data['cli']['inputs'][input_key]
        :return: A new CliArgument
        """
        input_binding = cli_input_description['inputBinding']
        input_binding_position = input_binding.get('position', 0)
        prefix = input_binding.get('prefix')
        separate = input_binding.get('separate', True)
        item_separator = input_binding.get('itemSeparator')

        input_type = InputType.from_string(cli_input_description['type'])

        if prefix:
            arg = CliArgument.new_named_argument(input_key, input_type, prefix, separate, item_separator)
        else:
            arg = CliArgument.new_positional_argument(input_key, input_type, input_binding_position, item_separator)
        return arg


def get_cli_arguments(cli_inputs):
    """
    Returns a sorted list of cli arguments.

    :param cli_inputs: The cli inputs description
    :return: A list of CliArguments
    """
    cli_arguments = []
    for input_key, cli_input_description in cli_inputs.items():
        cli_arguments.append(CliArgument.from_cli_input_description(input_key, cli_input_description))
    return sorted(cli_arguments, key=lambda cli_argument: cli_argument.argument_position)


def produce_base_command(cwl_base_command):
    """
    Returns a list of strings describing the base command

    :param cwl_base_command: The cwl base command as written in the red file.
    :return: A stripped list of strings representing the base command
    """
    if isinstance(cwl_base_command, list):
        base_command = [w.strip() for w in cwl_base_command]
    elif isinstance(cwl_base_command, str):
        base_command = [cwl_base_command.strip()]
    else:
        base_command = []

    return base_command


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
