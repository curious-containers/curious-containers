"""
This module defines functions for the following functionalities:
- Create the command to execute from cli-inputs and job-inputs
"""
from enum import Enum
from functools import total_ordering

from red_val.red_types import InputType
from cc_core.commons.exceptions import JobSpecificationError


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
        return 'CliArgumentPosition(argument_position_type={}, binding_position={})' \
            .format(self.argument_position_type, self.binding_position)
