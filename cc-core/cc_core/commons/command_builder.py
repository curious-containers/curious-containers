"""
This module defines functions for the following functionalities:
- Create the command to execute from cli-inputs and job-inputs
"""
from cc_core.commons.cwl_types import InputType, CliArgument
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
