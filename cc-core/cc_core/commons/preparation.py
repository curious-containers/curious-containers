import hashlib
import os

import enum

from functools import total_ordering
from typing import List, Dict


class OutputMode(enum.Enum):
    Connectors = 0
    Directory = 1


def _format_exception(exception):
    return '[{}]\n{}\n'.format(type(exception).__name__, str(exception))


class ConnectorManager:
    def __init__(self):
        self._input_runners = [] 
        self._output_runners = [] 
        self._cli_output_runners = []  # type: List[CliOutputRunner]
        self._connector_cli_version_cache = {}  # type: Dict[str, str]

    def import_output_connectors(self, outputs, cli_outputs, output_mode, cli_stdout, cli_stderr):
        """
        Creates OutputConnectorRunner for every key in outputs.
        In Addition creates a CliOutputRunner for every key in cli_outputs.

        :param outputs: The outputs to create runner for.
        :param cli_outputs: The output cli description.
        :param output_mode: The output mode for this execution
        :param cli_stdout: The value of the stdout cli description (the path to the stdout file)
        :param cli_stderr: The value of the stderr cli description (the path to the stderr file)
        """

        for cli_output_key, cli_output_value in cli_outputs.items():
            output_value = outputs.get(cli_output_key)
            runner = create_cli_output_runner(
                cli_output_key,
                cli_output_value,
                output_value,
                cli_stdout,
                cli_stderr
            )

            self._cli_output_runners.append(runner)

    def prepare_directories(self):
        """
        Tries to create directories needed to execute the connectors.

        :raise ConnectorError: If the needed directory could not be created, or if a received file does already exists
        """
        for runner in self._input_runners:
            runner.prepare_directory()

    def validate_connectors(self, validate_outputs):
        """
        Validates connectors.

        :param validate_outputs: If True, output runners are validated
        """
        for runner in self._input_runners:
            runner.validate_receive()

        if validate_outputs:
            for runner in self._output_runners:
                runner.validate_send()

    def receive_connectors(self):
        """
        Executes receive_file, receive_dir or receive_mount for every input with connector.
        Schedules the mounting runners first for performance reasons.
        """
        not_mounting_runners = []
        # receive mounting input runners
        for runner in self._input_runners:
            if runner.is_mounting():
                runner.receive()
            else:
                not_mounting_runners.append(runner)

        # receive not mounting input runners
        for runner in not_mounting_runners:
            runner.receive()

    def send_connectors(self):
        """
        Tries to executes send for all output connectors.
        If a send runner fails, will try to send the other runners and fails afterwards.

        :raise ConnectorError: If one ore more OutputRunners fail to send.
        """
        errors = []
        for runner in self._output_runners:
            try:
                runner.try_send()
            except ConnectorError as e:
                errors.append(e)

        errors_len = len(errors)
        if errors_len == 1:
            raise errors[0]
        elif errors_len > 1:
            error_strings = [_format_exception(e) for e in errors]
            raise ConnectorError('{} output connectors failed:\n{}'.format(
                errors_len, '\n'.join(error_strings)))

    def inputs_to_dict(self):
        """
        Translates the imported input connectors into a dictionary.

        :return: A dictionary containing status information about all imported input connectors
        """
        inputs_dict = {}

        for input_runner in self._input_runners:
            inputs_dict[input_runner.format_input_key()
                        ] = input_runner.to_dict()

        return inputs_dict

    def outputs_to_dict(self, docker_manager, container):
        """
        Translates the imported output connectors into a dictionary.

        :return: A dictionary containing status information about all imported output connectors
        """
        outputs_dict = {}

        for output_runner in self._cli_output_runners:
            outputs_dict[output_runner.get_output_key()
                         ] = output_runner.to_dict(docker_manager, container)

        return outputs_dict

    def check_outputs(self, docker_manager, container):
        """
        Checks if all output files/directories are present relative to the given working directory

        :raise ConnectorError: If an output file/directory could not be found
        """
        for runner in self._cli_output_runners:
            runner.check_output(docker_manager, container)

    def umount_connectors(self):
        """
        Tries to execute umount for every connector.

        :return: The errors that occurred during execution
        """
        errors = []
        for runner in self._input_runners:
            try:
                runner.try_umount()
            except ConnectorError as e:
                errors.append(e)

        return errors


def outputs(connector_manager, docker_manager, container):
    connector_manager.check_outputs(docker_manager, container)
    return connector_manager.outputs_to_dict(docker_manager, container)


def prepare_execution(connector_manager, restricted_red_data):
    """
    Prepares the execution of a restricted red command by generating the command string and configuring output connectors.

    :param connector_manager: A connector manager instance used to import output connectors.
    :type connector_manager: ConnectorManager

    :param restricted_red_data: The restricted red data containing the command and its configuration.
    :type restricted_red_data: dict

    :return: A tuple containing the generated command string, the configured CLI stdout redirect, and the configured CLI stderr redirect.
    :rtype: tuple
    """
    output_mode = OutputMode.Directory

    base_command = restricted_red_data.get('command')
    _validate_command(base_command)
    cli_arguments = get_cli_arguments(restricted_red_data['cli']['inputs'])
    command = generate_command(
        base_command, cli_arguments, restricted_red_data)

    outputs = restricted_red_data.get('outputs', {})
    cli = restricted_red_data.get('cli', {})
    cli_outputs = cli.get('outputs', {})
    cli_stdout = cli.get('stdout')
    cli_stderr = cli.get('stderr')

    connector_manager.import_output_connectors(
        outputs, cli_outputs, output_mode, cli_stdout, cli_stderr)

    return (command, cli_stdout, cli_stderr)


def create_cli_output_runner(cli_output_key, cli_output_value, output_value=None, cli_stdout=None, cli_stderr=None):
    """
    Creates a CliOutputRunner.

    :param cli_output_key: The output key of the corresponding cli output
    :param cli_output_value: The output value given in the restricted_red file of the corresponding cli output
    :param output_value: The job output value for this output key. Can be None
    :param cli_stdout: The path to the stdout file
    :param cli_stderr: The path to the stderr file
    :return: A new instance of CliOutputRunner
    :raise ConnectorError: If the cli output is not valid.
    """
    try:
        output_class = OutputConnectorClass.from_string(
            cli_output_value['type'])
        if output_class.is_stdout():
            if cli_stdout is None:
                raise ConnectorError(
                    'Type of output key "{}" is "stdout", but no stdout file specified in cli section of red file'
                    .format(cli_output_key)
                )
            glob_pattern = cli_stdout
        elif output_class.is_stderr():
            if cli_stderr is None:
                raise ConnectorError(
                    'Type of output key "{}" is "stderr", but no stderr file specified in cli section of red file'
                    .format(cli_output_key)
                )
            glob_pattern = cli_stderr
        else:
            glob_pattern = cli_output_value['outputBinding']['glob']
    except KeyError as e:
        raise ConnectorError('Could not create cli runner for output key "{}".\n'
                             'The following property was not found: "{}"'.format(cli_output_key, str(e)))

    if output_value is None:
        checksum = None
        size = None
        listing = None
    else:
        checksum = output_value.get('checksum')
        size = output_value.get('size')
        listing = output_value.get('listing')

    return CliOutputRunner(cli_output_key, glob_pattern, output_class, checksum, size, listing)


class OutputConnectorType(enum.Enum):
    File = 0
    Directory = 1
    stdout = 2
    stderr = 3

    @staticmethod
    def get_list():
        """
        Returns a list containing all variants as string

        :rtype: List[str]
        """
        result = []
        for ct in OutputConnectorType:
            result.append(ct.name)
        return result


FILE_LIKE_OUTPUT_TYPES = {
    OutputConnectorType.File,
    OutputConnectorType.stdout,
    OutputConnectorType.stderr,
}


class OutputConnectorClass:
    def __init__(self, connector_type, is_optional):
        self.connector_type = connector_type
        self._is_optional = is_optional

    @staticmethod
    def from_string(s):
        is_optional = s.endswith('?')
        if is_optional:
            s = s[:-1]

        connector_type = None
        for ct in OutputConnectorType:
            if s == ct.name:
                connector_type = ct

        if connector_type is None:
            raise ConnectorError(
                'Could not extract output connector class from string "{}". Connector class should be one of {}'
                .format(s, OutputConnectorType.get_list())
            )

        return OutputConnectorClass(connector_type, is_optional)

    def to_string(self):
        if self._is_optional:
            return '{}?'.format(self.connector_type.name)
        else:
            return self.connector_type.name

    def __repr__(self):
        return self.to_string()

    def __eq__(self, other):
        return self.connector_type == other.connector_type

    def is_file(self):
        return self.connector_type == OutputConnectorType.File

    def is_directory(self):
        return self.connector_type == OutputConnectorType.Directory

    def is_stdout(self):
        return self.connector_type == OutputConnectorType.stdout

    def is_stderr(self):
        return self.connector_type == OutputConnectorType.stderr

    def is_stream(self):
        return self.is_stdout() or self.is_stderr()

    def is_optional(self):
        return self._is_optional

    def is_file_like(self):
        """
        Returns True if this OutputConnectorClass stands for a File, stdout or stderr. Otherwise returns False

        :return: Whether this OutputConnectorClass represents a file
        """
        return self.connector_type in FILE_LIKE_OUTPUT_TYPES


class CliOutputRunner:
    """
    This CliOutputRunner is used to check if an cli output key is fulfilled and move the corresponding file into the
    outputs directory if needed.
    """

    def __init__(self, output_key, glob_pattern, output_class, checksum=None, size=None, listing=None):
        """
        Creates a new CliOutputRunner

        :param output_key: The corresponding output key
        :param glob_pattern: The glob pattern to match against output files
        :param output_class: The class of the output
        :type output_class: OutputConnectorClass
        :param checksum: The expected checksum of the file
        :param size: The expected size of the file
        """
        self._output_key = output_key
        self._glob_pattern = glob_pattern
        self._output_class = output_class
        self._checksum = checksum
        self._size = size
        self._listing = listing

    def get_output_key(self):
        return self._output_key

    def to_dict(self, docker_manager, container):
        """
        Returns a dictionary representing this output file

        :return: A dictionary containing information about this output file
        """
        dict_representation = {
            'class': self._output_class.to_string(),
            'glob': self._glob_pattern,
        }

        paths = _resolve_glob_pattern(
            self._glob_pattern, docker_manager, container, self._output_class.connector_type)

        if len(paths) == 0:
            dict_representation['path'] = None
        elif len(paths) == 1:
            path = paths[0]

            if self._output_class.is_file_like():
                dict_representation['checksum'] = calculate_file_checksum(path, docker_manager, container)
                command = f'python3 -c "import os; print(os.path.getsize(\'{path}\'))"'
                execution_result = docker_manager.run_command(
                    container, command)._stdout.strip()
                
                dict_representation['size'] = execution_result

            dict_representation['path'] = path
        else:
            dict_representation['path'] = paths

        return dict_representation

    def check_output(self, docker_manager, container):
        """
        Checks if the corresponding output is present in the working directory.

        :raise ConnectorError: If the corresponding file/directory is not present on disk
        """
        glob_result = _resolve_glob_pattern(
            self._glob_pattern,
            docker_manager,
            container,
            self._output_class.connector_type
        )

        # check ambiguous
        if len(glob_result) >= 2:
            if self._output_class.is_file_like():
                files_directories = 'files'
            else:
                files_directories = 'directories'

            raise ConnectorError('Could not resolve glob "{}" for output key "{}". Glob is '
                                 'ambiguous. Found the following {}:\n{}'
                                 .format(self._glob_pattern, self._output_key, files_directories, glob_result))

        # check if key is required
        if not self._output_class.is_optional():
            if len(glob_result) == 0:
                if self._output_class.is_file_like():
                    file_directory = 'File'
                else:
                    file_directory = 'Directory'
                raise ConnectorError('Could not resolve glob "{}" for required output key "{}". {} not '
                                     'found.'.format(self._glob_pattern, self._output_key, file_directory))

        # check checksum and file size
        if len(glob_result) == 1:
            path = glob_result[0]

            if self._checksum is not None:
                file_checksum = calculate_file_checksum(
                    path, docker_manager, container)
                if file_checksum != self._checksum:
                    raise ConnectorError(
                        'The given checksum for output key "{}" does not match.\n\tgiven checksum: "{}"'
                        '\n\tfile checksum : "{}"'.format(
                            self._output_key, self._checksum, file_checksum)
                    )

            if self._size is not None:
                file_size = os.path.getsize(path)
                if file_size != self._size:
                    raise ConnectorError(
                        'The given file size for output key "{}" does not match.\n\tgiven size: {}'
                        '\n\tfile size : {}'.format(
                            self._output_key, self._size, file_size)
                    )

            if self._listing:
                listing_content_check = directory_listing_content_check(
                    path, docker_manager, container, self._listing)
                if listing_content_check:
                    raise ConnectorError(
                        'Listing validation for output key "{}" failed:\n{}'
                        .format(self._output_key, listing_content_check)
                    )


FILE_CHUNK_SIZE = 1024 * 1024


def calculate_file_checksum(path,  docker_manager, container):
    """
    Calculates the sha1 checksum of a given file. The checksum is formatted in the following way: 'sha1$<checksum>'

    :param path: The path to the file, whose checksum should be calculated.
    :return: The sha1 checksum of the given file as string
    """

    command = '''
import hashlib
FILE_CHUNK_SIZE = 1048576
hasher = hashlib.sha1()
with open(\'{}\', \'rb\') as file:
    while True:
        buf = file.read(FILE_CHUNK_SIZE)
        if buf:
            hasher.update(buf)
        else:
            break
print(\'sha1$\' + hasher.hexdigest())
'''.format(path)


    command_str = f'python3 -c "{command.strip()}"'
    execution_result = docker_manager.run_command(container, command_str)
    if (execution_result._stdout):
        return execution_result._stdout.strip()
    hasher = hashlib.sha1()
    with open(path, 'rb') as file:
        while True:
            buf = file.read(FILE_CHUNK_SIZE)
            if buf:
                hasher.update(buf)
            else:
                break
    return 'sha1${}'.format(hasher.hexdigest())


def _directory_listing_file_check(file_description, path, docker_manager, container):
    """
    Validates if the given file is present in the filesystem and checks for size and checksum, if given in the
    file_description.

    :param file_description: A dictionary describing a file given in a listing.
                             necessary keys: ['class', 'basename']
                             optional keys: ['size', 'checksum']
    :param path: The path to the file, where it should be present in the local filesystem

    :return: None, if the file is present and checksum and size given in the file_description match the real file,
             otherwise a string describing the mismatch.
    """
    if not os.path.isfile(path):
        return 'listing contains "{}" but this file could not be found on disk.'.format(path)

    checksum = file_description.get('checksum')
    if checksum is not None:
        file_checksum = calculate_file_checksum(
            path, docker_manager, container)
        if checksum != file_checksum:
            return 'checksum of file "{}" does not match the checksum given in listing.' \
                   '\n\tgiven checksum: "{}"\n\tfile checksum : "{}"'.format(
                       path, checksum, file_checksum)

    size = file_description.get('size')
    if size is not None:
        file_size = os.path.getsize(path)
        if size != file_size:
            return 'file size of "{}" does not match the file size given in listing.' \
                   '\n\tgiven size: {}\n\tfile size : {}'.format(
                       path, size, file_size)

    return None


def directory_listing_content_check(directory_path, docker_manager, container,listing):
    """
    Checks if a given listing is present under the given directory path.

    :param directory_path: The path to the base directory
    :param listing: The listing to check
    :return: None if no errors could be found, otherwise a string describing the error
    """
    for sub in listing:
        path = os.path.join(directory_path, sub['basename'])
        if sub['class'] == 'File':
            file_check_result = _directory_listing_file_check(sub, path, docker_manager, container)
            if file_check_result is not None:
                return file_check_result
        elif sub['class'] == 'Directory':
            if not os.path.isdir(path):
                return 'listing contains "{}" but this directory could not be found on disk'.format(path)
            listing = sub.get('listing')
            if listing:
                res = directory_listing_content_check(path, docker_manager, container, listing)
                if res is not None:
                    return res
    return None


def _resolve_glob_pattern(glob_pattern, docker_manager, container, connector_type=None):
    """
    Tries to resolve the given glob_pattern.

    :param glob_pattern: The glob pattern to resolve
    :param connector_type: The connector class to search for
    :return: the resolved glob_pattern as list of strings
    :rtype: List[str]
    """
    command = f'python3 -c "import glob, os; print(glob.glob(os.path.abspath(\'{glob_pattern}\')))"'
    execution_result = docker_manager.run_command(
        container, command)._stdout

    execution_result = execution_result.strip().lstrip(
        "[").rstrip("]").replace("'", "")
    glob_result = [execution_result]
    if connector_type == OutputConnectorType.File:
        glob_result = [f for f in glob_result if not os.path.splitext(f)[
            1] == '']
    elif connector_type == OutputConnectorType.Directory:
        glob_result = [f for f in glob_result if os.path.splitext(f)[1] == '']
    return glob_result


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
            argument_list = _get_boolean_array_argument_list(
                cli_argument, batch_value)
        else:
            for sub_batch_value in batch_value:
                r = INPUT_CATEGORY_REPRESENTATION_MAPPER[cli_argument.get_type_category()](
                    sub_batch_value)
                argument_list.append(r)
    else:
        # do not insert anything for boolean
        if not cli_argument.is_boolean():
            argument_list.append(
                INPUT_CATEGORY_REPRESENTATION_MAPPER[cli_argument.get_type_category()](batch_value))

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
            r = INPUT_CATEGORY_REPRESENTATION_MAPPER[cli_argument.get_type_category()](
                sub_batch_value)
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
                    joined_argument = '{}{}'.format(
                        cli_argument.prefix, argument_list[0])
                    execution_argument.append(joined_argument)
    else:
        execution_argument.extend(argument_list)

    return execution_argument


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
        execution_argument = create_execution_argument(
            cli_argument, batch_value)
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
            raise JobSpecificationError(
                'Required argument "{}" is missing'.format(cli_argument.input_key))

    argument_list = _create_argument_list(cli_argument, batch_value)

    # join argument list, depending on item separator
    if argument_list and cli_argument.item_separator:
        argument_list = [cli_argument.item_separator.join(argument_list)]

    return _argument_list_to_execution_argument(argument_list, cli_argument, batch_value)


class ConnectorError(Exception):
    pass


class ExecutionError(Exception):
    pass


class RedSpecificationError(Exception):
    pass


class JobSpecificationError(Exception):
    pass


class InputType:
    class InputCategory(enum.Enum):
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
            raise RedSpecificationError(
                'The given input type "{}" is not valid'.format(s))

        return InputType(input_category, is_array, is_optional)

    def to_string(self):
        return '{}{}{}'.format(self.input_category.name,
                               '[]' if self._is_array else '',
                               '?' if self._is_optional else '')

    def __repr__(self):
        return self.to_string()

    def __eq__(self, other):
        return (self.input_category == other.input_category) and \
               (self._is_array == other.is_array()) and \
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
                                                           'argument_position={}'.format(
                                                               self.argument_position),
                                                           'input_type={}'.format(
                                                               self.input_type),
                                                           'prefix={}'.format(
                                                               self.prefix),
                                                           'separate={}'.format(
                                                               self.separate),
                                                           'item_separator={}'.format(self.item_separator)]))

    @staticmethod
    def new_positional_argument(input_key, input_type, input_binding_position, item_separator):
        return CliArgument(input_key=input_key,
                           argument_position=CliArgumentPosition.new_positional_argument(
                               input_binding_position),
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
            arg = CliArgument.new_named_argument(
                input_key, input_type, prefix, separate, item_separator)
        else:
            arg = CliArgument.new_positional_argument(
                input_key, input_type, input_binding_position, item_separator)
        return arg


def _validate_command(command):
    if command is None:
        raise ExecutionError(
            'Invalid RESTRICTED_RED_FILE. "command" is not specified.')

    if not isinstance(command, list):
        raise ExecutionError(
            'Invalid RESTRICTED_RED_FILE. "command" has to be a list of strings.\ncommand: "{}"'.format(
                command)
        )

    for s in command:
        if not isinstance(s, str):
            raise ExecutionError(
                'Invalid RESTRICTED_RED_FILE. "command" has to be a list of strings.\ncommand: "{}"\n'
                '"{}" is not a string'.format(command, s)
            )


def get_cli_arguments(cli_inputs):
    """
    Returns a sorted list of cli arguments.

    :param cli_inputs: The cli inputs description
    :return: A list of CliArguments
    """
    cli_arguments = []
    for input_key, cli_input_description in cli_inputs.items():
        cli_arguments.append(CliArgument.from_cli_input_description(
            input_key, cli_input_description))
    return sorted(cli_arguments, key=lambda cli_argument: cli_argument.argument_position)


@total_ordering
class CliArgumentPosition:
    class CliArgumentPositionType(enum.Enum):
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
