import glob
import hashlib
import os
import sys

import enum
import shutil
import stat
import subprocess
import json
import tempfile
from argparse import ArgumentParser
from json import JSONDecodeError

from traceback import format_exc
from typing import List, Dict

DESCRIPTION = 'Run an experiment as described in a RESTRICTED_RED_FILE.'
JSON_INDENT = 2

FILE_CHUNK_SIZE = 1024 * 1024


def attach_args(parser):
    parser.add_argument(
        'restricted_red_file', action='store', type=str, metavar='RESTRICTED_RED_FILE',
        help='RESTRICTED_RED_FILE (json) containing an experiment description as local PATH or http URL.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()

    result = run(args)

    print(json.dumps(result, indent=JSON_INDENT))

    if result['state'] == 'succeeded':
        return 0

    return 1


def get_restricted_red_data(restricted_red_location):
    """
    Tries to load the file as local file.

    :param restricted_red_location: A local file path as string
    :return: The content of the given file as dictionary
    """
    try:
        with open(restricted_red_location, 'r') as restricted_red_file:
            return json.load(restricted_red_file)
    except FileNotFoundError as file_error:
        raise ExecutionError(
            'Could not find restricted RED file "{}" locally. Failed with the following message:\n{}'
            .format(restricted_red_location, str(file_error))
        )
    except JSONDecodeError as e:
        raise ExecutionError(
            'Could not parse restricted RED file "{}". File is not json formatted.\n{}'
            .format(restricted_red_location, str(e))
        )


def run(args):
    result = {
        'debugInfo': None,
        'outputs': None,
        'state': 'succeeded'
    }

    connector_manager = ConnectorManager()
    try:
        restricted_red_location = args.restricted_red_file
        output_mode = OutputMode.Connectors

        restricted_red_data = get_restricted_red_data(restricted_red_location)

        if output_mode == OutputMode.Connectors and 'outputs' not in restricted_red_data:
            raise ExecutionError(
                '--outputs/-o argument is set but no outputs section is defined in RESTRICTED_RED_FILE.'
            )

        outputs = restricted_red_data.get('outputs', {})
        cli = restricted_red_data.get('cli', {})
        cli_outputs = cli.get('outputs', {})
        cli_stdout = cli.get('stdout')
        cli_stderr = cli.get('stderr')

        connector_manager.import_output_connectors(
            outputs, cli_outputs, output_mode, cli_stdout, cli_stderr)

        if not args.disable_connector_validation:
            connector_manager.validate_connectors(
                validate_outputs=(output_mode == OutputMode.Connectors))

        # send files and directories
        if output_mode == OutputMode.Connectors:
            connector_manager.send_connectors()

    except Exception as e:
        print_exception(e)
        result['debugInfo'] = exception_format()
        result['state'] = 'failed'
    finally:
        # umount directories
        umount_errors = [_format_exception(
            e) for e in connector_manager.umount_connectors()]
        if umount_errors:
            umount_errors.insert(0, 'Errors while unmounting directories:')
            result['debugInfo'] = umount_errors

    return result


class OutputMode(enum.Enum):
    Connectors = 0
    Directory = 1


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


class ConnectorManager:
    def __init__(self):
        self._output_runners = []  # type: List[OutputConnectorRunner]
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

        for output_key, output_value in outputs.items():
            cli_output_value = cli_outputs.get(output_key)
            if cli_output_value is None:
                raise KeyError('Could not find output key "{}" in cli description, but was given in "outputs".'
                               .format(output_key))

            runner = create_output_connector_runner(
                output_key,
                output_value,
                cli_output_value,
                self._connector_cli_version_cache,
                cli_stdout,
                cli_stderr
            )

            self._output_runners.append(runner)

    def validate_connectors(self, validate_outputs):
        """
        Validates connectors.

        :param validate_outputs: If True, output runners are validated
        """
        if validate_outputs:
            for runner in self._output_runners:
                runner.validate_send()

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

    def outputs_to_dict(self):
        """
        Translates the imported output connectors into a dictionary.

        :return: A dictionary containing status information about all imported output connectors
        """
        outputs_dict = {}

        for output_runner in self._cli_output_runners:
            outputs_dict[output_runner.get_output_key()
                         ] = output_runner.to_dict()

        return outputs_dict

    def check_outputs(self):
        """
        Checks if all output files/directories are present relative to the given working directory

        :raise ConnectorError: If an output file/directory could not be found
        """
        for runner in self._cli_output_runners:
            runner.check_output()


def _resolve_glob_pattern(glob_pattern, connector_type=None):
    """
    Tries to resolve the given glob_pattern.

    :param glob_pattern: The glob pattern to resolve
    :param connector_type: The connector class to search for
    :return: the resolved glob_pattern as list of strings
    :rtype: List[str]
    """
    glob_result = glob.glob(os.path.abspath(glob_pattern))
    if connector_type == OutputConnectorType.File:
        glob_result = [f for f in glob_result if os.path.isfile(f)]
    elif connector_type == OutputConnectorType.Directory:
        glob_result = [f for f in glob_result if os.path.isdir(f)]
    return glob_result


def _resolve_glob_pattern_and_throw(glob_pattern, output_key, connector_type=None):
    """
    Tries to resolve the given glob_pattern. Raises an error, if the pattern could not be resolved or is ambiguous

    :param glob_pattern: The glob pattern to resolve
    :param output_key: The corresponding output key for Exception text
    :param connector_type: The connector class to search for
    :return: The resolved path as string
    :raise ConnectorError: If the given glob_pattern could not be resolved or is ambiguous
    """
    paths = _resolve_glob_pattern(glob_pattern, connector_type)
    if len(paths) == 1:
        return paths[0]
    elif len(paths) == 0:
        raise ConnectorError(
            'Could not resolve glob "{}" for output key "{}". File/Directory not found.'
            .format(glob_pattern, output_key)
        )
    else:
        raise ConnectorError(
            'Could not resolve glob "{}" for output key "{}". Glob is ambiguous.'.format(
                glob_pattern, output_key)
        )


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

    def to_dict(self):
        """
        Returns a dictionary representing this output file

        :return: A dictionary containing information about this output file
        """
        dict_representation = {
            'class': self._output_class.to_string(),
            'glob': self._glob_pattern,
        }

        paths = _resolve_glob_pattern(
            self._glob_pattern, self._output_class.connector_type)

        if len(paths) == 0:
            dict_representation['path'] = None
        elif len(paths) == 1:
            path = paths[0]

            if self._output_class.is_file_like():
                # dict_representation['checksum'] = calculate_file_checksum(path)
                dict_representation['size'] = os.path.getsize(path)

            dict_representation['path'] = path
        else:
            dict_representation['path'] = paths

        return dict_representation

    def check_output(self):
        """
        Checks if the corresponding output is present in the working directory.

        :raise ConnectorError: If the corresponding file/directory is not present on disk
        """
        glob_result = _resolve_glob_pattern(
            self._glob_pattern,
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
                file_checksum = calculate_file_checksum(path)
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
                    path, self._listing)
                if listing_content_check:
                    raise ConnectorError(
                        'Listing validation for output key "{}" failed:\n{}'
                        .format(self._output_key, listing_content_check)
                    )


class ExecutionResult:
    def __init__(self, std_out, std_err, return_code):
        """
        Initializes a new ExecutionResult

        :param std_out: The std_err of the execution as list of strings
        :type std_out: list[str]
        :param std_err: The std_out of the execution as list of strings
        :type std_err: list[str]
        :param return_code: The return code of the execution
        """
        self.std_out = std_out
        self.std_err = std_err
        self.return_code = return_code

    def get_std_err(self):
        if self.std_err is None:
            return None
        return '\n'.join(self.std_err)

    def get_connector_error_text(self):
        """
        Returns a readable text describing the error that occurred during the execution of a connector.

        :return: A text describing an error
        :rtype: str
        """
        error_text = []

        if self.return_code < 0:
            error_text.append(
                'Connector was killed by the operating system with signal {}'.format(-self.return_code))

        stderr = self.get_std_err()
        if stderr:
            error_text.append(stderr)
        else:
            stdout = self.get_std_out()
            if stdout:
                error_text.append(
                    'Connector stderr was empty. Showing stdout.\n{}'.format(stdout))

        if not error_text:
            error_text.append(
                'Could not get error message. Connector stderr and stdout is empty.')

        return '\n'.join(error_text)

    def get_std_out(self):
        if self.std_out is None:
            return None
        return '\n'.join(self.std_out)

    def successful(self):
        return self.return_code == 0

    def to_dict(self):
        d = {'returnCode': self.return_code}
        if self.std_out:
            d['stdOut'] = self.std_out
        if self.std_err:
            d['stdErr'] = self.std_err
        return d


def _exec(command, work_dir, stdout=None, stderr=None):
    """
    Executes the given command.

    :param command: The command to execute
    :param work_dir: The working directory where to execute the command
    :param stdout: Specifies a path, where the stdout file should be created. If None subprocess.PIPE is used.
    :param stderr: Specifies a path, where the stderr file should be created. If None subprocess.PIPE is used.
    :return: a tuple (return_code, stdout, stderr). If a filename for stdout/stderr is given, the return code will
             contain None for stdout/stderr
    """
    if stdout is None:
        stdout_file = subprocess.PIPE
    else:
        stdout_file = open(stdout, 'w')

    if stderr is None:
        stderr_file = subprocess.PIPE
    else:
        stderr_file = open(stderr, 'w')

    try:
        sp = subprocess.Popen(
            command,
            stdout=stdout_file,
            stderr=stderr_file,
            cwd=work_dir,
            universal_newlines=True,
            encoding='utf-8'
        )
    except TypeError:
        sp = subprocess.Popen(
            command,
            stdout=stdout_file,
            stderr=stderr_file,
            cwd=work_dir,
            universal_newlines=True
        )

    std_out, std_err = sp.communicate()
    return_code = sp.returncode

    return return_code, std_out, std_err


def execute(command, work_dir=None, stdout_file=None, stderr_file=None):
    """
    Executes a given commandline command and returns a dictionary with keys: 'returnCode', 'stdOut', 'stdErr'

    :param command: The command to execute as list of strings.
    :param work_dir: The working directory for the executed command
    :param stdout_file: A path, specifying where the stdout of the command should be saved. If None stdout is returned
                        in the execution result
    :param stderr_file: A path, specifying where the stderr of the command should be saved. If None stderr is returned
                        in the execution result
    :return: An ExecutionResult. stdout/stderr of the execution result will be None, if stdout_file/stderr_file is given
    :rtype: ExecutionResult

    :raise ExecutionError: If the file to execute could not be found
    """
    if shutil.which(command[0]) is None:
        raise ExecutionError('Command "{}" not in PATH.'.format(command[0]))

    try:
        return_code, std_out, std_err = _exec(
            command, work_dir, stdout=stdout_file, stderr=stderr_file)
    except FileNotFoundError as e:
        raise ExecutionError(
            'Command "{}" not found.\n{}'.format(command[0], str(e)))

    except PermissionError as e:
        raise ExecutionError(
            'Could not execute command "{}" in directory "{}". PermissionError:\n{}'.format(
                command, os.getcwd(), str(e)
            )
        )

    if std_out:
        std_out = _split_lines(std_out)

    if std_err:
        std_err = _split_lines(std_err)

    return ExecutionResult(std_out, std_err, return_code)


def exception_format():
    exc_text = format_exc()
    return [_lstrip_quarter(line.replace("'", '').rstrip()) for line in exc_text.split('\n') if line]


def _lstrip_quarter(s):
    len_s = len(s)
    s = s.lstrip()
    len_s_strip = len(s)
    quarter = (len_s - len_s_strip) // 4
    return ' ' * quarter + s


def _format_exception(exception):
    return '[{}]\n{}\n'.format(type(exception).__name__, str(exception))


def print_exception(exception):
    """
    Prints the exception message and the name of the exception class to stderr.

    :param exception: The exception to print
    """
    print(_format_exception(exception), file=sys.stderr)


def _split_lines(lines):
    return [line for line in lines.split(os.linesep) if line]


class ConnectorError(Exception):
    pass


class ExecutionError(Exception):
    pass


class RedSpecificationError(Exception):
    pass


class JobSpecificationError(Exception):
    pass


def resolve_connector_cli_version(connector_command, connector_cli_version_cache):
    """
    Returns the cli-version of the given connector.

    :param connector_command: The connector command to resolve the cli-version for.
    :param connector_cli_version_cache: Cache for connector cli version
    :return: The cli version string of the given connector
    :raise ConnectorError: If the cli-version could not be resolved.
    """
    cache_value = connector_cli_version_cache.get(connector_command)
    if cache_value:
        return cache_value

    try:
        result = execute([connector_command, 'cli-version'])
    except ExecutionError as e:
        raise ConnectorError(
            'Failed to execute connector "{}"\n{}'.format(connector_command, str(e)))

    std_out = result.std_out
    if result.successful() and len(std_out) == 1:
        cli_version = std_out[0]
        connector_cli_version_cache[connector_command] = cli_version
        return cli_version
    else:
        std_err = result.get_connector_error_text()
        raise ConnectorError(
            'Could not detect cli version for connector "{}". Failed with following message:\n{}'
            .format(connector_command, std_err)
        )


def execute_connector(connector_command, top_level_argument, access=None, path=None, listing=None):
    """
    Executes the given connector command with

    :param connector_command: The connector command to execute
    :param top_level_argument: The top level argument of the connector
    :param access: An access dictionary, if given the connector is executed with a temporary file as argument, that
                   contains the access information
    :param path: The path where to receive the file/directory to or which file/directory to send
    :param listing: An optional listing, that is given to the connector as temporary file
    :return: A dictionary with keys 'returnCode', 'stdOut', 'stdErr'
    """
    # create access file
    access_file = None
    if access is not None:
        access_file = tempfile.NamedTemporaryFile('w')
        json.dump(access, access_file)
        access_file.flush()

    # create listing file
    listing_file = None
    if listing is not None:
        listing_file = tempfile.NamedTemporaryFile('w')
        json.dump(listing, listing_file)
        listing_file.flush()

    # build command
    command = [connector_command, top_level_argument]
    if access_file is not None:
        command.append('{}'.format(access_file.name))
    if path is not None:
        command.append('{}'.format(path))
    if listing_file is not None:
        command.append('--listing={}'.format(listing_file.name))

    # execute connector
    execution_result = execute(command)

    # remove temporary files
    if access_file is not None:
        access_file.close()
    if listing_file is not None:
        listing_file.close()

    return execution_result


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


def calculate_file_checksum(path):
    """
    Calculates the sha1 checksum of a given file. The checksum is formatted in the following way: 'sha1$<checksum>'

    :param path: The path to the file, whose checksum should be calculated.
    :return: The sha1 checksum of the given file as string
    """
    hasher = hashlib.sha1()
    with open(path, 'rb') as file:
        while True:
            buf = file.read(FILE_CHUNK_SIZE)
            if buf:
                hasher.update(buf)
            else:
                break
    return 'sha1${}'.format(hasher.hexdigest())


def is_directory_writable(d):
    """
    Returns whether the given directory is writable or not. Assumes, that it is present in the local filesystem.

    :param d: The directory to check, whether it is writable
    :return: True, if the given directory is writable, otherwise False
    """
    st = os.stat(d)
    user_has_permissions = bool(st.st_mode & stat.S_IRUSR) and bool(
        st.st_mode & stat.S_IWUSR)
    group_has_permissions = bool(st.st_mode & stat.S_IRGRP) and bool(
        st.st_mode & stat.S_IWGRP)
    others_have_permissions = bool(
        st.st_mode & stat.S_IROTH) and bool(st.st_mode & stat.S_IWOTH)

    return user_has_permissions or group_has_permissions or others_have_permissions


def ensure_directory(d):
    """
    Ensures that directory d exists, is empty and is writable

    :param d: The directory that you want to make sure is either created or exists already.
    :type d: str
    :raise PermissionError: If the directory exists, but is not writable
    :raise FileExistsError: If the directory already exists and is not empty
    """
    if os.path.exists(d):
        if os.listdir(d):
            raise FileExistsError(
                'Directory "{}" already exists and is not empty.'.format(d))
        else:
            return
    os.makedirs(d)

    # check write permissions
    if not is_directory_writable(d):
        raise PermissionError('Directory "{}" is not writable.'.format(d))


def get_listing_information(path, listing):
    """
    Creates a dictionary that contains readable information about a given directory, that is present in the local
    filesystem under path.

    :param path: The path where the directory is present in the local filesystem
    :param listing: The listing to get information about. Every file/directory in listing should contain a basename,
                    which has to be present in the filesystem.
    :type listing: list[dict]
    :return: A dictionary containing ['class', 'basename', 'checksum', 'size'] for every file in the given listing and
             ['class', 'basename'] (and optional 'listing') for every directory.
    """
    listing_information = []

    for sub in listing:
        sub_information = {}
        sub_path = os.path.join(path, sub['basename'])

        if sub['class'] == 'File':
            sub_information['class'] = 'File'
            sub_information['basename'] = sub['basename']
            # sub_information['checksum'] = calculate_file_checksum(sub_path)
            sub_information['size'] = os.path.getsize(sub_path)
        elif sub['class'] == 'Directory':
            sub_information['class'] = 'Directory'
            sub_information['basename'] = sub['basename']

            sub_listing = sub.get('listing')
            if sub_listing:
                sub_information['listing'] = get_listing_information(
                    sub_path, sub_listing)

        listing_information.append(sub_information)

    return listing_information


def directory_listing_content_check(directory_path, listing):
    """
    Checks if a given listing is present under the given directory path.

    :param directory_path: The path to the base directory
    :param listing: The listing to check
    :return: None if no errors could be found, otherwise a string describing the error
    """
    for sub in listing:
        path = os.path.join(directory_path, sub['basename'])
        if sub['class'] == 'File':
            file_check_result = _directory_listing_file_check(sub, path)
            if file_check_result is not None:
                return file_check_result
        elif sub['class'] == 'Directory':
            if not os.path.isdir(path):
                return 'listing contains "{}" but this directory could not be found on disk'.format(path)
            listing = sub.get('listing')
            if listing:
                res = directory_listing_content_check(path, listing)
                if res is not None:
                    return res
    return None


def _directory_listing_file_check(file_description, path):
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
        file_checksum = calculate_file_checksum(path)
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


class OutputConnectorRunner:
    """
    A OutputConnectorRunner can be used to execute different output functions of a Connector.

    A ConnectorRunner subclass is associated with a connector cli-version.
    Subclasses implement different cli-versions for connectors.

    A ConnectorRunner instance is associated with a restricted_red input, that uses a connector.
    For every restricted_red output, that uses a connector a new OutputConnectorRunner instance is created.
    """

    def __init__(self, output_key, connector_command, output_class, access, glob_pattern, listing=None):
        """
        initiates a OutputConnectorRunner.

        :param output_key: The restricted_red output key
        :type output_key: str
        :param connector_command: The connector command to execute
        :type connector_command: str
        :param output_class: The ConnectorClass for this output
        :type output_class: OutputConnectorClass
        :param access: The access information for the connector
        :type access: dict
        :param glob_pattern: The glob_pattern to match
        :type glob_pattern: str
        :param listing: An optional listing for the associated connector
        :type listing: list
        """
        self._output_key = output_key
        self._connector_command = connector_command
        self._output_class = output_class
        self._access = access
        self._glob_pattern = glob_pattern
        self._listing = listing

    def get_output_key(self):
        return self._output_key

    def validate_send(self):
        """
        Executes send_file_validate, send_dir_validate or send_mount_validate depending on input_class and mount
        """
        if self._output_class.is_directory():
            self.send_dir_validate()
        elif self._output_class.is_file_like():
            self.send_file_validate()

    def try_send(self):
        """
        Executes send_file or send_dir depending on input_class.

        :raise ConnectorError: If the given glob_pattern could not be resolved or is ambiguous.
                               Or if the executed connector fails.
        """
        path = _resolve_glob_pattern_and_throw(
            self._glob_pattern,
            self._output_key,
            self._output_class.connector_type
        )

        if self._output_class.is_file_like():
            self.send_file(path)
        elif self._output_class.is_directory():
            self.send_dir(path)

    def send_file_validate(self):
        raise NotImplementedError()

    def send_file(self, path):
        raise NotImplementedError()

    def send_dir_validate(self):
        raise NotImplementedError()

    def send_dir(self, path):
        raise NotImplementedError()


class OutputConnectorRunner01(OutputConnectorRunner):
    """
    This OutputConnectorRunner implements the connector cli-version 0.1
    """

    def send_file(self, path):
        execution_result = execute_connector(
            self._connector_command,
            'send-file',
            access=self._access,
            path=path
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to send file for output key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self._output_key, execution_result.return_code, execution_result.get_connector_error_text()
                )
            )

    def send_file_validate(self):
        execution_result = execute_connector(
            self._connector_command,
            'send-file-validate',
            access=self._access
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to validate send file for output key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self._output_key, execution_result.return_code, execution_result.get_connector_error_text()
                )
            )

    def send_dir(self, path):
        execution_result = execute_connector(
            self._connector_command,
            'send-dir',
            access=self._access,
            path=path,
            listing=self._listing
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to validate send directory for output key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'
                .format(self._output_key, execution_result.return_code, execution_result.get_connector_error_text())
            )

    def send_dir_validate(self):
        execution_result = execute_connector(
            self._connector_command,
            'send-dir-validate',
            access=self._access,
            listing=self._listing
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to validate send directory for output key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'
                .format(self._output_key, execution_result.return_code, execution_result.get_connector_error_text())
            )


CONNECTOR_CLI_VERSION_OUTPUT_RUNNER_MAPPING = {
    '0.1': OutputConnectorRunner01,
    '1': OutputConnectorRunner01  # cli version 1 is equal to 0.1
}


def create_output_connector_runner(
        output_key,
        output_value,
        cli_output_value,
        connector_cli_version_cache,
        cli_stdout,
        cli_stderr
):
    """
    Creates a proper OutputConnectorRunner instance for the given connector command.

    :param output_key: The output key of the runner
    :param output_value: The output to create a runner for
    :param cli_output_value: The cli description for the runner
    :param connector_cli_version_cache: Cache for connector cli version
    :param cli_stdout: The path to the stdout file
    :param cli_stderr: The path to the stderr file
    :return: A ConnectorRunner
    """
    try:
        connector_data = output_value['connector']
        connector_command = connector_data['command']
        access = connector_data['access']

        output_class = OutputConnectorClass.from_string(
            cli_output_value['type'])

        if output_class.is_stdout():
            if cli_stdout is None:
                raise ConnectorError(
                    'Type of output key "{}" is "stdout", but no stdout file specified in cli section of red file'
                    .format(output_key)
                )
            glob_pattern = cli_stdout
        elif output_class.is_stderr():
            if cli_stderr is None:
                raise ConnectorError(
                    'Type of output key "{}" is "stderr", but no stderr file specified in cli section of red file'
                    .format(output_key)
                )
            glob_pattern = cli_stderr
        else:
            glob_pattern = cli_output_value['outputBinding']['glob']
    except KeyError as e:
        raise ConnectorError(
            'Could not create connector for output key "{}".\nThe following property was not found: "{}"'
            .format(output_key, str(e))
        )

    mount = connector_data.get('mount', False)
    listing = output_value.get('listing')

    try:
        cli_version = resolve_connector_cli_version(
            connector_command, connector_cli_version_cache)
    except ConnectorError:
        raise ConnectorError('Could not resolve connector cli version for connector "{}" in output key "{}"'
                             .format(connector_command, output_key))

    if mount and not output_class.is_directory():
        raise ConnectorError('Connector for input key "{}" has mount flag set but class is "{}". '
                             'Unable to mount if class is different from "Directory"'
                             .format(output_key, output_class.to_string()))

    connector_runner_class = CONNECTOR_CLI_VERSION_OUTPUT_RUNNER_MAPPING.get(
        cli_version)
    if connector_runner_class is None:
        raise Exception('This agent does not support connector cli-version "{}", but needed by connector "{}"'
                        .format(cli_version, connector_command))

    connector_runner = connector_runner_class(
        output_key,
        connector_command,
        output_class,
        access,
        glob_pattern,
        listing
    )

    return connector_runner


class ConnectorError(Exception):
    pass


class RedSpecificationError(Exception):
    pass


class JobSpecificationError(Exception):
    pass
