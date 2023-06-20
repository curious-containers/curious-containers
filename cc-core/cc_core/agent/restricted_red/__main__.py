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
from functools import total_ordering
from json import JSONDecodeError
from traceback import format_exc
from typing import List, Dict


DESCRIPTION = 'Run an experiment as described in a RESTRICTED_RED_FILE.'
JSON_INDENT = 2

RESTRICTED_RED_INPUT_CLASSES = {'File', 'Directory'}
FILE_CHUNK_SIZE = 1024 * 1024


def attach_args(parser):
    parser.add_argument(
        'restricted_red_file', action='store', type=str, metavar='RESTRICTED_RED_FILE',
        help='RESTRICTED_RED_FILE (json) containing an experiment description as local PATH or http URL.'
    )
    parser.add_argument(
        '-o', '--outputs', action='store_true',
        help='Enable connectors specified in the RESTRICTED_RED_FILE outputs section.'
    )
    parser.add_argument(
        '--disable-connector-validation', action='store_true',
        help='Skip connector validation.'
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


class OutputMode(enum.Enum):
    Connectors = 0
    Directory = 1


def run(args):
    result = {
        'command': None,
        'process': {
            'returnCode': None,
            'executed': False,
            'stdout': None,
            'stderr': None
        },
        'debugInfo': None,
        'inputs': None,
        'outputs': None,
        'state': 'succeeded'
    }

    connector_manager = ConnectorManager()
    try:
        restricted_red_location = args.restricted_red_file
        if args.outputs:
            output_mode = OutputMode.Connectors
        else:
            output_mode = OutputMode.Directory

        restricted_red_data = get_restricted_red_data(restricted_red_location)

        if output_mode == OutputMode.Connectors and 'outputs' not in restricted_red_data:
            raise ExecutionError(
                '--outputs/-o argument is set but no outputs section is defined in RESTRICTED_RED_FILE.'
            )

        # validate base_command and build command
        base_command = restricted_red_data.get('command')
        _validate_command(base_command)
        result['command'] = base_command
        cli_arguments = get_cli_arguments(restricted_red_data['cli']['inputs'])
        command = generate_command(base_command, cli_arguments, restricted_red_data)
        
        # mount cc-cloud
        mount_cloud(restricted_red_data.get('cloud'))

        # import, validate and execute connectors
        inputs = restricted_red_data.get('inputs')
        if inputs is None:
            raise ExecutionError('Invalid RESTRICTED_RED_FILE. "inputs" is not specified.')
        connector_manager.import_input_connectors(inputs)

        outputs = restricted_red_data.get('outputs', {})
        cli = restricted_red_data.get('cli', {})
        cli_outputs = cli.get('outputs', {})
        cli_stdout = cli.get('stdout')
        cli_stderr = cli.get('stderr')

        connector_manager.import_output_connectors(outputs, cli_outputs, output_mode, cli_stdout, cli_stderr)
        connector_manager.prepare_directories()

        if not args.disable_connector_validation:
            connector_manager.validate_connectors(validate_outputs=(output_mode == OutputMode.Connectors))

        connector_manager.receive_connectors()
        result['inputs'] = connector_manager.inputs_to_dict()

        # execute command
        try:
            execution_result = execute(command, stdout_file=cli_stdout, stderr_file=cli_stderr)
        except ExecutionError:
            raise

        result['process']['returnCode'] = execution_result.return_code
        result['process']['executed'] = True
        result['process']['stdout'] = cli_stdout
        result['process']['stderr'] = cli_stderr
        if not execution_result.successful():
            raise ExecutionError('Execution of command "{}" failed.'.format(' '.join(command)))

        # check output files/directories
        connector_manager.check_outputs()
        result['outputs'] = connector_manager.outputs_to_dict()

        # send files and directories
        if output_mode == OutputMode.Connectors:
            connector_manager.send_connectors()

    except Exception as e:
        print_exception(e)
        result['debugInfo'] = exception_format()
        result['state'] = 'failed'
    finally:
        # umount directories
        umount_errors = [_format_exception(e) for e in connector_manager.umount_connectors()]
        if umount_errors:
            umount_errors.insert(0, 'Errors while unmounting directories:')
            result['debugInfo'] = umount_errors

    return result


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


def _validate_command(command):
    if command is None:
        raise ExecutionError('Invalid RESTRICTED_RED_FILE. "command" is not specified.')

    if not isinstance(command, list):
        raise ExecutionError(
            'Invalid RESTRICTED_RED_FILE. "command" has to be a list of strings.\ncommand: "{}"'.format(command)
        )

    for s in command:
        if not isinstance(s, str):
            raise ExecutionError(
                'Invalid RESTRICTED_RED_FILE. "command" has to be a list of strings.\ncommand: "{}"\n'
                '"{}" is not a string'.format(command, s)
            )


def is_directory_writable(d):
    """
    Returns whether the given directory is writable or not. Assumes, that it is present in the local filesystem.

    :param d: The directory to check, whether it is writable
    :return: True, if the given directory is writable, otherwise False
    """
    st = os.stat(d)
    user_has_permissions = bool(st.st_mode & stat.S_IRUSR) and bool(st.st_mode & stat.S_IWUSR)
    group_has_permissions = bool(st.st_mode & stat.S_IRGRP) and bool(st.st_mode & stat.S_IWGRP)
    others_have_permissions = bool(st.st_mode & stat.S_IROTH) and bool(st.st_mode & stat.S_IWOTH)

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
            raise FileExistsError('Directory "{}" already exists and is not empty.'.format(d))
        else:
            return
    os.makedirs(d)

    # check write permissions
    if not is_directory_writable(d):
        raise PermissionError('Directory "{}" is not writable.'.format(d))
    

def mount_cloud(cloud):
    """
    Mount the remote cc-cloud directory using SSHFS.

    :param cloud: A dictionary containing cloud configuration details
    :type cloud: dict
    :raise OSError: If there is an error during the mounting process or when updating the known_hosts file.
    """
    if cloud and cloud.get('enable'):
        disableStrictHostKeyChecking = ''
        if cloud['disableStrictHostKeyChecking']:
            disableStrictHostKeyChecking = '-o StrictHostKeyChecking=no'
        else:
            append_known_host(cloud['host'], cloud['publicsshKey'])
        os.system(f"echo {cloud['auth']['password']} | sshfs -p {cloud['auth']['sshPort']} -o sshfs_sync -o password_stdin {disableStrictHostKeyChecking} {cloud['auth']['ssh_user']}@{cloud['host']}:/{cloud['upload_directory_name']} {cloud['mountDir']}")


def append_known_host(host, public_key):
    """
    Append a known host entry to the SSH known_hosts file.

    :param host: The hostname or IP address of the remote host.
    :type host: str
    :param public_key: The public key associated with the remote host.
    :type host: str
    :raise OSError: If there is an error while creating or writing to the known_hosts file.
    """
    known_hosts_path = os.path.expanduser('~/.ssh/known_hosts')
    known_host_entry = f"{host} {public_key}\n"
    os.makedirs(os.path.dirname(known_hosts_path), exist_ok=True)
    with open(known_hosts_path, 'a') as known_hosts_file:
        known_hosts_file.write(known_host_entry)


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
        raise ConnectorError('Failed to execute connector "{}"\n{}'.format(connector_command, str(e)))

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


class InputConnectorType(enum.Enum):
    File = 0
    Directory = 1


class InputConnectorClass:
    def __init__(self, connector_type, is_array, is_optional):
        self.connector_type = connector_type
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

        connector_type = None
        for ct in InputConnectorType:
            if s == ct.name:
                connector_type = ct

        if connector_type is None:
            raise ConnectorError(
                'Could not extract input connector class from string "{}". Connector classes should start with "File" '
                'or "Directory" and optionally end with "[]" or "?" or "[]?"'.format(s)
            )

        return InputConnectorClass(connector_type, is_array, is_optional)

    def to_string(self):
        if self._is_array:
            return '{}[]'.format(self.connector_type.name)
        else:
            return self.connector_type.name

    def __repr__(self):
        return self.to_string()

    def __eq__(self, other):
        return (self.connector_type == other.connector_type) and (self._is_array == other.is_array())

    def is_file(self):
        return self.connector_type == InputConnectorType.File

    def is_directory(self):
        return self.connector_type == InputConnectorType.Directory

    def is_array(self):
        return self._is_array

    def is_optional(self):
        return self._is_optional


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
                sub_information['listing'] = get_listing_information(sub_path, sub_listing)

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
                   '\n\tgiven checksum: "{}"\n\tfile checksum : "{}"'.format(path, checksum, file_checksum)

    size = file_description.get('size')
    if size is not None:
        file_size = os.path.getsize(path)
        if size != file_size:
            return 'file size of "{}" does not match the file size given in listing.' \
                   '\n\tgiven size: {}\n\tfile size : {}'.format(path, size, file_size)

    return None


class InputConnectorRunner:
    """
    A ConnectorRunner can be used to execute the different functions of a Connector.

    A ConnectorRunner subclass is associated with a connector cli-version.
    Subclasses implement different cli-versions for connectors.

    A ConnectorRunner instance is associated with a restricted_red input, that uses a connector.
    For every restricted_red input, that uses a connector a new ConnectorRunner instance is created.
    """

    def __init__(self,
                 input_key,
                 input_index,
                 connector_command,
                 input_class,
                 mount,
                 access,
                 path,
                 listing=None,
                 checksum=None,
                 size=None):
        """
        Initiates an InputConnectorRunner.

        :param input_key: The restricted_red input key
        :param input_index: The input index in case of File/Directory lists
        :param connector_command: The connector command to execute
        :param input_class: Either 'File' or 'Directory'
        :type input_class: ConnectorClass
        :param mount: Whether the associated connector mounts or not
        :param access: The access information for the connector
        :param path: The path where to put the data
        :param listing: An optional listing for the associated connector
        :param checksum: An optional checksum (sha1 hash) for the associated file
        :param size: The optional size of the associated file in bytes
        """
        self._input_key = input_key
        self._input_index = input_index
        self._connector_command = connector_command
        self._input_class = input_class
        self._mount = mount
        self._access = access
        self._path = path
        self._listing = listing
        self._checksum = checksum
        self._size = size

        # Is set to true, after mounting
        self._has_mounted = False

    def to_dict(self):
        """
        Returns a dictionary representing this input file or directory

        :return: A dictionary containing information about this input file or directory
        """
        dict_representation = {
            'class': self._input_class.to_string(),
            'path': self._path,
        }

        if self._input_class.is_file():
            # dict_representation['checksum'] = calculate_file_checksum(self._path)
            dict_representation['size'] = os.path.getsize(self._path)
        elif self._input_class.is_directory() and self._listing:
            listing = get_listing_information(self._path, self._listing)
            dict_representation['listing'] = listing

        return dict_representation

    def get_input_class(self):
        return self._input_class

    def is_mounting(self):
        """
        :return: Returns whether this runner is mounting or not.
        """
        return self._mount

    def prepare_directory(self):
        """
        In case of input_class == 'Directory' creates path.
        In case of input_class == 'File' creates os.path.dirname(path).

        :raise ConnectorError: If the directory could not be created or if the path already exist.
        """
        path_to_create = self._path if self._input_class.is_directory() else os.path.dirname(self._path)

        try:
            ensure_directory(path_to_create)
        except PermissionError as e:
            raise ConnectorError('Could not prepare directory for input key "{}" with path "{}". PermissionError:\n{}'
                                 .format(self.format_input_key(), path_to_create, str(e)))
        except FileExistsError as e:
            raise ConnectorError('Could not prepare directory for input key "{}" with path "{}". '
                                 'Directory already exists and is not empty.\n{}'
                                 .format(self.format_input_key(), path_to_create, str(e)))

    def _receive_directory_content_check(self):
        """
        Checks if the given directory exists and if listing is set, if the listing is fulfilled.

        :raise ConnectorError: If the directory content is not as expected.
        """
        if not os.path.isdir(self._path):
            raise ConnectorError('Content check for input directory "{}" failed. Path "{}" does not exist.'
                                 .format(self.format_input_key(), self._path))

        if self._listing:
            listing_check_result = directory_listing_content_check(self._path, self._listing)
            if listing_check_result is not None:
                raise ConnectorError('Content check for input key "{}" failed. Listing is not fulfilled:\n{}'
                                     .format(self.format_input_key(), listing_check_result))

    def _receive_file_content_check(self):
        """
        Checks if the given file exists. If a checksum is given checks if this checksum matches. If a size is given
        checks if this size matches the file size.

        :raise ConnectorError: If the given file does not exist, if the given hash does not match or if the given file
                               size does not match.
        """
        if not os.path.isfile(self._path):
            raise ConnectorError('Content check for input file "{}" failed. Path "{}" does not exist.'
                                 .format(self.format_input_key(), self._path))
        if self._checksum:
            file_checksum = calculate_file_checksum(self._path)
            if self._checksum != file_checksum:
                raise ConnectorError('Content check for input file "{}" failed. The given checksum "{}" '
                                     'does not match the checksum calculated from the file "{}".'
                                     .format(self.format_input_key(), self._checksum, file_checksum))

        if self._size is not None:
            size = os.path.getsize(self._path)
            if self._size != size:
                raise ConnectorError('Content check for input file "{}" failed. The given file size "{}" '
                                     'does not match the calculated file size "{}".'
                                     .format(self.format_input_key(), self._size, size))

    def validate_receive(self):
        """
        Executes receive_file_validate, receive_dir_validate or mount_dir_validate depending on input_class and mount
        """
        if self._input_class.is_directory():
            if self._mount:
                self.mount_dir_validate()
            else:
                self.receive_dir_validate()
        elif self._input_class.is_file():
            self.receive_file_validate()

    def receive(self):
        """
        Executes receive_file, receive_directory or receive_mount depending on input_class and mount
        """
        if self._input_class.is_directory():
            if self._mount:
                self.mount_dir()
                self._receive_directory_content_check()
                self._has_mounted = True
            else:
                self.receive_dir()
                self._receive_directory_content_check()
        elif self._input_class.is_file():
            self.receive_file()
            self._receive_file_content_check()

    def try_umount(self):
        """
        Executes umount, if connector is mounting and has mounted, otherwise does nothing.

        :raise ConnectorError: If the Connector fails to umount the directory
        """
        if self._has_mounted:
            self.umount_dir()

    def format_input_key(self):
        return format_key_index(self._input_key, self._input_index)

    def receive_file(self):
        raise NotImplementedError()

    def receive_file_validate(self):
        raise NotImplementedError()

    def receive_dir(self):
        raise NotImplementedError()

    def receive_dir_validate(self):
        raise NotImplementedError()

    def mount_dir_validate(self):
        raise NotImplementedError()

    def mount_dir(self):
        raise NotImplementedError()

    def umount_dir(self):
        raise NotImplementedError()


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
            'Could not resolve glob "{}" for output key "{}". Glob is ambiguous.'.format(glob_pattern, output_key)
        )


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

        paths = _resolve_glob_pattern(self._glob_pattern, self._output_class.connector_type)

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
                        '\n\tfile checksum : "{}"'.format(self._output_key, self._checksum, file_checksum)
                    )

            if self._size is not None:
                file_size = os.path.getsize(path)
                if file_size != self._size:
                    raise ConnectorError(
                        'The given file size for output key "{}" does not match.\n\tgiven size: {}'
                        '\n\tfile size : {}'.format(self._output_key, self._size, file_size)
                    )

            if self._listing:
                listing_content_check = directory_listing_content_check(path, self._listing)
                if listing_content_check:
                    raise ConnectorError(
                        'Listing validation for output key "{}" failed:\n{}'
                        .format(self._output_key, listing_content_check)
                    )


class InputConnectorRunner01(InputConnectorRunner):
    """
    This InputConnectorRunner implements the connector cli-version 0.1
    """

    def receive_file(self):
        execution_result = execute_connector(
            self._connector_command,
            'receive-file',
            access=self._access,
            path=self._path
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to receive file for input key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self.format_input_key(), execution_result.return_code, execution_result.get_connector_error_text()
                )
            )

    def receive_file_validate(self):
        execution_result = execute_connector(
            self._connector_command,
            'receive-file-validate',
            access=self._access
        )
        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to validate receive file for input key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self.format_input_key(), execution_result.return_code, execution_result.get_connector_error_text()
                )
            )

    def receive_dir(self):
        execution_result = execute_connector(
            self._connector_command,
            'receive-dir',
            access=self._access,
            path=self._path,
            listing=self._listing
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to receive directory for input key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self.format_input_key(), execution_result.return_code, execution_result.get_connector_error_text()
                )
            )

    def receive_dir_validate(self):
        execution_result = execute_connector(
            self._connector_command,
            'receive-dir-validate',
            access=self._access,
            listing=self._listing
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to validate receive directory for input key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self.format_input_key(), execution_result.return_code, execution_result.get_connector_error_text()
                )
            )

    def mount_dir(self):
        execution_result = execute_connector(
            self._connector_command,
            'mount-dir',
            access=self._access,
            path=self._path
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to mount directory for input key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self.format_input_key(), execution_result.return_code, execution_result.get_connector_error_text()
                )
            )

    def mount_dir_validate(self):
        execution_result = execute_connector(
            self._connector_command,
            'mount-dir-validate',
            access=self._access
        )

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to validate mount directory for input key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self.format_input_key(), execution_result.return_code, execution_result.get_connector_error_text()
                )
            )

    def umount_dir(self):
        execution_result = execute_connector(self._connector_command, 'umount-dir', path=self._path)

        if not execution_result.successful():
            raise ConnectorError(
                'Connector failed to umount directory for input key "{}".\nConnector return code: {}\n'
                'Failed with the following message:\n{}'.format(
                    self.format_input_key(), execution_result.return_code, execution_result.get_connector_error_text()
                )
            )


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


CONNECTOR_CLI_VERSION_INPUT_RUNNER_MAPPING = {
    '0.1': InputConnectorRunner01,
    '1': InputConnectorRunner01  # cli version 1 is equal to 0.1
}


def create_input_connector_runner(input_key, input_value, input_index, assert_class, assert_list,
                                  connector_cli_version_cache):
    """
    Creates a proper InputConnectorRunner instance for the given connector command.

    :param input_key: The input key of the runner
    :param input_value: The input to create an runner for
    :param input_index: The index of the input in case of File/Directory lists
    :param assert_class: Assert this input class
    :param assert_list: Assert the input class to be a list of Files or Directories. Otherwise fail.
    :param connector_cli_version_cache: Cache for connector cli version
    :return: A ConnectorRunner
    :rtype InputConnectorRunner
    """
    try:
        try:
            connector_data = input_value['connector']
        except TypeError:
            raise Exception('input key: {}; input_value: {}; index: {}'.format(input_key, input_value, input_index))
        connector_command = connector_data['command']
        access = connector_data['access']

        clazz = input_value['class']
        if assert_list:
            clazz = '{}[]'.format(clazz)

        input_class = InputConnectorClass.from_string(clazz)
        path = input_value['path']
    except KeyError as e:
        raise ConnectorError('Could not create connector for input key "{}".\n'
                             'The following property was not found: "{}"'
                             .format(format_key_index(input_key, input_index), str(e)))

    mount = connector_data.get('mount', False)
    listing = input_value.get('listing')
    checksum = input_value.get('checksum')
    size = input_value.get('size')

    try:
        cli_version = resolve_connector_cli_version(connector_command, connector_cli_version_cache)
    except ConnectorError as e:
        raise ConnectorError(
            'Could not resolve connector cli version for connector "{}" in input key "{}". Failed with the following'
            ' message:\n{}'.format(connector_command, format_key_index(input_key, input_index), str(e))
        )

    if mount and not input_class.is_directory():
        raise ConnectorError('Connector for input key "{}" has mount flag set but class is "{}". '
                             'Unable to mount if class is different from "Directory"'
                             .format(format_key_index(input_key, input_index), input_class.to_string()))

    # check if is ConnectorType matches
    if assert_list and not input_class.is_array():
        raise ConnectorError('Connector for input key "{}" is given as list, but input class is not list.'
                             .format(format_key_index(input_key, input_index)))
    if (assert_list is None) and input_class.is_array():
        raise ConnectorError('Connector for input key "{}" is not given as list, but input class is list.'
                             .format(format_key_index(input_key, input_index)))
    if (assert_class is not None) and (assert_class != input_class):
        raise ConnectorError('Connector for input key "{}" has unexpected class "{}". Expected class is "{}"'
                             .format(format_key_index(input_key, input_index), input_class, assert_class))

    connector_runner_class = CONNECTOR_CLI_VERSION_INPUT_RUNNER_MAPPING.get(cli_version)
    if connector_runner_class is None:
        raise Exception('This agent does not support connector cli-version "{}", but needed by connector "{}"'
                        .format(cli_version, connector_command))

    connector_runner = connector_runner_class(
        input_key,
        input_index,
        connector_command,
        input_class,
        mount,
        access,
        path,
        listing,
        checksum,
        size
    )

    return connector_runner


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

        output_class = OutputConnectorClass.from_string(cli_output_value['type'])

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
        cli_version = resolve_connector_cli_version(connector_command, connector_cli_version_cache)
    except ConnectorError:
        raise ConnectorError('Could not resolve connector cli version for connector "{}" in output key "{}"'
                             .format(connector_command, output_key))

    if mount and not output_class.is_directory():
        raise ConnectorError('Connector for input key "{}" has mount flag set but class is "{}". '
                             'Unable to mount if class is different from "Directory"'
                             .format(output_key, output_class.to_string()))

    connector_runner_class = CONNECTOR_CLI_VERSION_OUTPUT_RUNNER_MAPPING.get(cli_version)
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
        output_class = OutputConnectorClass.from_string(cli_output_value['type'])
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
            error_text.append('Connector was killed by the operating system with signal {}'.format(-self.return_code))

        stderr = self.get_std_err()
        if stderr:
            error_text.append(stderr)
        else:
            stdout = self.get_std_out()
            if stdout:
                error_text.append('Connector stderr was empty. Showing stdout.\n{}'.format(stdout))

        if not error_text:
            error_text.append('Could not get error message. Connector stderr and stdout is empty.')

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
        return_code, std_out, std_err = _exec(command, work_dir, stdout=stdout_file, stderr=stderr_file)
    except FileNotFoundError as e:
        raise ExecutionError('Command "{}" not found.\n{}'.format(command[0], str(e)))

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


def format_key_index(input_key, input_index=None):
    if input_index is None:
        return input_key
    return '{}:{}'.format(input_key, input_index)


class ConnectorManager:
    def __init__(self):
        self._input_runners = []  # type: List[InputConnectorRunner]
        self._output_runners = []  # type: List[OutputConnectorRunner]
        self._cli_output_runners = []  # type: List[CliOutputRunner]
        self._connector_cli_version_cache = {}  # type: Dict[str, str]

    def import_input_connectors(self, inputs):
        """
        Creates InputConnectorRunner for every key in inputs (or more Runners for File/Directory lists).

        :param inputs: The inputs to create Runner for
        """
        for input_key, input_value in inputs.items():
            if not _is_connector_input_value(input_value):
                continue

            if isinstance(input_value, dict):
                runner = create_input_connector_runner(
                    input_key,
                    input_value,
                    None,
                    None,
                    False,
                    self._connector_cli_version_cache
                )
                self._input_runners.append(runner)
            elif isinstance(input_value, list):
                assert_class = None
                for index, sub_input in enumerate(input_value):
                    runner = create_input_connector_runner(
                        input_key,
                        sub_input,
                        index,
                        assert_class,
                        True,
                        self._connector_cli_version_cache
                    )
                    assert_class = runner.get_input_class()
                    self._input_runners.append(runner)

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
        if output_mode == OutputMode.Connectors:
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
            raise ConnectorError('{} output connectors failed:\n{}'.format(errors_len, '\n'.join(error_strings)))

    def inputs_to_dict(self):
        """
        Translates the imported input connectors into a dictionary.

        :return: A dictionary containing status information about all imported input connectors
        """
        inputs_dict = {}

        for input_runner in self._input_runners:
            inputs_dict[input_runner.format_input_key()] = input_runner.to_dict()

        return inputs_dict

    def outputs_to_dict(self):
        """
        Translates the imported output connectors into a dictionary.

        :return: A dictionary containing status information about all imported output connectors
        """
        outputs_dict = {}

        for output_runner in self._cli_output_runners:
            outputs_dict[output_runner.get_output_key()] = output_runner.to_dict()

        return outputs_dict

    def check_outputs(self):
        """
        Checks if all output files/directories are present relative to the given working directory

        :raise ConnectorError: If an output file/directory could not be found
        """
        for runner in self._cli_output_runners:
            runner.check_output()

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


# ------------ Command building -----------------

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
            raise RedSpecificationError('The given input type "{}" is not valid'.format(s))

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


def _is_connector_input_value(input_value):
    """
    Returns whether the given input value defines a connector.

    :param input_value: The input value as list or value, that may contain a connector
    :return: True, if input value contains a connector definition, otherwise false
    """
    if isinstance(input_value, list):
        if not input_value:
            return False

        for sub_input_value in input_value:
            if not _is_connector_input_value(sub_input_value):
                return False
        return True
    elif isinstance(input_value, dict):
        return input_value.get('class') in RESTRICTED_RED_INPUT_CLASSES

    return False


if __name__ == '__main__':
    exit(main())
