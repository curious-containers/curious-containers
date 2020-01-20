"""
                          Host                    Container
restricted_red_file       in memory               /cc/restricted_red_file.json
restricted_red_agent      <import...>             /cc/restricted_red_agent.py
outputs                   ./outputs[_batch_id]    /cc/outputs (defined in red_to_restricted_red.py)
"""
import os

from typing import List
from enum import Enum
from uuid import uuid4
from pathlib import PurePosixPath, Path

from cc_core.commons.docker_utils import create_batch_archive, retrieve_file_archive, get_first_tarfile_member
from cc_core.commons.engines import engine_validation
from cc_core.commons.exceptions import brief_exception_text, exception_format, AgentError, JobExecutionError
from cc_core.commons.gpu_info import get_gpu_requirements, match_gpus, InsufficientGPUError
from cc_core.commons.red_to_restricted_red import convert_red_to_restricted_red, CONTAINER_OUTPUT_DIR, \
    CONTAINER_AGENT_PATH, CONTAINER_RESTRICTED_RED_FILE_PATH, RestrictedRedBatch
from cc_core.commons.red_secrets import get_secret_values

from cc_faice.commons.docker import env_vars, DockerManager
from red_val.red_validation import red_validation

DESCRIPTION = 'Run an experiment as described in a REDFILE with restricted red agent in a docker container.'

PYTHON_INTERPRETER = 'python3'
DEFAULT_STDOUT_HOST_FILE = 'stdout.txt'
DEFAULT_STDERR_HOST_FILE = 'stderr.txt'


class OutputMode(Enum):
    Connectors = 0
    Directory = 1


def run(
    red_data,
    disable_pull=False,
    leave_container=False,
    preserve_environment=None,
    insecure=False,
    output_mode=OutputMode.Connectors,
    gpu_ids=None
):
    """
    Executes a RED Experiment.

    :param red_data: A dict containing red data that will be executed. This red data cannot contain template keys
    :param disable_pull: If True the docker image is not pulled from an registry
    :param leave_container: If set to True, the executed docker container will not be removed.
    :param preserve_environment: List of environment variables to preserve inside the docker container.
    :param insecure: Allow insecure capabilities
    :param output_mode: Either Connectors or Directory. If Connectors, the restricted_red agent will try to execute the
                        output connectors. If Directory faice will copy the output files into the host output directory.
    :param gpu_ids: A list of gpu ids, that should be used. If None all gpus are considered.
    :type gpu_ids: List[int] or None

    :raise TemplateError: If template keys are found in the given red data
    """
    result = {
        'containers': [],
        'debugInfo': None,
        'state': 'succeeded'
    }

    secret_values = None

    try:
        # secrets
        secret_values = get_secret_values(red_data)

        # validation
        red_validation(
            red_data,
            output_mode == OutputMode.Directory,
            container_requirement=True,
            allow_variables=False
        )
        engine_validation(red_data, 'container', ['docker'], optional=False)

        # process red data
        restricted_red_batches = convert_red_to_restricted_red(red_data)

        # docker settings
        docker_image = red_data['container']['settings']['image']['url']
        ram = red_data['container']['settings'].get('ram')
        environment = env_vars(preserve_environment)

        # create docker manager
        docker_manager = DockerManager()

        # gpus
        gpus = get_gpus(docker_manager, red_data['container']['settings'].get('gpus'), gpu_ids)

        if not disable_pull:
            registry_auth = red_data['container']['settings']['image'].get('auth')
            docker_manager.pull(docker_image, auth=registry_auth)

        if len(restricted_red_batches) == 1:
            host_outdir = Path('outputs')
        else:
            host_outdir = Path('outputs_{batch_index}')

        for batch_index, restricted_red_batch in enumerate(restricted_red_batches):
            container_execution_result = run_restricted_red_batch(
                restricted_red_batch=restricted_red_batch,
                docker_manager=docker_manager,
                docker_image=docker_image,
                host_outdir=host_outdir,
                output_mode=output_mode,
                leave_container=leave_container,
                batch_index=batch_index,
                ram=ram,
                gpus=gpus,
                environment=environment,
                insecure=insecure
            )

            # handle execution result
            # noinspection PyTypeChecker
            result['containers'].append(container_execution_result.to_dict())
            result['furtherErrors'] = container_execution_result.get_further_errors()
            container_execution_result.raise_for_state()
    except Exception as e:
        result['briefExceptionText'] = brief_exception_text(e, secret_values)
        result['debugInfo'] = exception_format(secret_values)
        result['state'] = 'failed'

    return result


def get_gpu_devices(docker_manager, gpu_ids):
    """
    Gets all GPU devices that are available for this execution. If gpu_ids is given, the returned devices are limited to
    devices that are in gpu_ids. If a gpu_id is given, whose device could not be found, an InsufficientGPUError is
    raised.

    :param docker_manager: The DockerManager used to query gpus
    :type docker_manager: DockerManager
    :param gpu_ids: The gpu_ids specified by the user to use for the execution. If None, all gpus are considered.
    :type gpu_ids: List[int]

    :return: An iterable containing all gpu devices which are available for this execution
    :rtype: List[GPUDevice]

    :raise InsufficientGPUError: If a gpu_id was given, but no device with this gpu_id was found.
    """
    gpu_devices = docker_manager.get_nvidia_docker_gpus()

    # limit gpu devices to the given gpu ids, if given
    if gpu_ids:
        gpu_ids = gpu_ids.copy()
        # only use gpu devices, that are specified in gpu_ids
        used_gpu_devices = []
        for gpu_device in gpu_devices:
            if gpu_device.device_id in gpu_ids:
                used_gpu_devices.append(gpu_device)
                gpu_ids.remove(gpu_device.device_id)

        gpu_devices = used_gpu_devices

        # check for gpu_ids, that have no device
        if gpu_ids:
            raise InsufficientGPUError(
                'GPU id "{}" was specified by cli argument, but no device with this id was found'.format(gpu_ids)
            )

    return gpu_devices


def get_gpus(docker_manager, gpu_settings, gpu_ids):
    """
    Returns a list of gpus which are sufficient for the given gpu settings. Otherwise raise an Exception

    :param docker_manager: The DockerManager used to query gpus
    :type docker_manager: DockerManager
    :param gpu_settings: The gpu settings of the red experiment specifying the required gpus
    :type gpu_settings: Dict
    :param gpu_ids: The gpu_ids specified by the user to use for the execution. If None all gpus are considered.
    :type gpu_ids: List[int] or None

    :return: A list of GPUDevices to use for this experiment
    :rtype: List[GPUDevice]

    :raise InsufficientGPUError: If GPU settings could not be fulfilled
    """
    gpus = None

    gpu_requirements = get_gpu_requirements(gpu_settings)

    # dont do anything, if no gpus are required
    if gpu_requirements or gpu_ids:
        gpu_devices = get_gpu_devices(docker_manager, gpu_ids)

        gpus = match_gpus(gpu_devices, gpu_requirements)

        # if gpu_ids are specified, ignore gpu matching
        if gpu_ids:
            gpus = gpu_devices

    return gpus


def _get_restricted_red_batch_mount_keys(restricted_red_batch):
    """
    Returns a list of input/output keys, that use mounting connectors

    :param restricted_red_batch: The restricted_red batch to analyse
    :return: A list of input/outputs keys as strings
    """
    mount_connectors = []

    # check input keys
    for input_key, input_value in restricted_red_batch['inputs'].items():
        if not isinstance(input_value, list):
            input_value = [input_value]

        if not isinstance(input_value[0], dict):
            continue

        for input_value_element in input_value:
            connector = input_value_element.get('connector')
            if connector and connector.get('mount', False):
                mount_connectors.append(input_key)

    return mount_connectors


class ExecutionResultType(Enum):
    Succeeded = 0
    Failed = 1

    def __str__(self):
        return self.name.lower()


class FurtherExecutionErrors:
    def __init__(self, retrieve_stdout=None, retrieve_stderr=None):
        """
        Describes further errors, which can occur during the execution process.

        :param retrieve_stdout: A string describing the error, that occurred, while trying to retrieve the stdout file
               from the docker container
        :type retrieve_stdout: str
        :param retrieve_stderr: A string describing the error, that occurred, while trying to retrieve the stderr file
               from the docker container
        :type retrieve_stderr: str
        """
        self.retrieve_stdout = retrieve_stdout
        self.retrieve_stderr = retrieve_stderr

    def as_list(self):
        """
        Formats this object as list of strings.

        :return: A list of strings
        """
        result = []

        if self.retrieve_stdout:
            result.append('Error during fetching stdout:')
            result.append(self.retrieve_stdout)
        if self.retrieve_stderr:
            result.append('Error during fetching stderr:')
            result.append(self.retrieve_stderr)

        return result


class ContainerExecutionResult:
    def __init__(
            self,
            state,
            command,
            container_name,
            agent_execution_result,
            agent_std_err,
            container_stats,
            further_execution_errors
    ):
        """
        Creates a new Container Execution Result.

        :param state: The state of the agent execution ('failed', 'successful')
        :param command: The command, that executes the restricted_red agent inside the docker container
        :param container_name: The name of the docker container
        :param agent_execution_result: The parsed json output of the restricted_red agent
        :param agent_std_err: The std err as list of string of the restricted_red agent
        :param container_stats: The stats of the executed container, given as dictionary
        :param further_execution_errors: An object describing further errors during the execution process
        :type further_execution_errors: FurtherExecutionErrors
        """
        self.state = state
        self.command = command
        self.container_name = container_name
        self.agent_execution_result = agent_execution_result
        self.agent_std_err = agent_std_err
        self.container_stats = container_stats
        self.further_execution_errors = further_execution_errors

    def successful(self):
        return self.state == ExecutionResultType.Succeeded

    def to_dict(self):
        """
        Transforms self into a dictionary representation.

        :return: self as dictionary
        """
        return {
            'state': str(self.state),
            'command': self.command,
            'containerName': self.container_name,
            'agentStdOut': self.agent_execution_result,
            'agentStdErr': self.agent_std_err,
            # 'dockerStats': self.container_stats  # Excluded, because it does not contain useful information
        }

    def raise_for_state(self):
        """
        Raises an AgentError, if state is not successful.

        :raise AgentError: If state is not successful
        """
        if not self.successful():
            raise AgentError(self.agent_std_err)

    def get_further_errors(self):
        """
        :returns: the contained errors as list of strings
        """
        return self.further_execution_errors.as_list()


def run_restricted_red_batch(
        restricted_red_batch,
        docker_manager,
        docker_image,
        host_outdir,
        output_mode,
        leave_container,
        batch_index,
        ram,
        gpus,
        environment,
        insecure
):
    """
    Executes an restricted_red agent inside a docker container that takes the given restricted_red batch as argument.

    :param restricted_red_batch: The restricted_red batch to execute
    :type restricted_red_batch: RestrictedRedBatch
    :param docker_manager: The docker manager to use for executing the batch
    :type docker_manager: DockerManager
    :param docker_image: The docker image url to use. This docker image should be already present on the host machine
    :param host_outdir: The outputs directory of the host. This is mounted as outdir inside the docker container
                        mounted into the docker container, where host_outputs_dir is the host directory.
    :type host_outdir: Path
    :param output_mode: If output mode == Connectors the restricted_red agent will be started with '--outputs' flag
                        Otherwise this function will retrieve the output files with container.get_archive()
    :param leave_container: If True, the started container will not be stopped after execution.
    :param batch_index: The index of the current batch
    :param ram: The RAM limit for the docker container, given in MB
    :param gpus: The gpus to use for this batch execution
    :param environment: The environment to use for the docker container
    :param insecure: Allow insecure capabilities

    :return: A container result
    :rtype: ContainerExecutionResult
    """

    container_name = str(uuid4())

    command = _create_restricted_red_agent_command()

    if output_mode == OutputMode.Connectors:
        command.append('--outputs')

    is_mounting = define_is_mounting(restricted_red_batch.data, insecure)

    container = docker_manager.create_container(
        name=container_name,
        image=docker_image,
        working_directory=CONTAINER_OUTPUT_DIR,
        ram=ram,
        gpus=gpus,
        environment=environment,
        enable_fuse=is_mounting,
    )

    with create_batch_archive(restricted_red_batch.data) as restricted_red_archive:
        docker_manager.put_archive(container, restricted_red_archive)

    # hack to make fuse work under osx
    if is_mounting:
        set_osx_fuse_permissions_command = [
            'chmod',
            'o+rw',
            '/dev/fuse'
        ]
        osx_fuse_result = docker_manager.run_command(
            container,
            set_osx_fuse_permissions_command,
            user='root',
            work_dir='/'
        )
        if osx_fuse_result.return_code != 0:
            raise JobExecutionError(
               'Failed to set fuse permissions (exitcode: {}). Failed with the following message:\n{}\n{}'
               .format(osx_fuse_result.return_code, osx_fuse_result.get_stdout(), osx_fuse_result.get_stderr())
            )

    # run restricted red agent
    agent_execution_result = docker_manager.run_command(container, command)

    further_errors = FurtherExecutionErrors()

    restricted_red_agent_result = agent_execution_result.get_agent_result_dict()

    abs_host_outdir = Path(os.path.abspath(str(host_outdir).format(batch_index=batch_index)))  # type: Path
    if restricted_red_agent_result['state'] == 'succeeded':
        state = ExecutionResultType.Succeeded

        # handle stdout
        if restricted_red_batch.stdout_specified_by_user():
            _transfer_file(restricted_red_agent_result['process']['stdout'], container, abs_host_outdir)

        # handle stderr
        if restricted_red_batch.stderr_specified_by_user():
            _transfer_file(restricted_red_agent_result['process']['stderr'], container, abs_host_outdir)

        # create outputs directory
        if output_mode == OutputMode.Directory:
            _handle_directory_outputs(
                abs_host_outdir,
                restricted_red_agent_result['outputs'],
                container
            )
    else:
        state = ExecutionResultType.Failed

        # only create stdout/stderr, if user process was executed
        if restricted_red_agent_result['process']['executed']:
            errors_handling_stdout_stderr = _handle_stdout_stderr_on_failure(
                abs_host_outdir,
                restricted_red_batch,
                container
            )
            if errors_handling_stdout_stderr:
                further_errors.retrieve_stdout = errors_handling_stdout_stderr.get('stdout')
                further_errors.retrieve_stderr = errors_handling_stdout_stderr.get('stderr')

    # kill the /bin/sh container
    container.kill()

    if not leave_container:
        container.remove()

    return ContainerExecutionResult(
        state,
        command,
        container_name,
        restricted_red_agent_result,
        agent_execution_result.get_stderr(),
        agent_execution_result.get_stats(),
        further_errors
    )


def _handle_directory_outputs(host_outdir, outputs, container):
    """
    Creates the host_outdir and retrieves the files given in outputs from the docker container. The retrieved files are
    then stored in the created host_outdir.

    :param host_outdir: The absolute path to the output directory of the host.
    :type host_outdir: Path
    :param outputs: A dictionary mapping output_keys to file information.
    :type outputs: Dict[str, Dict]
    :param container: The container to get the outputs from
    :type container: Container

    :raise AgentError: If a file given in outputs could not be retrieved from the container
    """
    # noinspection PyTypeChecker
    os.makedirs(host_outdir, exist_ok=True)

    for output_key, output_file_information in outputs.items():
        container_file_path = output_file_information['path']

        # continue, if the output file was not found
        if container_file_path is None:
            continue

        _transfer_file(container_file_path, container, host_outdir)


def _transfer_file(container_path, container, host_outdir):
    """
    Transfers a given file path from the given container into the given host output directory

    :param container_path: The path inside the container where the source file is located. If absolute the absolute path
                           is taken. If relative it is relative to the cc CONTAINER_OUTPUT_DIR.
    :type container_path: PurePosixPath
    :param container: The docker container from which to transfer the file
    :type container: Container
    :param host_outdir: The host output directory where to copy the file
    :type host_outdir: Path

    :raise AgentError: If the given container file could not be retrieved
    """
    abs_container_path = CONTAINER_OUTPUT_DIR / container_path
    try:
        with retrieve_file_archive(container, abs_container_path) as file_archive:
            file_archive.extractall(host_outdir)
    except AgentError as e:
        raise AgentError(
            'Could not retrieve stdout file with path "{}" from docker container. '
            'Failed with the following message:\n{}'.format(abs_container_path, str(e))
        )


def _handle_stdout_stderr_on_failure(host_outdir, restricted_red_batch, container):
    """
    Creates the stdout/stderr file, if the process failed.

    :param host_outdir: The absolute path to the output directory of the host.
    :type host_outdir: Path
    :param restricted_red_batch: The restricted red data containing stdout/stderr information
    :type restricted_red_batch: RestrictedRedBatch
    :param container: The container to get the outputs from
    :type container: Container

    :returns: A dictionary mapping the strings 'stdout' / 'stderr' to a string describing the error that occurred,
              while trying to fetch stdout/stderr from the docker container. This dictionary is empty, if no errors
              occurred.
    :rtype: dict[str, str]
    """
    # noinspection PyTypeChecker
    os.makedirs(host_outdir, exist_ok=True)

    stdout_stderr = [
        ('stdout', 0, DEFAULT_STDOUT_HOST_FILE),
        ('stderr', 1, DEFAULT_STDERR_HOST_FILE)
    ]

    errors = {}

    for out_err, index, default_name in stdout_stderr:
        # define container path (e.g. /cc/outputs/stdout.txt)
        container_path = CONTAINER_OUTPUT_DIR / restricted_red_batch.data['cli'][out_err]

        # define host filename (e.g. stdout.txt)
        host_file_name = default_name
        if restricted_red_batch.stdout_stderr_specified_by_user[index]:
            host_file_name = restricted_red_batch.data['cli'][out_err]

        host_file_path = host_outdir / host_file_name

        try:
            with retrieve_file_archive(container, container_path) as file_archive:
                # copy archive file to outputs directory
                with get_first_tarfile_member(file_archive) as source_file:
                    with open(host_file_path, 'wb') as target_file:
                        for line in source_file.readlines():
                            target_file.write(line)

        except AgentError as e:
            errors[out_err] = 'Could not retrieve "{}" with path "{}" from docker container. ' \
                              'Failed with the following message:\n{}'.format(out_err, container_path, str(e))

    return errors or None


def define_is_mounting(restricted_red_batch, insecure):
    mount_connectors = _get_restricted_red_batch_mount_keys(restricted_red_batch)
    if mount_connectors:
        if not insecure:
            raise Exception(
                'The following keys are mounting directories {}.\nTo enable mounting inside a docker container run '
                'faice with --insecure (see --help).\nBe aware that this will enable SYS_ADMIN capabilities in order to'
                ' enable FUSE mounts.'.format(mount_connectors)
            )
        return True
    return False


def _create_restricted_red_agent_command():
    """
    Defines the command to execute inside the docker container to execute the restricted_red agent.
    The resulting command looks similar to "python3 /cc/restricted_red_agent.py /cc/restricted_red_file.json"

    :return: A list of strings to execute inside the docker container.
    :rtype: List[str]
    """
    return [PYTHON_INTERPRETER, CONTAINER_AGENT_PATH.as_posix(), CONTAINER_RESTRICTED_RED_FILE_PATH.as_posix()]
