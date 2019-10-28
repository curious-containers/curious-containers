"""
                Host                    Container
blue_file       in memory               /cc/blue_file.json
blue_agent      <import...>             /cc/blue_agent.py
outputs         ./outputs[_batch_id]    /cc/outputs (defined in red_to_restricted_red.py)
"""
import os

from argparse import ArgumentParser
from typing import List
from enum import Enum
from uuid import uuid4

from cc_core.commons.docker_utils import create_batch_archive
from cc_core.commons.engines import engine_validation
from cc_core.commons.exceptions import print_exception, exception_format, AgentError, JobExecutionError, TemplateError
from cc_core.commons.files import load_and_read, dump_print
from cc_core.commons.gpu_info import get_gpu_requirements, match_gpus, InsufficientGPUError
from cc_core.commons.red import red_validation
from cc_core.commons.red_to_restricted_red import convert_red_to_restricted_red, CONTAINER_OUTPUT_DIR, CONTAINER_AGENT_PATH, \
    CONTAINER_BLUE_FILE_PATH
from cc_core.commons.templates import get_secret_values, normalize_keys, get_template_keys

from cc_faice.commons.templates import complete_red_templates
from cc_faice.commons.docker import env_vars, DockerManager

DESCRIPTION = 'Run an experiment as described in a REDFILE with ccagent red in a container.'

PYTHON_INTERPRETER = 'python3'


class OutputMode(Enum):
    Connectors = 0
    Directory = 1


def run(red_data,
        disable_pull=False,
        leave_container=False,
        preserve_environment=None,
        insecure=False,
        output_mode=OutputMode.Connectors,
        gpu_ids=None,
        ):
    """
    Executes a RED Experiment.

    :param red_data: A dict containing red data that will be executed. This red data cannot contain template keys
    :param disable_pull: If True the docker image is not pulled from an registry
    :param leave_container: If set to True, the executed docker container will not be removed.
    :param preserve_environment: List of environment variables to preserve inside the docker container.
    :param insecure: Allow insecure capabilities
    :param output_mode: Either Connectors or Directory. If Connectors, the blue agent will try to execute the output
                        connectors. If Directory faice will copy the output files into the host output directory.
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
        # validation
        red_validation(red_data, output_mode == OutputMode.Directory, container_requirement=True)
        engine_validation(red_data, 'container', ['docker'], optional=False)

        check_for_template_keys(red_data)

        # templates and secrets
        secret_values = get_secret_values(red_data)

        # process red data
        blue_batches = convert_red_to_restricted_red(red_data)

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

        if len(blue_batches) == 1:
            host_outdir = 'outputs'
        else:
            host_outdir = 'outputs_{batch_index}'

        for batch_index, blue_batch in enumerate(blue_batches):
            container_execution_result = run_blue_batch(
                blue_batch=blue_batch,
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
            result['containers'].append(container_execution_result.to_dict())
            container_execution_result.raise_for_state()
    except Exception as e:
        print_exception(e, secret_values)
        result['debugInfo'] = exception_format(secret_values)
        result['state'] = 'failed'

    return result


def check_for_template_keys(red_data):
    """
    Raises an TemplateError, if template keys are found in the given red data. Template keys should be resolved by the
    faice client, before committing them to the faice execution engine.

    :param red_data: The red data that is checked
    :type red_data: dict

    :raise TemplateError: If template keys are found in the given red_data
    """
    template_keys = set()
    get_template_keys(red_data, template_keys)
    if template_keys:
        raise TemplateError(
            'Found template keys in red_data. Please resolve them before committing to faice execution engine. '
            'The following keys were found: "{}"'.format(', '.join(template_keys))
        )


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


def _get_blue_batch_mount_keys(blue_batch):
    """
    Returns a list of input/output keys, that use mounting connectors

    :param blue_batch: The blue batch to analyse
    :return: A list of input/outputs keys as strings
    """
    mount_connectors = []

    # check input keys
    for input_key, input_value in blue_batch['inputs'].items():
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


class ContainerExecutionResult:
    def __init__(self, state, command, container_name, agent_execution_result, agent_std_err, container_stats):
        """
        Creates a new Container Execution Result.

        :param state: The state of the agent execution ('failed', 'successful')
        :param command: The command, that executes the blue agent inside the docker container
        :param container_name: The name of the docker container
        :param agent_execution_result: The parsed json output of the blue agent
        :param agent_std_err: The std err as list of string of the blue agent
        :param container_stats: The stats of the executed container, given as dictionary
        """
        self.state = state
        self.command = command
        self.container_name = container_name
        self.agent_execution_result = agent_execution_result
        self.agent_std_err = agent_std_err
        self.container_stats = container_stats

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
            'dockerStats': self.container_stats
        }

    def raise_for_state(self):
        """
        Raises an AgentError, if state is not successful.

        :raise AgentError: If state is not successful
        """
        if not self.successful():
            raise AgentError(self.agent_std_err)


def run_blue_batch(blue_batch,
                   docker_manager,
                   docker_image,
                   host_outdir,
                   output_mode,
                   leave_container,
                   batch_index,
                   ram,
                   gpus,
                   environment,
                   insecure):
    """
    Executes an blue agent inside a docker container that takes the given blue batch as argument.

    :param blue_batch: The blue batch to execute
    :param docker_manager: The docker manager to use for executing the batch
    :type docker_manager: DockerManager
    :param docker_image: The docker image url to use. This docker image should be already present on the host machine
    :param host_outdir: The outputs directory of the host. This is mounted as outdir inside the docker container
                        mounted into the docker container, where host_outputs_dir is the host directory.
    :param output_mode: If output mode == Connectors the blue agent will be started with '--outputs' flag
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

    command = _create_blue_agent_command()

    if output_mode == OutputMode.Connectors:
        command.append('--outputs')

    is_mounting = define_is_mounting(blue_batch, insecure)

    container = docker_manager.create_container(
        name=container_name,
        image=docker_image,
        working_directory=CONTAINER_OUTPUT_DIR,
        ram=ram,
        gpus=gpus,
        environment=environment,
        enable_fuse=is_mounting,
    )

    with create_batch_archive(blue_batch) as blue_archive:
        docker_manager.put_archive(container, blue_archive)

    # hack to make fuse working under osx
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

    agent_execution_result = docker_manager.run_command(container, command, user='cc')

    blue_agent_result = agent_execution_result.get_agent_result_dict()

    if blue_agent_result['state'] == 'succeeded':
        state = ExecutionResultType.Succeeded

        # create outputs directory
        if output_mode == OutputMode.Directory:
            abs_host_outdir = os.path.abspath(host_outdir.format(batch_index=batch_index))
            _handle_directory_outputs(abs_host_outdir, blue_agent_result['outputs'], container, docker_manager)
    else:
        state = ExecutionResultType.Failed

    container.stop()

    if not leave_container:
        container.remove()

    return ContainerExecutionResult(
        state,
        command,
        container_name,
        blue_agent_result,
        agent_execution_result.get_stderr(),
        agent_execution_result.get_stats()
    )


def _handle_directory_outputs(host_outdir, outputs, container, docker_manager):
    """
    Creates the host_outdir and retrieves the files given in outputs from the docker container. The retrieved files are
    then stored in the created host_outdir.

    :param host_outdir: The absolute path to the output directory of the host.
    :type host_outdir: str
    :param outputs: A dictionary mapping output_keys to file information.
    :type outputs: Dict[str, Dict]
    :param container: The container to get the outputs from
    :type container: Container
    :param docker_manager: The docker manager from which to retrieve the files
    :type docker_manager: DockerManager

    :raise AgentError: If a file given in outputs could not be retrieved by the docker manager
    """

    os.makedirs(host_outdir, exist_ok=True)

    for output_key, output_file_information in outputs.items():
        container_file_path = output_file_information['path']

        # continue, if the output file was not found
        if container_file_path is None:
            continue

        file_path = os.path.join(CONTAINER_OUTPUT_DIR, container_file_path)

        if not file_path:
            continue

        try:
            file_archive = docker_manager.get_file_archive(container, file_path)
        except AgentError as e:
            raise AgentError(
                'Could not retrieve output file "{}" with path "{}" from docker container. '
                'Failed with the following message:\n{}'
                .format(output_key, file_path, str(e))
            )

        file_archive.extractall(host_outdir)
        file_archive.close()


def define_is_mounting(blue_batch, insecure):
    mount_connectors = _get_blue_batch_mount_keys(blue_batch)
    if mount_connectors:
        if not insecure:
            raise Exception(
                'The following keys are mounting directories {}.\nTo enable mounting inside a docker container run '
                'faice with --insecure (see --help).\nBe aware that this will enable SYS_ADMIN capabilities in order to'
                ' enable FUSE mounts.'.format(mount_connectors)
            )
        return True
    return False


def _create_blue_agent_command():
    """
    Defines the command to execute inside the docker container to execute the blue agent.
    The resulting command looks similar to "python3 /cc/blue_agent.py /cc/blue_file.json --debug"

    :return: A list of strings to execute inside the docker container.
    :rtype: List[str]
    """
    return [PYTHON_INTERPRETER, CONTAINER_AGENT_PATH, CONTAINER_BLUE_FILE_PATH, '--debug']
