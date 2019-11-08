import io
import json
import os
import tarfile
from typing import List

import docker
from docker.errors import DockerException, APIError
from docker.models.containers import Container
from docker.types import Ulimit
from requests.exceptions import ConnectionError

from cc_core.commons.docker_utils import create_container_with_gpus, detect_nvidia_docker_gpus
from cc_core.commons.exceptions import AgentError
from cc_core.commons.gpu_info import set_nvidia_environment_variables, GPUDevice

NOFILE_LIMIT = 4096


def env_vars(preserve_environment):
    if preserve_environment is None:
        return {}

    environment = {}

    for var in preserve_environment:
        if var in os.environ:
            environment[var] = os.environ[var]

    return environment


class AgentExecutionResult:
    def __init__(self, return_code, stdout, stderr, stats):
        """
        Creates a new AgentExecutionResult

        :param return_code: The return code of the execution
        :type return_code: int
        :param stdout: The decoded agent stdout
        :type stdout: str
        :param stderr: The decoded agent stderr
        :type stderr: str
        :param stats: A dictionary containing information about the container execution
        :type stats: Dict
        """
        self.return_code = return_code
        self._stdout = stdout
        self._parsed_stdout = None
        self._stderr = stderr
        self._stats = stats

    def get_stdout(self):
        return self._stdout

    def get_agent_result_dict(self):
        """
        This function parses the stdout only once.

        :return: The result of the agent as dictionary
        :rtype: Dict

        :raise AgentError: If the stdout of the agent is not valid json
        """
        if self._parsed_stdout is None:
            try:
                self._parsed_stdout = json.loads(self._stdout)
            except json.JSONDecodeError:
                raise AgentError(
                    'Could not parse stdout of agent.\n'
                    'Agent stdout:\n{}'
                    '\nAgent stderr:\n{}'
                    .format(self._stdout, self._stderr)
                )

        return self._parsed_stdout

    def get_stderr(self):
        return self._stderr

    def get_stats(self):
        """
        :return: the stats of the docker container after execution has finished
        :rtype: Dict
        """
        return self._stats


class DockerManager:
    def __init__(self):
        try:
            self._client = docker.from_env()
            info = self._client.info()  # This raises a ConnectionError, if the docker socket was not found
        except ConnectionError:
            raise DockerException('Could not connect to docker socket. Is the docker daemon running?')
        except DockerException:
            raise DockerException('Could not create docker client from environment.')

        self._runtimes = info.get('Runtimes')

    def get_nvidia_docker_gpus(self):
        """
        Returns a list of GPUDevices, which are available for this docker client.

        This function starts a nvidia docker container and executes nvidia-smi in order to retrieve information about
        the gpus, that are available to this docker_manager.

        :raise DockerException: If the stdout of the query could not be parsed or if the container execution failed

        :return: A list of GPUDevices
        :rtype: List[GPUDevice]
        """
        return detect_nvidia_docker_gpus(self._client, self._runtimes)

    def pull(self, image, auth=None):
        self._client.images.pull(image, auth_config=auth)

    def create_container(
            self,
            name,
            image,
            ram,
            working_directory,
            gpus=None,
            environment=None,
            enable_fuse=False
    ):
        """
        Creates a docker container with the given arguments. This docker container is running endlessly until
        container.stop() is called.
        If nvidia gpus are specified, the nvidia runtime is used, if available. Otherwise a device request for nvidia
        gpus is added.

        :param name: The name of the container
        :type name: str
        :param image: The image to use for this container
        :type image: str
        :param ram: The ram limit for this container in megabytes
        :type ram: int
        :param working_directory: The working directory inside the docker container
        :type working_directory: str
        :param gpus: A specification of gpus to enable in this docker container
        :type gpus: List[GPUDevice]
        :param environment: A dictionary containing environment variables, which should be set inside the container
        :type environment: Dict[str, Any]
        :param enable_fuse: If True, SYS_ADMIN capabilities are granted for this container and /dev/fuse is mounted
        :type enable_fuse: bool

        :return: The created container
        :rtype: Container

        :raise RuntimeNotSupportedError: If the specified runtime is not installed on the docker host
        """
        if environment is None:
            environment = {}

        mem_limit = None
        if ram is not None:
            mem_limit = '{}m'.format(ram)

        gpu_ids = None
        if gpus:
            set_nvidia_environment_variables(environment, map(lambda gpu: gpu.device_id, gpus))
            gpu_ids = [gpu.device_id for gpu in gpus]

        # enable fuse
        devices = []
        capabilities = []
        if enable_fuse:
            devices.append('/dev/fuse')
            capabilities.append('SYS_ADMIN')

        container = create_container_with_gpus(
            self._client,
            image,
            command='/bin/sh',
            gpus=gpu_ids,
            available_runtimes=self._runtimes,
            name=name,
            user='1000:1000',
            working_dir=working_directory,
            mem_limit=mem_limit,
            memswap_limit=mem_limit,
            environment=environment,
            cap_add=capabilities,
            devices=devices,
            ulimits=[Ulimit(name='nofile', soft=NOFILE_LIMIT, hard=NOFILE_LIMIT)],
            # needed to run the container endlessly
            tty=True,
            stdin_open=True,
            auto_remove=False,
        )
        container.start()

        return container

    @staticmethod
    def put_archive(container, archive):
        """
        Inserts the given tar archive into the container.

        :param container: The container to put the archive in
        :type container: Container
        :param archive: The archive, that is copied into the container
        :type archive: bytes
        """
        container.put_archive('/', archive)

    @staticmethod
    def run_command(container, command, user='1000:1000', work_dir=None):
        """
        Runs the given command in the given container and waits for the execution to end.

        :param container: The container to run the command in. The given container should be in state running, like it
                          is, if created by docker_manager.create_container()
        :type container: Container
        :param command: The command to execute inside the given docker container
        :type command: list[str] or str
        :param user: The user to execute the command
        :type user: str or int
        :param work_dir: The working directory where to execute the command
        :type work_dir: str

        :return: A agent execution result, representing the result of this container execution
        :rtype: AgentExecutionResult
        """
        try:
            return_code, logs = container.exec_run(
                cmd=command,
                user=user,
                workdir=work_dir,
                stdout=True,
                stderr=True,
                demux=True
            )
        except APIError as e:
            raise ValueError(
                'could not execute command "{}" in container "{}". Failed with the following message:\n{}'
                .format(command, container, str(e))
            )

        if logs[0] is None:
            stdout = None
        else:
            stdout = logs[0].decode('utf-8')

        if logs[1] is None:
            stderr = None
        else:
            stderr = logs[1].decode('utf-8')

        stats = container.stats(stream=False)

        return AgentExecutionResult(return_code, stdout, stderr, stats)

    @staticmethod
    def get_file_archive(container, file_path):
        """
        Retrieves the given file path as tar-archive from the internal docker container.

        :param container: The container to get the archive from
        :type container: Container
        :param file_path: A file path inside the docker container
        :type file_path: str

        :return: A tar archive, which corresponds to the given file path
        :rtype: tarfile.TarFile

        :raise AgentError: If the given file could not be fetched
        """
        try:
            bits, _ = container.get_archive(file_path)

            output_archive_bytes = io.BytesIO()
            for chunk in bits:
                output_archive_bytes.write(chunk)

            output_archive_bytes.seek(0)
        except DockerException as e:
            raise AgentError(str(e))

        return tarfile.TarFile(fileobj=output_archive_bytes)
