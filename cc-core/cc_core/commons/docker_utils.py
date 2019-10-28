import io
import json
import tarfile
from typing import List

import docker
from cc_core.commons.gpu_info import GPUDevice, NVIDIA_GPU_VENDOR
from docker.errors import DockerException
from requests.exceptions import ConnectionError
# noinspection PyProtectedMember
from docker.models.containers import Container, _create_container_args
from docker.models.images import Image

from cc_core.commons.engines import NVIDIA_DOCKER_RUNTIME
from cc_core.commons.files import create_directory_tarinfo
from cc_core.commons.red_to_blue import CONTAINER_AGENT_PATH, CONTAINER_BLUE_FILE_PATH, CONTAINER_OUTPUT_DIR, \
    CONTAINER_INPUT_DIR

GPU_CAPABILITIES = [['gpu'], ['nvidia'], ['compute'], ['compat32'], ['graphics'], ['utility'], ['video'], ['display']]
GPU_QUERY_IMAGE = 'nvidia/cuda:8.0-runtime'


def create_container_with_gpus(client, image, command, available_runtimes, gpus=None, environment=None, **kwargs):
    """
    Creates a docker container with optional gpus, accessible by nvidia runtime or nvidia-container-toolkit.

    If gpus are required it first looks for the nvidia runtime. If nvidia runtime is configured, sets this nvidia
    runtime and nvidia environment variables.
    If gpus are required, but no nvidia runtime is configured, a device request for the requested gpus is created.

    :param client: The docker client to use for the creation of the container.
    :type client: docker.DockerClient
    :param image: The image for the docker container
    :type image: str
    :param command: The command to execute inside this container
    :type command: str or list[str]
    :param available_runtimes: A list of available docker runtimes configured inside the given client
    :type available_runtimes: list[str]
    :param gpus: One of the following options:
                 - The string 'all' to use all available gpus
                 - An int representing the number of gpus to use
                 - a list of device ids or uuids
                 - None to not use gpus
    :type gpus: str or int or List[str or int]
    :param environment: The environment of this docker container
    :type environment: dict
    :param kwargs: The same arguments as in docker.DockerClient.containers.create(kwargs)

    :return: A newly created docker container
    :rtype: Container

    :raise DockerException: If the connection to the docker daemon is broken
    """
    try:
        if gpus:
            if environment is None:
                environment = {}
            environment['NVIDIA_VISIBLE_DEVICES'] = _get_nvidia_visible_devices_from_gpus(gpus)
            kwargs['environment'] = environment

            if NVIDIA_DOCKER_RUNTIME in available_runtimes:
                kwargs['runtime'] = NVIDIA_DOCKER_RUNTIME
                container = client.containers.create(image, command, **kwargs)
            else:
                # if nvidia runtime is not installed on this docker daemon, but gpus are required:
                # try creation with device request
                container = _create_with_nvidia_container_toolkit(client, image, command, gpus, kwargs)
        else:
            container = client.containers.create(image, command, environment=environment, **kwargs)
    except ConnectionError as e:
        raise DockerException(
            'Could not create docker container. Failed with the following ConnectionError:\n{}'.format(str(e))
        )
    return container


def _create_with_nvidia_container_toolkit(client, image, command, gpus, kwargs):
    """
    This function adds the gpu option to the normal client.containers.create(...) function and adds a device request.
    This function does not modify the environment variable.

    :param client: The docker client to use for this create
    :type client: docker.DockerClient
    :param image: The image for this docker container
    :type image: str
    :param command: The command for this docker container
    :type command: str or list[str]
    :param gpus: One of the following options:
                 - The string 'all' to use all available gpus
                 - An int representing the number of gpus to use
                 - a list of device ids or uuids
    :type gpus: str or int or List[str or int]
    :param kwargs: The kwargs of the docker.DockerClient.containers.create() function
    """
    # start addition
    device_request = _get_gpu_device_request(gpus)
    # end addition

    if isinstance(image, docker.models.images.Image):
        image = image.id
    kwargs['image'] = image
    kwargs['command'] = command
    # noinspection PyProtectedMember
    kwargs['version'] = client.api._version
    create_kwargs = _create_container_args(kwargs)

    # addition to the original create function
    create_kwargs['host_config']['DeviceRequests'] = [device_request]
    # end addition

    resp = client.api.create_container(**create_kwargs)
    return client.containers.get(resp['Id'])


def _get_gpu_device_request(gpus):
    """
    :param gpus: The string 'all', an int representing the number of gpus to use or a list of device ids
    :type gpus: str or int or List[str]
    """
    if gpus == 'all':
        return {
            'Driver': 'nvidia',
            'Capabilities': GPU_CAPABILITIES,
            'Count': -1,  # enable all gpus
        }

    elif isinstance(gpus, int):
        if gpus <= 0:
            raise ValueError('gpus is not a positive number: {}'.format(gpus))
        return {
            'Driver': 'nvidia',
            'Capabilities': GPU_CAPABILITIES,
            'Count': gpus,
        }

    elif isinstance(gpus, list):
        return {
            'Driver': 'nvidia',
            'Capabilities': GPU_CAPABILITIES,
            'DeviceIDs': [str(gpu) for gpu in gpus],
        }

    raise TypeError('gpus should be the string "all" an int or a list, but found "{}"'.format(gpus))


def _get_nvidia_visible_devices_from_gpus(gpus):
    """
    Returns the value for the NVIDIA_VISIBLE_DEVICES environment variable.

    :param gpus: The string 'all', an int representing the number of gpus to use or a list of device ids
    :type gpus: str or int or List[str]
    :return: The value for the NVIDIA_VISIBLE_DEVICES environment variable
    :rtype: str

    :raise ValueError: If gpus is an negative int
    :raise TypeError: If gpus is not one of the specified types
    """
    if gpus == 'all':
        return 'all'

    elif isinstance(gpus, int):
        if gpus <= 0:
            raise ValueError('gpus is not a positive number: {}'.format(gpus))
        return ','.join(map(str, range(gpus)))

    elif isinstance(gpus, list):
        return ','.join([str(gpu_id) for gpu_id in gpus])

    raise TypeError('gpus should be the string "all" an int or a list, but found "{}"'.format(gpus))


def create_batch_archive(blue_data):
    """
    Creates a tar archive that can be put into a cc_core container to execute the blue agent.

    This archive contains the blue agent, a blue file, the outputs-directory and the inputs-directory.
    The blue file is filled with the given blue data.
    The outputs-directory is an empty directory, with name 'outputs'
    The inputs-directory is an empty directory, with name 'inputs'
    The tar archive and the blue file are always in memory and never stored on the local filesystem.

    The resulting archive is:
    /cc
    |--/blue_agent.py
    |--/blue_file.json
    |--/outputs/
    |--/inputs/

    :param blue_data: The data to put into the blue file of the returned archive
    :type blue_data: dict
    :return: A tar archive containing the blue agent, a blue file, and input/output directories
    :rtype: io.BytesIO or bytes
    """
    data_file = io.BytesIO()
    tar_file = tarfile.open(mode='w', fileobj=data_file)

    # add blue agent
    tar_file.add(get_blue_agent_host_path(), arcname=CONTAINER_AGENT_PATH, recursive=False)

    # add blue file
    blue_batch_content = json.dumps(blue_data).encode('utf-8')
    # see https://bugs.python.org/issue22208 for more information
    blue_batch_tarinfo = tarfile.TarInfo(CONTAINER_BLUE_FILE_PATH)
    blue_batch_tarinfo.size = len(blue_batch_content)
    tar_file.addfile(blue_batch_tarinfo, io.BytesIO(blue_batch_content))

    # add outputs directory
    output_directory_tarinfo = create_directory_tarinfo(CONTAINER_OUTPUT_DIR, owner_name='cc')
    tar_file.addfile(output_directory_tarinfo)

    # add inputs_directory
    input_directory_tarinfo = create_directory_tarinfo(CONTAINER_INPUT_DIR, owner_name='cc')
    tar_file.addfile(input_directory_tarinfo)

    # close file
    tar_file.close()
    data_file.seek(0)

    return data_file


def get_blue_agent_host_path():
    """
    Returns the path of the blue agent in the host machine.

    :return: The path to the blue agent
    :rtype: str
    """
    import cc_core.agent.blue.__main__ as blue_main
    return blue_main.__file__


def image_to_str(image):
    """
    Converts a docker image into a readable string using the first tag if available otherwise the id.

    :param image: The image to convert to string
    :type image: Image
    :return: A string representation of the given image
    :rtype: str
    """
    tags = image.tags
    if tags:
        return tags[0]
    return str(image.id)


def detect_nvidia_docker_gpus(client, runtimes):
    """
    Returns a list of GPUDevices, which are available for the given docker client.

    This function starts a nvidia docker container and executes nvidia-smi in order to retrieve information about
    the gpus, that are available to the docker client.

    :param client: The docker client to use for gpu detection
    :type client: docker.DockerClient
    :param runtimes: The available runtimes for this docker client
    :type runtimes: List[str]

    :raise DockerException: If the stdout of the query could not be parsed or if the container execution failed

    :return: A list of GPUDevices
    :rtype: List[GPUDevice]
    """
    client.images.pull(GPU_QUERY_IMAGE)

    # this creates an csv output that contains gpu indices and their total memory in mega bytes
    command = [
        'nvidia-smi',
        '--query-gpu=index,memory.total',
        '--format=csv,noheader,nounits'
    ]

    try:
        container = create_container_with_gpus(
            client,
            GPU_QUERY_IMAGE,
            command=command,
            available_runtimes=runtimes,
            gpus='all'
        )  # type: Container
        container.start()
        container.wait()
        stdout = container.logs(stdout=True, stderr=False, stream=False)
        container.remove()
    except DockerException as e:
        raise DockerException(
            'Could not query gpus. Make sure the nvidia-runtime or nvidia-container-toolkit is configured on '
            'the docker host. Container failed with following message:\n{}'.format(str(e))
        )

    gpus = []
    for gpu_line in stdout.decode('utf-8').splitlines():
        try:
            index_text, memory_text = gpu_line.split(sep=',')  # type: str

            index = int(index_text.strip())
            memory = int(memory_text.strip())

            gpu = GPUDevice(index, memory, NVIDIA_GPU_VENDOR)
            gpus.append(gpu)

        except ValueError as e:
            raise DockerException(
                'Could not parse gpu query output:\n{}\nFailed with the following message:\n{}'
                .format(stdout, str(e))
            )

    return gpus
