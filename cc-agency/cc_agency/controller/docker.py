import io
import json
import os
import sys
from threading import Thread, Event
import concurrent.futures
import time
from traceback import format_exc
from typing import List, Tuple, Dict
from tarfile import StreamError

import docker
from cc_core.commons.exceptions import log_format_exception
from docker.errors import DockerException, APIError
from docker.models.containers import Container
from docker.models.images import Image
from docker.tls import TLSConfig
from docker.types import Ulimit
import jsonschema
import pymongo
from requests.exceptions import ConnectionError, ReadTimeout
from bson.objectid import ObjectId
import bson.errors

from cc_core.commons.gpu_info import GPUDevice, NVIDIA_GPU_VENDOR
from cc_agency.commons.schemas.callback import agent_result_schema
from cc_agency.commons.secrets import get_experiment_secret_keys, fill_experiment_secrets, fill_batch_secrets, \
    get_batch_secret_keys, TrusteeClient
from cc_core.commons.docker_utils import create_container_with_gpus, create_batch_archive, image_to_str, \
    detect_nvidia_docker_gpus, retrieve_file_archive, get_first_tarfile_member
from cc_core.commons.red_to_restricted_red import convert_red_to_restricted_red, CONTAINER_OUTPUT_DIR, \
    CONTAINER_AGENT_PATH, CONTAINER_RESTRICTED_RED_FILE_PATH
from cc_agency.commons.helper import batch_failure, USER_SPECIFIED_STDOUT_KEY, USER_SPECIFIED_STDERR_KEY, \
    get_gridfs_filename, STDERR_FILE_KEY, STDOUT_FILE_KEY

INSPECTION_IMAGE = 'docker.io/busybox:latest'
NVIDIA_INSPECTION_IMAGE = 'nvidia/cuda:8.0-runtime'
NOFILE_LIMIT = 4096
CHECK_EXITED_CONTAINERS_INTERVAL = 1.0
OFFLINE_INSPECTION_INTERVAL = 10
CHECK_FOR_BATCHES_INTERVAL = 20
IMAGE_PRUNE_INTERVAL = 3600


class ImagePullResult:
    def __init__(self, image_url, auth, successful, debug_info, depending_batches):
        """
        Creates a new DockerImagePull object.

        :param image_url: The url of the image to pull
        :type image_url: str
        :param auth: The authentication data for this image. Can be None if no authentication is required, otherwise it
                     has to be a tuple (username, password).
        :type auth: None or Tuple[str, str]
        :param successful: A boolean that is True, if the pull was successful
        :type successful: bool
        :param debug_info: A list of strings describing the error if the pull failed. Otherwise None.
        :type debug_info: List[str] or None
        :param depending_batches: A list of batches that depend on the execution of this docker pull
        :type depending_batches: List[Dict]
        """
        self.image_url = image_url
        self.auth = auth
        self.successful = successful
        self.debug_info = debug_info
        self.depending_batches = depending_batches


def _pull_image(docker_client, image_url, auth, depending_batches):
    """
    Pulls the given docker image and returns a ImagePullResult object.

    :param docker_client: The docker client, which is used to pull the image
    :type docker_client: docker.DockerClient
    :param image_url: The image to pull
    :type image_url: str
    :param auth: A tuple containing (username, password) or None
    :type auth: Tuple[str, str] or None
    :param depending_batches: A list of batches, which depend on the given image
    :type depending_batches: List[Dict]

    :return: An ImagePullResult describing the pull
    :rtype: ImagePullResult
    """
    try:
        docker_client.images.pull(image_url, auth_config=auth)
    except Exception as e:
        debug_info = log_format_exception(e).split('\n')
        return ImagePullResult(image_url, auth, False, debug_info, depending_batches)

    return ImagePullResult(image_url, auth, True, None, depending_batches)


def fill_experiment_secret_keys(trustee_client, experiment):
    """
    Returns the given experiment with filled template keys and values.

    :param trustee_client: The trustee client to fetch the secret values to fill into the experiment
    :type trustee_client: TrusteeClient
    :param experiment: The experiment to complete.

    :return: Returns the given experiment with filled template keys and values.

    :raise TrusteeServiceError: If the trustee service is unavailable or the trustee service could not fulfill all
                                requested keys
    """
    experiment_secret_keys = get_experiment_secret_keys(experiment)
    response = trustee_client.collect(experiment_secret_keys)
    if response['state'] == 'failed':

        debug_info = response['debugInfo']

        if response.get('inspect'):
            response = trustee_client.inspect()
            if response['state'] == 'failed':
                debug_info = response['debug_info']
                raise TrusteeServiceError('Trustee service unavailable:{}{}'.format(os.linesep, debug_info))

        experiment_id = str(experiment['_id'])
        raise TrusteeServiceError(
            'Trustee service request failed for experiment "{}":{}{}'.format(experiment_id, os.linesep, debug_info)
        )

    experiment_secrets = response['secrets']
    return fill_experiment_secrets(experiment, experiment_secrets)


class ClientProxy:
    """
    A client proxy handles a cluster node and the docker client of this node.
    It takes over the following tasks:
    - It queries the db for new batches, that got scheduled to this node to start them
    - It queries the docker client for containers, which are finished and updates the db accordingly
    - It queries the db to remove cancelled containers
    - If an error occurred it tries to reinitialize the docker client

    A client proxy contains a "online-flag" implemented as threading.Event. Any thread inside this client proxy except
    the inspection loop should wait for this event to be set, before starting a new execution cycle.
    Only the inspect-thread is allowed to set/clear the "online-flag" after successful/failed inspection.

    On error the check_for_batches and check_exited_containers-threads can trigger an inspection.

    inspect:
      If this ClientProxy is offline regularly inspects the connection to the docker daemon. If the inspection failed,
      the "online-flag" is cleared and this node is set to offline.

      If this ClientProxy is online inspect the docker engine, by starting a docker container and examine the result
      of this execution. If the execution was successful, set the "online-flag" and make this node online, otherwise
      repeat in some interval.

    check-for-batches:
      Regularly queries the database for batches, which are scheduled to this node. All found batches are then started
      with the docker client, if online. This can be triggered manually by setting the check-for-batches-event.
      If this client proxy is changed to be offline this thread processes the current cycle until it has finished and
      then waits for the "online-flag" to be set.

    check-exited-containers:
      Regularly queries the containers from the docker client and the database, which are currently running on this
      node. Checks the containers, which are not running anymore and handles their execution result. This can be
      triggered by setting the check_exited_containers-flag.
      If this client proxy is changed to be offline this thread processes the current cycle until it has finished and
      then waits for the "online-flag" to be set.
    """
    NUM_WORKERS = 4

    def __init__(self, node_name, conf, mongo, trustee_client, scheduling_event):
        self._node_name = node_name
        self._mongo = mongo
        self._trustee_client = trustee_client

        self._scheduling_event = scheduling_event

        node_conf = conf.d['controller']['docker']['nodes'][node_name]
        self._image_prune_duration = conf.d['controller']['docker'].get('image_prune_duration')
        self._last_prune_timestamp = 0
        self._base_url = node_conf['base_url']
        self._tls = False
        if 'tls' in node_conf:
            self._tls = TLSConfig(**node_conf['tls'])

        self._environment = node_conf.get('environment')
        self._network = node_conf.get('network')
        self._gpu_blacklist = node_conf.get('hardware', {}).get('gpu_blacklist')  # type: List[GPUDevice]

        # create db entry for this node
        node = {
            'nodeName': node_name,
            'state': None,
            'history': [],
            'ram': None,
            'cpus': None,
            'gpus': None
        }

        bson_node_id = self._mongo.db['nodes'].insert_one(node).inserted_id
        self._node_id = str(bson_node_id)

        # init docker client
        self._client = None
        # used to prevent "Failed to init docker client" spam
        self._printed_failed_docker_client_init = False  # type: bool
        self._runtimes = None
        self._gpus = None  # type: List[GPUDevice] or None
        self._online = Event()  # type: Event

        self._inspection_event = Event()  # type: Event
        self._check_for_batches_event = Event()  # type: Event
        self._check_exited_containers_event = Event()  # type: Event

        if self._init_docker_client():
            self._remove_old_containers()
        else:
            self.do_inspect()
            self._set_offline(format_exc())

        Thread(target=self._inspection_loop).start()
        Thread(target=self._check_for_batches_loop).start()
        Thread(target=self._check_exited_containers_loop).start()

        # initialize Executor Pools
        self._pull_executor = concurrent.futures.ThreadPoolExecutor(max_workers=ClientProxy.NUM_WORKERS)
        self._run_executor = concurrent.futures.ThreadPoolExecutor(max_workers=ClientProxy.NUM_WORKERS)

    def _remove_old_containers(self):
        """
        Only execute this function at the start.
        This function removes all batch containers in state created.
        """
        try:
            for batch_id, container in self._batch_containers('created').items():
                container.stop()
                container.remove()

                self._run_batch_container_failure(
                    batch_id,
                    'agency was restarted during processing of this batch and the batch could not be started correctly.',
                    'created'
                )
        except Exception as e:
            self._log('Error while removing old containers', e)

    def get_gpus(self):
        return self._gpus

    def is_online(self):
        return self._online.is_set()

    def _set_online(self, ram, cpus):
        print('Node online:', self._node_name)

        gpus = list(map(lambda gpu_device: gpu_device.to_dict(), self._gpus)) if (self._gpus is not None) else None

        bson_node_id = ObjectId(self._node_id)
        self._mongo.db['nodes'].update_one(
            {'_id': bson_node_id},
            {
                '$set': {
                    'state': 'online',
                    'ram': ram,
                    'cpus': cpus,
                    'gpus': gpus
                },
                '$push': {
                    'history': {
                        'state': 'online',
                        'time': time.time(),
                        'debugInfo': None
                    }
                }
            }
        )

        self._online.set()  # start _check_batch_containers and _check_exited_containers

    def _set_offline(self, debug_info):
        self._log('Node offline: {}'.format(self._node_name))

        self._online.clear()

        timestamp = time.time()
        bson_node_id = ObjectId(self._node_id)
        self._mongo.db['nodes'].update_one(
            {'_id': bson_node_id},
            {
                '$set': {'state': 'offline'},
                '$push': {
                    'history': {
                        'state': 'offline',
                        'time': timestamp,
                        'debugInfo': debug_info
                    }
                }
            }
        )

        # change state of assigned batches
        cursor = self._mongo.db['batches'].find(
            {
                'node': self._node_name,
                'state': {'$in': ['scheduled', 'processing']}
            },
            {'_id': 1, 'state': 1}
        )

        for batch in cursor:
            bson_id = batch['_id']
            batch_id = str(bson_id)
            debug_info = 'Node offline: {}'.format(self._node_name)
            batch_failure(self._mongo, batch_id, debug_info, None, batch['state'])

    def _info(self):
        info = self._client.info()
        ram = info['MemTotal'] // (1024 * 1024)
        cpus = info['NCPU']
        runtimes = info['Runtimes']
        return ram, cpus, runtimes

    @staticmethod
    def _log(message, e=None):
        """
        Logs a message by printing it to make it visible to journalctl.

        :param message: The message to print
        :type message: str
        :param e: The exception to print
        :type e: Exception
        """
        print(message, file=sys.stderr)
        if e is not None:
            print(log_format_exception(e), file=sys.stderr)

    def _batch_containers(self, status):
        """
        Returns a dictionary that maps container names to the corresponding container.
        If this client proxy is offline, the result will always be an empty dictionary.

        :param status: A status string. Containers, which have a different state are not contained in the result of this
                       function
        :type status: str or None
        :return: A dictionary mapping container names to docker containers
        :rtype: Dict[str, Container]

        :raise DockerException: If the docker engine returns an error
        """
        batch_containers = {}  # type: Dict[str, Container]

        if not self.is_online():
            return batch_containers

        filters = {'status': status}
        if status is None:
            filters = None

        try:
            containers = self._client.containers.list(
                all=True,
                limit=-1,
                filters=filters,
                ignore_removed=True  # to ignore failures due to parallel removed containers
            )  # type: List[Container]
        except (ConnectionError, ReadTimeout) as e:
            raise DockerException(
                'Could not list current containers. Failed with the following message:\n{}'
                .format(log_format_exception(e))
            )

        for c in containers:
            try:
                ObjectId(c.name)
                batch_containers[c.name] = c
            except (bson.errors.InvalidId, TypeError):
                pass

        return batch_containers

    def _remove_cancelled_containers(self):
        """
        Stops all docker containers, whose batches got cancelled.

        :raise DockerException: If the docker server returns an error
        """
        running_containers = self._batch_containers('running')

        cursor = self._mongo.db['batches'].find(
            {
                '_id': {'$in': [ObjectId(_id) for _id in running_containers]},
                'state': 'cancelled'
            },
            {'_id': 1}
        )
        resources_freed = False
        for batch in cursor:
            batch_id = str(batch['_id'])

            c = running_containers[batch_id]
            c.remove(force=True)
            resources_freed = True

        return resources_freed

    def _can_execute_container(self):
        """
        Tries to execute a docker container using the docker client.

        :return: A tuple (successful, info/error)
                 successful: True, if the docker container could be executed, otherwise False
                 info/error:
                   - In case of success a tuple containing (ram, cpus, runtimes),
                   - In case of failure the error message as string.
        :rtype: tuple[bool, tuple or str]
        """
        command = ['echo', 'test']

        inspection_image = NVIDIA_INSPECTION_IMAGE if self._has_nvidia_gpus() else INSPECTION_IMAGE

        try:
            self._client.containers.run(
                inspection_image,
                command,
                user='1000:1000',
                remove=True,
                environment=self._environment,
                network=self._network
            )
            info = self._info()
        except (DockerException, ConnectionError) as e:
            return False, log_format_exception(e)
        except Exception as e:
            self._log('Failed to inspect docker client for "{}":'.format(self._node_name), e)
            return False, log_format_exception(e)

        return True, info

    def _inspect_on_error(self):
        """
        Inspects the current docker client and sets this node to offline, if the inspection fails.
        """
        success, state = self._can_execute_container()

        if not success:
            self._set_offline(state)

    def _init_docker_client(self):
        """
        Tries to reinitialize the docker client. If successful, this node is online after this function execution.
        After initialization tries to detect nvidia gpus.

        :return: True, if the initialization succeeded, otherwise False
        :rtype: bool
        """
        init_succeeded = False
        try:
            self._client = docker.DockerClient(base_url=self._base_url, tls=self._tls, version='auto')

            successful, state = self._can_execute_container()
            if successful:
                ram, cpus, runtimes = state
                self._runtimes = runtimes
                self._init_gpus()  # try to detect gpus
                if not self.is_online():
                    self._set_online(ram, cpus)
                    init_succeeded = True
                    self._printed_failed_docker_client_init = False
            else:
                if not self._printed_failed_docker_client_init:
                    self._log('Failed to init docker client for "{}":\n{}'.format(self._node_name, state))
                    self._printed_failed_docker_client_init = True
        except (DockerException, ConnectionError) as e:
            if not self._printed_failed_docker_client_init:
                self._log('Failed to init docker client "{}" with exception:'.format(self._node_name), e)
                self._printed_failed_docker_client_init = True
        return init_succeeded

    def _init_gpus(self):
        """
        Tries to detect gpus for this ClientProxy by executing nvidia-smi in a docker container.
        If found sets self._gpus to a list of detected gpu devices. Does consider self._gpu_blacklist to ignore certain
        gpus.
        """
        try:
            gpu_devices = detect_nvidia_docker_gpus(self._client, self._runtimes)
            if self._gpu_blacklist:
                self._gpus = list(filter(
                    lambda gpu_device: gpu_device.device_id not in self._gpu_blacklist,
                    gpu_devices
                ))
            else:
                self._gpus = gpu_devices
        except DockerException:
            pass  # If this fails, no gpus are assumed
        except ConnectionError as e:
            self._log('GPU Detection failed:', e)
            self.do_inspect()

    def _inspection_loop(self):
        """
        Regularly inspects the connection the docker daemon by running a docker container. If an error was found, clears
        the "online-flag" and puts in a error token in the inspection queue.

        Waits for errors inside the inspection-queue. If an error token was found inside the inspection-queue, handles
        this error. Otherwise performs a routine check of the docker client, after a given timeout.
        If this client proxy is offline, tries to restart it.
        """
        while True:
            try:
                if self.is_online():
                    self._inspection_event.wait()
                    self._inspection_event.clear()
                    self._inspect_on_error()
                else:
                    self._inspection_event.wait(timeout=OFFLINE_INSPECTION_INTERVAL)
                    self._inspection_event.clear()
                    self._init_docker_client()  # tries to reinitialize the docker client
            except Exception as e:
                self._log('Error while inspecting:', e)

    def _check_exited_containers(self):
        """
        Checks all containers which have "recently exited". A container is considered "recently exited", if the docker
        container is in state 'exited' and the corresponding batch is in state 'processing'.

        After this function execution all "recently exited" containers of this docker client should have batches, which
        are in one of the following states:
        - succeeded: If the batch execution was successful
        - failed: If the batch execution has failed

        :return: True if a exited container was found, otherwise False
        :rtype: bool

        :raise DockerException: If the connection to the docker daemon is interrupted
        """
        exited_containers = self._batch_containers('exited')  # type: Dict[str, Container]

        batch_cursor = self._mongo.db['batches'].find(
            {'_id': {'$in': [ObjectId(_id) for _id in exited_containers]}},
            {'state': 1, STDOUT_FILE_KEY: 1, STDERR_FILE_KEY: 1}
        )
        resources_freed = False
        for batch in batch_cursor:
            batch_id = str(batch['_id'])

            exited_container = exited_containers[batch_id]

            self._check_exited_container(exited_container, batch)

            exited_container.remove()

            resources_freed = True

        return resources_freed

    def _check_exited_containers_loop(self):
        """
        Regularly checks exited containers. Waits for this client proxy to come online, before starting a new cycle.
        Also removes containers, whose batches got cancelled.
        """
        while True:
            self._online.wait()  # wait for this node to come online

            self._check_exited_containers_event.wait(timeout=CHECK_EXITED_CONTAINERS_INTERVAL)
            self._check_exited_containers_event.clear()

            try:
                resources_freed = self._check_exited_containers()
                resources_freed = self._remove_cancelled_containers() or resources_freed

                if resources_freed:
                    self._scheduling_event.set()
            except (DockerException, ConnectionError) as e:
                self._log('Error while checking exited containers:', e)
                self.do_inspect()
            except Exception as e:
                self._log('Error while checking exited containers:', e)

    def _check_exited_container(self, container, batch):
        """
        Inspects the logs of the given exited container and updates the database accordingly.

        Also handles stdout/stderr:
        - If the batch failed, stdout/stderr are copied into the mongo GridFS
        - If the experiment explicitly specified stdout/stderr they are also copied into the mongo GridFS

        :param container: The container to inspect
        :type container: Container
        :param batch: The batch to update according to the result of the container execution.
        :type batch: dict
        """
        bson_batch_id = batch['_id']
        batch_id = str(bson_batch_id)

        gridfs_stdout_filename = get_gridfs_filename(batch_id, 'stdout')
        gridfs_stderr_filename = get_gridfs_filename(batch_id, 'stderr')

        try:
            stdout_logs = container.logs(stderr=False).decode('utf-8')
            stderr_logs = container.logs(stdout=False).decode('utf-8')

            docker_stats = container.stats(stream=False)
        except Exception as e:
            self._log('Failed to get container logs:', e)
            debug_info = 'Could not get logs or stats of container: {}'.format(log_format_exception(e))
            batch_failure(self._mongo, batch_id, debug_info, None, batch['state'])
            return

        data = None
        try:
            data = json.loads(stdout_logs)
        except json.JSONDecodeError as e:
            debug_info = 'stdout of agent is not a valid json object: {}\nstdout of agent was:\n{}'\
                         .format(log_format_exception(e), stdout_logs)
            batch_failure(self._mongo, batch_id, debug_info, data, batch['state'], docker_stats=docker_stats)
            self._log('Failed to load json from restricted red agent:', e)
            return

        container_stdout_path = None
        if STDOUT_FILE_KEY in batch:
            container_stdout_path = CONTAINER_OUTPUT_DIR.joinpath(batch.get(STDOUT_FILE_KEY))

        container_stderr_path = None
        if STDERR_FILE_KEY in batch:
            container_stderr_path = CONTAINER_OUTPUT_DIR.joinpath(batch.get(STDERR_FILE_KEY))

        def write_stdout_stderr_to_gridfs(include_stdout=True, include_stderr=True):
            """
            Helper function to write the stdout/stderr files of the docker container into mongo gridfs.

            :return: A list of strings describing the errors that occurred during transferring the files.
            :rtype: list[str]
            """
            errors = []
            if include_stdout and container_stdout_path is not None:
                try:
                    archive_stdout = retrieve_file_archive(container, container_stdout_path)
                    with get_first_tarfile_member(archive_stdout) as file_stdout:
                        self._mongo.write_file_from_file(gridfs_stdout_filename, file_stdout)
                except (DockerException, ValueError, StreamError) as ex:
                    errors.append(
                        'Failed to create stdout for batch {}. Failed with the following message:\n{}'
                        .format(batch_id, log_format_exception(ex))
                    )

            if include_stderr and container_stderr_path is not None:
                try:
                    archive_stderr = retrieve_file_archive(container, container_stderr_path)
                    with get_first_tarfile_member(archive_stderr) as file_stderr:
                        self._mongo.write_file_from_file(gridfs_stderr_filename, file_stderr)
                except (DockerException, ValueError, StreamError) as ex:
                    errors.append(
                        'Failed to create stderr for batch {}. Failed with the following message:\n{}'
                        .format(batch_id, log_format_exception(ex))
                    )

            return errors

        try:
            jsonschema.validate(data, agent_result_schema)
        except jsonschema.ValidationError as e:
            write_stdout_stderr_to_gridfs()
            debug_info = 'CC-Agent data sent by callback does not comply with jsonschema:\n{}'\
                         .format(log_format_exception(e))
            batch_failure(self._mongo, batch_id, debug_info, data, batch['state'], docker_stats=docker_stats)
            self._log('Failed to validate restricted_red agent output:', e)
            return

        if data['state'] == 'failed':
            write_stdout_stderr_to_gridfs()
            debug_info = 'Batch failed.\nContainer stderr:\n{}\ndebug info:\n{}'.format(stderr_logs, data['debugInfo'])
            batch_failure(self._mongo, batch_id, debug_info, data, batch['state'], docker_stats=docker_stats)
            return

        batch = self._mongo.db['batches'].find_one(
            {'_id': bson_batch_id},
            {'attempts': 1, 'node': 1, 'state': 1, USER_SPECIFIED_STDOUT_KEY: 1, USER_SPECIFIED_STDERR_KEY: 1}
        )
        if batch['state'] != 'processing':
            write_stdout_stderr_to_gridfs()
            debug_info = 'Batch failed.\nExited container, but not in state processing.'
            batch_failure(self._mongo, batch_id, debug_info, data, batch['state'], docker_stats=docker_stats)
            return

        # from here it is expected that the batch was successful
        debug_info = write_stdout_stderr_to_gridfs(
            include_stdout=batch[USER_SPECIFIED_STDOUT_KEY],
            include_stderr=batch[USER_SPECIFIED_STDERR_KEY]
        )

        self._mongo.db['batches'].update_one(
            {
                '_id': bson_batch_id,
                'state': 'processing'
            },
            {
                '$set': {
                    'state': 'succeeded'
                },
                '$push': {
                    'history': {
                        'state': 'succeeded',
                        'time': time.time(),
                        'debugInfo': debug_info or None,
                        'node': batch['node'],
                        'ccagent': data,
                        'dockerStats': docker_stats
                    }
                }
            }
        )

    def do_check_for_batches(self):
        """
        Triggers a check-for-batches cycle.
        """
        self._check_for_batches_event.set()

    def do_check_exited_containers(self):
        """
        Triggers a check-exited-containers cycle.
        """
        self._check_exited_containers_event.set()

    def do_inspect(self):
        """
        Triggers an inspection cycle.
        """
        self._inspection_event.set()

    def _check_for_batches_loop(self):
        """
        Regularly calls _check_for_batches. Does wait before executing a new cycle, if this client proxy is offline.
        Also prunes unused images.
        """
        while True:
            self._online.wait()

            self._check_for_batches_event.wait(timeout=CHECK_FOR_BATCHES_INTERVAL)
            self._check_for_batches_event.clear()

            try:
                self._check_for_batches()
            except TrusteeServiceError as e:
                self.do_inspect()
                self._log('TrusteeService unavailable while checking for batches:', e)
                continue
            except Exception as e:
                self._log('Error while checking for batches:', e)

            try:
                self._prune_docker_images()
            except Exception as e:
                self._log('Error while removing old docker images:', e)

    def _get_images_with_last_registration_time(self):
        """
        Returns a dict with images as keys and  last_registration_timestamps as values.
        The images are gathered from all experiments executed in this agency. The resulting list only contains images,
        which are present on the host.
        The last_registration_timestamp is the latest timestamp when an experiment was registered that uses this image.

        :return: A dict {images: last_execution_timestamps}
                 key: the docker image as Image object
                 value: last_execution_timestamp is a unix timestamp defining the last execution of the given image
        :rtype: Dict[Image, float]
        """
        images_with_execution_time = {}

        for image_url in self._mongo.db.experiments.distinct('container.settings.image.url'):
            try:
                image = self._client.images.get(image_url)
            except docker.errors.ImageNotFound:
                # if image does not exist on this host, proceed
                continue
            except ConnectionError as e:
                self.do_inspect()
                self._log('Failed to get image "{}" with registration time:'.format(image_url), e)
                continue

            latest_experiment = self._mongo.db.experiments.find_one(
                {'container.settings.image.url': image_url},
                sort=[('registrationTime', pymongo.DESCENDING)]
            )

            registration_time = latest_experiment['registrationTime']

            # update dict, except the image is already present with later timestamp
            if images_with_execution_time.get(image, 0) < registration_time:
                images_with_execution_time[image] = registration_time

        return images_with_execution_time

    def _prune_docker_images(self):
        """
        Removes all docker images, that are created longer ago than self._image_prune_duration.
        """
        if self._image_prune_duration is None:
            return

        t = time.time()
        # check if it is time to prune images
        if self._last_prune_timestamp + IMAGE_PRUNE_INTERVAL > t:
            return

        self._last_prune_timestamp = t

        used_images = self._get_images_with_last_registration_time()

        until_filter = time.time() - self._image_prune_duration

        for image, last_registration_timestamp in used_images.items():
            if last_registration_timestamp < until_filter:
                try:
                    self._client.images.remove(image.id)
                except APIError:
                    continue  # if image is used by other images
                except ConnectionError as e:
                    self.do_inspect()
                    self._log('Failed to remove image:', e)
                    break
                print('removed image {}'.format(image_to_str(image)))

    def _check_for_batches(self):
        """
        Queries the database to find batches, which are in state 'scheduled' and are scheduled to the node of this
        ClientProxy.
        First all docker images are pulled, which are used to process these batches. Afterwards the batch processing is
        run. The state in the database for these batches is then updated to 'processing'.

        :raise TrusteeServiceError: If the trustee service is unavailable or the trustee service could not fulfill all
        requested keys
        :raise ImageAuthenticationError: If the image authentication information is invalid.
        """

        # query for batches, that are in state 'scheduled' and are scheduled to this node
        query = {
            'state': 'scheduled',
            'node': self._node_name
        }

        # list containing batches that are scheduled to this node and save them together with their experiment
        batches_with_experiments = []  # type: List[Tuple[Dict, Dict]]

        # dictionary, that maps docker image authentications to batches, which need this docker image
        image_to_batches = {}  # type: Dict[Tuple, List[Dict]]

        for batch in self._mongo.db['batches'].find(query):
            experiment = self._get_experiment_with_secrets(batch['experimentId'])
            batches_with_experiments.append((batch, experiment))

            image_authentication = ClientProxy._get_image_authentication(experiment)
            if image_authentication not in image_to_batches:
                image_to_batches[image_authentication] = []
            image_to_batches[image_authentication].append(batch)

        # pull images
        pull_futures = []
        for image_authentication, depending_batches in image_to_batches.items():
            image_url, auth = image_authentication
            future = self._pull_executor.submit(_pull_image, self._client, image_url, auth, depending_batches)
            pull_futures.append(future)

        for pull_future in pull_futures:
            image_pull_result = pull_future.result()  # type: ImagePullResult

            # If pulling failed, the batches, which needed this image fail and are removed from the
            # batches_with_experiments list
            if not image_pull_result.successful:
                for batch in image_pull_result.depending_batches:
                    # fail the batch
                    batch_id = str(batch['_id'])
                    self._pull_image_failure(image_pull_result.debug_info, batch_id, batch['state'])

                    # remove batches that are failed
                    batches_with_experiments = list(filter(
                        lambda batch_with_experiment: str(batch_with_experiment[0]['_id']) != batch_id,
                        batches_with_experiments
                    ))

        # run every batch, that has not failed
        run_futures = []  # type: List[concurrent.futures.Future]
        for batch, experiment in batches_with_experiments:
            future = self._run_executor.submit(
                ClientProxy._run_batch_container_and_handle_exceptions,
                self,
                batch,
                experiment
            )
            run_futures.append(future)

        # wait for all batches to run
        concurrent.futures.wait(run_futures, return_when=concurrent.futures.ALL_COMPLETED)

    def _get_experiment_with_secrets(self, experiment_id):
        """
        Returns the experiment of the given experiment_id with filled secrets.

        :param experiment_id: The experiment id to resolve.
        :type experiment_id: ObjectId
        :return: The experiment as dictionary with filled template values.
        :raise TrusteeServiceError: If the trustee service is unavailable or the trustee service could not fulfill all
        requested keys
        """
        experiment = self._mongo.db['experiments'].find_one(
            {'_id': ObjectId(experiment_id)},
        )

        experiment = fill_experiment_secret_keys(self._trustee_client, experiment)

        return experiment

    @staticmethod
    def _get_image_url(experiment):
        """
        Gets the url of the docker image for the given experiment

        :param experiment: The experiment whose docker image url is returned
        :type experiment: Dict
        :return: The url of the docker image for the given experiment
        """
        return experiment['container']['settings']['image']['url']

    @staticmethod
    def _get_image_authentication(experiment):
        """
        Reads the docker authentication information from the given experiment and returns it as tuple. The first element
        is always the image_url for the docker image. The second element is a tuple containing the username and password
        for authentication at the docker registry. If no username and password is given, the second return value is
        None.

        :param experiment: An experiment with filled secret keys, whose image authentication information should be
                           returned
        :type experiment: Dict

        :return: A tuple containing the image_url as first element. The second element can be None or a Tuple containing
                 (username, password) for authentication at the docker registry.
        :rtype: Tuple[str, None] or Tuple[str, Tuple[str, str]]

        :raise ImageAuthenticationError: If the given image authentication information is not complete
                                         (username and password are mandatory)
        """

        image_url = ClientProxy._get_image_url(experiment)

        image_auth = experiment['container']['settings']['image'].get('auth')
        if image_auth:
            for mandatory_key in ('username', 'password'):
                if mandatory_key not in image_auth:
                    raise ImageAuthenticationError(
                        'Image authentication is given, but "{}" is missing'.format(mandatory_key)
                    )

            image_auth = (image_auth['username'], image_auth['password'])
        else:
            image_auth = None

        return image_url, image_auth

    def _run_batch_container_and_handle_exceptions(self, batch, experiment):
        """
        Runs the given batch by calling _run_batch_container(), but handles exceptions, by calling
        _run_batch_container_failure().

        :param batch: The batch to run
        :type batch: dict
        :param experiment: The experiment of this batch
        :type experiment: dict
        """
        try:
            self._run_batch_container(batch, experiment)
        except Exception as e:
            self._log('Error while running batch container:', e)
            batch_id = str(batch['_id'])
            self._run_batch_container_failure(batch_id, log_format_exception(e), batch['state'])

    def _run_batch_container(self, batch, experiment):
        """
        Creates a docker container and runs the given batch, with settings described in the given batch and experiment.
        Sets the state of the given batch to 'processing'.

        :param batch: The batch to run
        :type batch: dict
        :param experiment: The experiment of this batch
        :type experiment: dict
        """
        batch_id = str(batch['_id'])

        update_result = self._mongo.db['batches'].update_one(
            {
                '_id': ObjectId(batch_id),
                'state': 'scheduled'
            },
            {
                '$set': {
                    'state': 'processing',
                },
                '$push': {
                    'history': {
                        'state': 'processing',
                        'time': time.time(),
                        'debugInfo': None,
                        'node': self._node_name,
                        'ccagent': None,
                        'dockerStats': None
                    }
                }
            }
        )

        # only run the docker container, if the batch was successfully updated
        if update_result.modified_count == 1:
            self._run_container(batch, experiment)

    def _run_container(self, batch, experiment):
        """
        Runs a docker container for the given batch. Uses the following procedure:

        - Collects all arguments for the docker container execution
        - Removes old containers with the same name
        - Creates the docker container with the collected arguments
        - Creates an archive containing the restricted_red_agent and the restricted_red_file of this batch and copies
          this archive into the container
        - Starts the container

        :param batch: The batch to run inside the container
        :type batch: Dict[str, Any]
        :param experiment: The experiment of the given batch
        :type experiment: Dict[str, Any]

        :raise DockerException: If the connection to the docker daemon is broken
        """
        batch_id = str(batch['_id'])

        environment = {}
        if self._environment:
            environment = self._environment.copy()

        gpus = batch['usedGPUs']

        # set mount variables
        devices = []
        capabilities = []
        security_opt = []
        if batch['mount']:
            devices.append('/dev/fuse')
            capabilities.append('SYS_ADMIN')
            security_opt.append('apparmor:unconfined')

        # set image
        image = experiment['container']['settings']['image']['url']

        command = [
            'python3',
            CONTAINER_AGENT_PATH.as_posix(),
            '--outputs',
            CONTAINER_RESTRICTED_RED_FILE_PATH.as_posix()
        ]

        ram = experiment['container']['settings']['ram']
        mem_limit = '{}m'.format(ram)

        # set ulimits
        ulimits = [
            docker.types.Ulimit(
                name='nofile',
                soft=NOFILE_LIMIT,
                hard=NOFILE_LIMIT
            )
        ]

        # remove container if it exists from earlier attempt
        existing_container = self._batch_containers(None).get(batch_id)
        if existing_container is not None:
            existing_container.remove(force=True)

        # the user argument is not set to use the user specified by the docker image
        container = create_container_with_gpus(
            client=self._client,
            image=image,
            command=command,
            available_runtimes=self._runtimes,
            name=batch_id,
            working_dir=CONTAINER_OUTPUT_DIR.as_posix(),
            detach=True,
            mem_limit=mem_limit,
            memswap_limit=mem_limit,
            gpus=gpus,
            environment=environment,
            network=self._network,
            devices=devices,
            cap_add=capabilities,
            security_opt=security_opt,
            ulimits=ulimits
        )  # type: Container

        # copy restricted_red agent and restricted_red file to container
        with self._create_batch_archive(batch) as tar_archive:
            container.put_archive('/', tar_archive)

        container.start()

    def _create_restricted_red_batch(self, batch):
        """
        Creates a dictionary containing the data for a restricted_red batch.

        :param batch: The batch description
        :type batch: dict
        :return: A dictionary containing a restricted_red batch
        :rtype: dict
        :raise TrusteeServiceError: If the trustee service is unavailable or unable to collect the requested secret keys
        :raise ValueError: If there was more than one restricted_red batch after red_to_restricted_red
        """
        batch_id = str(batch['_id'])
        batch_secret_keys = get_batch_secret_keys(batch)
        response = self._trustee_client.collect(batch_secret_keys)

        if response['state'] == 'failed':
            debug_info = 'Trustee service failed:\n{}'.format(response['debug_info'])
            disable_retry = response.get('disable_retry')
            batch_failure(
                self._mongo,
                batch_id,
                debug_info,
                None,
                batch['state'],
                disable_retry_if_failed=disable_retry
            )
            raise TrusteeServiceError(debug_info)

        batch_secrets = response['secrets']
        batch = fill_batch_secrets(batch, batch_secrets)

        experiment_id = batch['experimentId']

        experiment = self._mongo.db['experiments'].find_one(
            {'_id': ObjectId(experiment_id)}
        )

        red_data = {
            'redVersion': experiment['redVersion'],
            'cli': experiment['cli'],
            'inputs': batch['inputs'],
            'outputs': batch['outputs']
        }

        restricted_red_batches = convert_red_to_restricted_red(red_data)

        if len(restricted_red_batches) != 1:
            raise ValueError('Got {} batches, but only one was asserted.'.format(len(restricted_red_batches)))

        restricted_red_batch = restricted_red_batches[0]

        # update stdout/stderr metadata
        self._mongo.db['batches'].update_one(
            {
                '_id': ObjectId(batch_id)
            },
            {
                '$set': {
                    USER_SPECIFIED_STDOUT_KEY: restricted_red_batch.stdout_specified_by_user(),
                    USER_SPECIFIED_STDERR_KEY: restricted_red_batch.stderr_specified_by_user(),
                    STDOUT_FILE_KEY: restricted_red_batch.data['cli']['stdout'],
                    STDERR_FILE_KEY: restricted_red_batch.data['cli']['stderr']
                },
            }
        )

        return restricted_red_batch.data

    def _create_batch_archive(self, batch):
        """
        Creates a tar archive to put into the docker container for the restricted_red agent execution.
        The restricted_red data is extracted from the given batch.

        :param batch: The data to put into the restricted_red file of the returned archive
        :type batch: dict
        :return: A tar archive containing the restricted_red agent and the given restricted_red batch
        :rtype: io.BytesIO or bytes
        """
        restricted_red_data = self._create_restricted_red_batch(batch)

        return create_batch_archive(restricted_red_data)

    def _run_batch_container_failure(self, batch_id, debug_info, current_state):
        try:
            batch_failure(self._mongo, batch_id, debug_info, None, current_state)
        except Exception as e:
            self._log('Error while handling batch failure:', e)

    def _pull_image_failure(self, debug_info, batch_id, current_state):
        self._run_batch_container_failure(batch_id, debug_info, current_state)

    def _has_nvidia_gpus(self):
        """
        Returns whether nvidia gpus are configured for this ClientProxy.

        :return: True, if gpus are configured, otherwise False
        :rtype: bool
        """
        if self._gpus is None:
            return False

        return any(map(lambda gpu: gpu.vendor == NVIDIA_GPU_VENDOR, self._gpus))


class TrusteeServiceError(Exception):
    pass


class ImageAuthenticationError(Exception):
    pass
