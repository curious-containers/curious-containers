from typing import List


NVIDIA_GPU_VENDOR = 'nvidia'


class InsufficientGPUError(Exception):
    pass


class GPUDevice:
    """
    Represents a GPU Device
    """
    def __init__(self, device_id, vram, vendor):
        """
        :param vram: The vram of this GPU in mega bytes
        """
        self.device_id = device_id
        self.vram = vram
        self.vendor = vendor

    def __repr__(self):
        return 'GPUDevice(device_id="{}" vram="{}" vendor="{}")'.format(self.device_id, self.vram, self.vendor)

    def to_dict(self):
        return {
            'id': self.device_id,
            'vram': self.vram,
            'vendor': self.vendor
        }


class GPURequirement:
    """
    Represents a GPU Requirement
    """
    def __init__(self, min_vram=None, vendor=None):
        """
        :param min_vram: The minimal vram needed for this device in mega bytes
                         If None, no vram limitation is used.
        :type min_vram: int
        :param vendor: The vendor of this gpu
        :type vendor: str
        """
        self.min_vram = min_vram
        self.vendor = vendor

    def is_sufficient(self, device):
        """
        Returns whether the device is sufficient for this requirement.

        :param device: A GPUDevice instance.
        :type device: GPUDevice
        :return: True if the requirement is fulfilled otherwise False
        """

        sufficient = True
        if (self.min_vram is not None) and (device.vram < self.min_vram):
            sufficient = False

        if (self.vendor is not None) and (self.vendor != device.vendor):
            sufficient = False

        return sufficient

    def __repr__(self):
        return 'GPURequirement(min_vram="{}" vendor="{}")'.format(self.min_vram, self.vendor)


def get_cuda_devices():
    """
    Imports pycuda at runtime and reads GPU information.

    :return: A list of available cuda GPUs.
    """

    devices = []

    try:
        import pycuda.autoinit
        import pycuda.driver as cuda

        for device_id in range(cuda.Device.count()):
            vram = cuda.Device(device_id).total_memory()

            devices.append(GPUDevice(device_id, vram, NVIDIA_GPU_VENDOR))
    except ImportError:
        raise InsufficientGPUError('No Nvidia-GPUs could be found, because "pycuda" could not be imported.')

    return devices


def no_devices():
    """
    Returns an empty list

    :return: []
    """
    return []


""" maps docker engines to functions returning gpu devices """
DEVICE_INFORMATION_MAP = {
    'docker': no_devices,
    'nvidia-docker': get_cuda_devices
}


def get_devices(engine):
    """
    Returns GPU device information.

    :param engine: The used docker engine.
    :type engine: str
    :return: A list of available devices
    """

    if engine in DEVICE_INFORMATION_MAP:
        return DEVICE_INFORMATION_MAP[engine]()
    else:
        return []


def search_device(requirement, devices):
    """
    Returns a sufficient device or None

    :param requirement: The requirement to fulfill
    :type requirement: GPURequirement
    :param devices: The list of available devices
    :type devices: List[GPUDevice]
    :return: A device from the list
    """

    for device in devices:
        if requirement.is_sufficient(device):
            return device
    return None


def match_gpus(available_devices, requirements):
    """
    Determines sufficient GPUs for the given requirements and returns a list of GPUDevices.
    If there aren't sufficient GPUs a InsufficientGPUError is thrown.

    :param available_devices: A list of GPUDevices
    :type available_devices: List[GPUDevice]
    :param requirements: A list of GPURequirements
    :type requirements: List[GPURequirement]

    :return: A list of sufficient devices
    :rtype: list[GPUDevice]

    :raise InsufficientGPUError: If no devices could be found or insufficient GPUs were found
    """

    if not requirements:
        return []

    if not available_devices:
        raise InsufficientGPUError("No GPU devices available, but {} devices required.".format(len(requirements)))

    available_devices = available_devices.copy()

    used_devices = []

    for req in requirements:
        dev = search_device(req, available_devices)
        if dev:
            used_devices.append(dev)
            available_devices.remove(dev)
        else:
            raise InsufficientGPUError("Not all GPU requirements could be fulfilled.")

    return used_devices


def get_gpu_requirements(gpus_reqs):
    """
    Extracts the GPU from a dictionary requirements as list of GPURequirements.

    :param gpus_reqs: A dictionary {'count': <count>} or a list [{min_vram: <min_vram>}, {min_vram: <min_vram>}, ...]
    :type gpus_reqs: dict or None
    :return: A list of GPURequirements
    :rtype: List[GPURequirement]
    """
    requirements = []

    if gpus_reqs:
        vendor = gpus_reqs['vendor']
        count = gpus_reqs.get('count')
        devices = gpus_reqs.get('devices')
        if devices:
            for device in devices:
                requirements.append(GPURequirement(min_vram=device['vramMin'], vendor=vendor))
        elif count:
            for i in range(count):
                requirements.append(GPURequirement(vendor=vendor))

    return requirements


def set_nvidia_environment_variables(environment, gpu_ids):
    """
    Updates a dictionary containing environment variables to setup Nvidia-GPUs.

    :param environment: The environment variables to update
    :param gpu_ids: A list of GPU ids
    """

    if gpu_ids:
        nvidia_visible_devices = ','.join(map(str, gpu_ids))
        environment["NVIDIA_VISIBLE_DEVICES"] = nvidia_visible_devices
