import sys
from argparse import ArgumentParser
from pprint import pprint

import requests

from cc_core.commons.exceptions import print_exception, exception_format, InvalidExecutionEngineArgumentException
from cc_core.commons.files import load_and_read, dump_print
from cc_core.commons.engines import engine_validation
from cc_core.commons.red_secrets import get_secret_values

from cc_faice.execution_engine.red_execution_engine import run as run_faice_execution_engine, OutputMode
from cc_faice.commons.templates import complete_red_variables
from red_val.red_validation import red_validation

DESCRIPTION = 'Execute experiment according to execution engine defined in REDFILE.'


# noinspection PyPep8Naming
def IntegerSet(s):
    return set(int(i) for i in s.split(','))


def attach_args(parser):
    parser.add_argument(
        'red_file', action='store', type=str, metavar='REDFILE',
        help='REDFILE (json or yaml) containing an experiment description as local PATH or http URL.'
    )
    parser.add_argument(
        '-d', '--debug', action='store_true',
        help='Write debug info, including detailed exceptions, to stdout.'
    )
    parser.add_argument(
        '--format', action='store', type=str, metavar='FORMAT', choices=['json', 'yaml', 'yml'], default='yaml',
        help='Specify FORMAT for generated data as one of [json, yaml, yml]. Default is yaml.'
    )
    parser.add_argument(
        '--non-interactive', action='store_true',
        help='Do not ask for RED variables interactively.'
    )
    parser.add_argument(
        '--keyring-service', action='store', type=str, metavar='KEYRING_SERVICE', default='red',
        help='Keyring service to resolve template values, default is "red".'
    )
    parser.add_argument(
        '--preserve-environment', action='append', type=str, metavar='ENVVAR',
        help='Only valid for ccfaice. Preserve specific environment variables when running container. '
             'May be provided multiple times.'
    )
    parser.add_argument(
        '--disable-pull', action='store_true',
        help='Only valid for ccfaice. Do not try to pull Docker images.'
    )
    parser.add_argument(
        '--leave-container', action='store_true',
        help='Only valid for ccfaice. Do not delete Docker container used by jobs after they exit.'
    )
    parser.add_argument(
        '--insecure', action='store_true',
        help='Only valid for ccfaice. Enables SYS_ADMIN capabilities in the docker container, to enable FUSE mounts.'
    )
    parser.add_argument(
        '--gpu-ids', type=IntegerSet, metavar='GPU_IDS',
        help='Only valid for ccfaice. Use the GPUs with the given GPU_IDS for this execution. GPU_IDS should be a '
             'comma separated list of integers, like --gpu-ids "1,2,3".'
    )
    parser.add_argument(
        '--disable-retry', action='store_true',
        help='Only valid for ccagency. If present the execution engine should stop the experiment execution, if the '
             'experiment failed. Otherwise cc-agency is allowed to retry the experiment.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()

    result = run(**args.__dict__)

    if args.debug:
        dump_print(result, args.format)

    if result['state'] != 'succeeded':
        return 1

    return 0


def _has_outputs(red_data):
    """
    Returns whether the given red data contains outputs.

    :param red_data: The red data to check
    :type red_data: Dict[str, Any]
    :return: True, if the given red_data contains outputs, otherwise False
    :rtype: bool
    """
    batches = red_data.get('batches')
    if batches is not None:
        for batch in batches:
            outputs = batch.get('outputs')
            if outputs:
                return True
    else:
        outputs = red_data.get('outputs')
        if outputs:
            return True
    return False


ALLOWED_EXECUTION_ENGINE_ARGUMENTS = {
    'ccfaice': ['preserve_environment', 'disable_pull', 'leave_container', 'insecure', 'gpu_ids'],
    'ccagency': ['disable_retry']
}


def _check_execution_arguments(execution_engine, **arguments):
    """
    Checks whether the given arguments are valid for the faice execution engine.

    :param execution_engine: The execution engine to use. One of ["ccfaice", "ccagency"].
    :type execution_engine: str
    :param arguments: The given cli arguments. Possible arguments:
        ["preserve_environment", "disable_pull", "leave_container", "insecure", "gpu_ids", "disable_retry"]

    :raise InvalidArgumentException: If an invalid argument was found
    """
    allowed_arguments = ALLOWED_EXECUTION_ENGINE_ARGUMENTS[execution_engine]
    invalid_arguments = []
    for key, value in arguments.items():
        if value:
            if key not in allowed_arguments:
                invalid_arguments.append(key)
    if len(invalid_arguments) == 1:
        raise InvalidExecutionEngineArgumentException(
            'Found invalid cli argument "{}" for {}:'.format(invalid_arguments[0], execution_engine)
        )
    elif invalid_arguments:
        raise InvalidExecutionEngineArgumentException(
            'Found invalid cli arguments for {}: {}'.format(execution_engine, ', '.join(invalid_arguments))
        )


def run(
        red_file,
        non_interactive,
        keyring_service,
        preserve_environment,
        disable_pull,
        leave_container,
        insecure,
        gpu_ids,
        disable_retry,
        **_
):
    """
    Runs the RED Client.

    :param red_file: The path or URL to the RED File to execute
    :param non_interactive: If True, unresolved template values are not asked interactively
    :type non_interactive: bool
    :param keyring_service: The keyring service name to use for template substitution
    :type keyring_service: str
    :param preserve_environment: List of environment variables to preserve inside the docker container.
    :type preserve_environment: list[str]
    :param disable_pull: If True the docker image is not pulled from an registry
    :type disable_pull: bool
    :param leave_container: If set to True, the executed docker container will not be removed.
    :type leave_container: bool
    :param insecure: Allow insecure capabilities
    :type insecure: bool
    :param gpu_ids: A list of gpu ids, that should be used. If None all gpus are considered.
    :type gpu_ids: List[int] or None
    :param disable_retry: If True, the execution engine will not retry the experiment, if it fails.
    :type disable_retry: bool

    :return: a dictionary containing debug information about the process
    """
    secret_values = None
    result = {
        'state': 'succeeded',
        'debugInfo': None
    }
    try:
        red_data = load_and_read(red_file, 'REDFILE')

        secret_values = get_secret_values(red_data)
        red_validation(red_data, False)
        engine_validation(red_data, 'execution', ['ccfaice', 'ccagency'], 'faice exec')
        _check_execution_arguments(
            red_data.get('execution', {}).get('engine', 'ccfaice'),
            preserve_environment=preserve_environment,
            disable_pull=disable_pull,
            leave_container=leave_container,
            insecure=insecure,
            gpu_ids=gpu_ids,
            disable_retry=disable_retry
        )

        if 'execution' not in red_data:
            raise KeyError('The key "execution" is needed in red file for usage with faice exec.')

        complete_red_variables(red_data, keyring_service, non_interactive)

        # exec via CC-FAICE
        if red_data['execution']['engine'] == 'ccfaice':
            return run_faice(red_data, preserve_environment, disable_pull, leave_container, insecure, gpu_ids)
        elif red_data['execution']['engine'] == 'ccagency':
            return run_agency(red_data, disable_retry)

    except Exception as e:
        print_exception(e, secret_values)
        result['debugInfo'] = exception_format(secret_values)
        result['state'] = 'failed'

    return result


def run_faice(red_data, preserve_environment, disable_pull, leave_container, insecure, gpu_ids):
    """
    Runs the faice execution engine.

    :param red_data: The file to execute
    :type red_data: str
    :param preserve_environment: A list of strings containing the environment variables, which should be forwarded to
                                 the docker container.
    :type preserve_environment: list[str]
    :param disable_pull: Specifies whether to disable pull or not
    :type disable_pull: bool
    :param leave_container: Specifies whether to remove the container after experiment execution
    :type leave_container: bool
    :param insecure: Specifies whether to allow SYS_ADMIN capabilities
    :type insecure: bool
    :param gpu_ids: List of gpu ids specifying which gpus to use
    :type gpu_ids: list[int]
    """
    # use connectors, if red file specifies outputs
    if _has_outputs(red_data):
        faice_output_mode = OutputMode.Connectors
    else:
        faice_output_mode = OutputMode.Directory

    result = run_faice_execution_engine(
        red_data=red_data,
        disable_pull=disable_pull,
        leave_container=leave_container,
        preserve_environment=preserve_environment,
        insecure=insecure,
        output_mode=faice_output_mode,
        gpu_ids=gpu_ids
    )

    brief_exception_text = result.get('briefExceptionText')
    if brief_exception_text:
        print(brief_exception_text, file=sys.stderr)

    return result


def run_agency(red_data, disable_retry):
    """
    Runs the agency execution engine

    :param red_data: The red data describing the RED experiment
    :type red_data: dict[str, Any]
    :param disable_retry: Specifies, if the experiment should be repeated, if it failed
    :type disable_retry: bool
    :return: The result dictionary of the execution
    :rtype: dict[str, Any]
    """
    result = {
        'state': 'succeeded',
        'debugInfo': None
    }
    if 'access' not in red_data['execution']['settings']:
        result['debugInfo'] = ['ERROR: cannot send RED data to CC-Agency if access settings are not defined.']
        result['state'] = 'failed'
        return result

    if 'auth' not in red_data['execution']['settings']['access']:
        result['debugInfo'] = ['ERROR: cannot send RED data to CC-Agency if auth is not defined in access '
                               'settings.']
        result['state'] = 'failed'
        return result

    access = red_data['execution']['settings']['access']

    r = requests.post(
        '{}/red'.format(access['url'].strip('/')),
        auth=(
            access['auth']['username'],
            access['auth']['password']
        ),
        json=red_data,
        params={'disableRetry': int(disable_retry)}
    )
    if 400 <= r.status_code < 500:
        try:
            pprint(r.json())
        except ValueError:  # if the body does not contain json, we ignore it
            pass
    r.raise_for_status()

    result['response'] = r.json()

    return result
