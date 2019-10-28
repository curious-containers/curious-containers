from argparse import ArgumentParser
from copy import deepcopy
from pprint import pprint

import requests

from cc_core.commons.exceptions import print_exception, exception_format
from cc_core.commons.files import load_and_read, dump_print
from cc_core.commons.red import red_validation
from cc_core.commons.engines import engine_validation
from cc_core.commons.templates import normalize_keys, get_secret_values

from cc_faice.agent.red.main import run as run_faice_agent_red, OutputMode
from cc_faice.commons.templates import complete_red_templates

DESCRIPTION = 'Execute experiment according to execution engine defined in REDFILE.'


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
        '--non-interactive', action='store_true',
        help='Do not ask for RED variables interactively.'
    )
    parser.add_argument(
        '--format', action='store', type=str, metavar='FORMAT', choices=['json', 'yaml', 'yml'], default='yaml',
        help='Specify FORMAT for generated data as one of [json, yaml, yml]. Default is yaml.'
    )
    parser.add_argument(
        '--insecure', action='store_true',
        help='This argument will be passed to ccfaice, if the given REDFILE refers to this execution engine. '
             'See "faice agent red --help" for more information.'
    )
    parser.add_argument(
        '--keyring-service', action='store', type=str, metavar='KEYRING_SERVICE', default='red',
        help='Keyring service to resolve template values, default is "red".'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()
    result = run(**args.__dict__, fmt=args.format)

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


def run(red_file, non_interactive, fmt, insecure, keyring_service, **_):
    secret_values = None
    result = {
        'state': 'succeeded',
        'debugInfo': None
    }
    try:
        red_data = load_and_read(red_file, 'REDFILE')
        red_validation(red_data, False)
        engine_validation(red_data, 'execution', ['ccfaice', 'ccagency'], 'faice exec')

        secret_values = get_secret_values(red_data)

        # exec via CC-FAICE
        # equivalent to `faice agent red --debug --outputs`
        if 'execution' not in red_data:
            raise KeyError('The key "execution" is needed in red file for usage with faice exec.')
        if red_data['execution']['engine'] == 'ccfaice':
            # use connectors, if red file specifies outputs
            if _has_outputs(red_data):
                faice_output_mode = OutputMode.Connectors
            else:
                faice_output_mode = OutputMode.Directory

            result = run_faice_agent_red(
                red_file=red_file,
                disable_pull=False,
                leave_container=False,
                preserve_environment=[],
                non_interactive=non_interactive,
                insecure=insecure,
                output_mode=faice_output_mode,
                keyring_service=keyring_service,
                gpu_ids=None
            )
            return result

        complete_red_templates(red_data, keyring_service, non_interactive)

        red_data_normalized = deepcopy(red_data)
        normalize_keys(red_data_normalized)

        if 'access' not in red_data_normalized['execution']['settings']:
            result['debugInfo'] = ['ERROR: cannot send RED data to CC-Agency if access settings are not defined.']
            result['state'] = 'failed'
            return result

        if 'auth' not in red_data_normalized['execution']['settings']['access']:
            result['debugInfo'] = ['ERROR: cannot send RED data to CC-Agency if auth is not defined in access '
                                   'settings.']
            result['state'] = 'failed'
            return result

        access = red_data_normalized['execution']['settings']['access']

        r = requests.post(
            '{}/red'.format(access['url'].strip('/')),
            auth=(
                access['auth']['username'],
                access['auth']['password']
            ),
            json=red_data
        )
        if 400 <= r.status_code < 500:
            try:
                pprint(r.json())
            except ValueError:  # if the body does not contain json, we ignore it
                pass
        r.raise_for_status()

        dump_print(r.json(), fmt)
    except Exception as e:
        print_exception(e, secret_values)
        result['debugInfo'] = exception_format(secret_values)
        result['state'] = 'failed'

    return result
