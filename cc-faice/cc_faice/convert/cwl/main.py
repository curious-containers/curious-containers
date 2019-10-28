from argparse import ArgumentParser

from jsonschema import validate
from jsonschema.exceptions import ValidationError

from cc_core.commons.exceptions import AgentError, print_exception, exception_format, RedSpecificationError
from cc_core.commons.files import dump_print, load_and_read


DESCRIPTION = 'Read cli section of a REDFILE and write it to stdout in the specified format.'


def attach_args(parser):
    parser.add_argument(
        'red_file', action='store', type=str, metavar='REDFILE',
        help='REDFILE (json or yaml) containing an experiment description as local PATH or http URL.'
    )
    parser.add_argument(
        '--format', action='store', type=str, metavar='FORMAT', choices=['json', 'yaml', 'yml'], default='yaml',
        help='Specify FORMAT for generated data as one of [json, yaml, yml]. Default is yaml.'
    )
    parser.add_argument(
        '-d', '--debug', action='store_true',
        help='Write debug info, including detailed exceptions, to stdout.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()

    result = run(**args.__dict__, fmt=args.format)

    if args.debug and (result['state'] != 'succeeded'):
        dump_print(result, args.format)

    if result['state'] != 'succeeded':
        return 1

    return 0


def run(red_file, fmt, **_):
    result = {
        'state': 'succeeded',
        'debugInfo': None
    }

    try:
        red_data = load_and_read(red_file, 'REDFILE')
    except AgentError as e:
        print_exception(e)
        result['debugInfo'] = exception_format()
        result['state'] = 'failed'
        return result

    if 'cli' not in red_data:
        print_exception(RedSpecificationError('ERROR: REDFILE does not contain cli section.'))
        result['debugInfo'] = exception_format()
        result['state'] = 'failed'
        return result

    cli = red_data['cli']

    dump_print(cli, fmt)

    return result
