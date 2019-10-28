from argparse import ArgumentParser

from cc_core.commons.exceptions import print_exception, AgentError, exception_format
from cc_core.commons.files import dump_print, load_and_read


DESCRIPTION = 'Read an arbitrary JSON or YAML file and convert it into the specified format.'


def attach_args(parser):
    parser.add_argument(
        'file', action='store', type=str, metavar='FILE',
        help='FILE (json or yaml) to be converted into specified FORMAT as local path or http url.'
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

    if result['state'] == 'succeeded':
        return 0
    else:
        if args.debug:
            dump_print(result, args.format)

    return 1


def run(file, fmt, **_):
    result = {
        'state': 'succeeded',
        'debugInfo': None
    }

    try:
        data = load_and_read(file, 'FILE')
        dump_print(data, fmt)
    except Exception as e:
        print_exception(e)
        result['debugInfo'] = exception_format()
        result['state'] = 'failed'

    return result
