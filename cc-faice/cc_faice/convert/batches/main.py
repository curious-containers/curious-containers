from argparse import ArgumentParser

from cc_core.commons.exceptions import print_exception, exception_format
from cc_core.commons.files import load_and_read, file_extension, wrapped_print, dump, dump_print
from cc_core.commons.red import red_validation, convert_batch_experiment
from cc_core.commons.templates import get_secret_values

DESCRIPTION = 'Convert batches from a single REDFILE into separate files containing only one batch each.'


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
        '--prefix', action='store', type=str, metavar='PREFIX', default='faice_',
        help='PREFIX for files dumped to storage, default is "faice_".'
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

    if args.debug:
        dump_print(result, args.format)

    if result['state'] != 'succeeded':
        return 1

    return 0


def run(red_file, fmt, prefix, **_):
    secret_values = None
    result = {
        'state': 'succeeded',
        'debugInfo': None
    }
    try:
        ext = file_extension(fmt)

        red_data = load_and_read(red_file, 'REDFILE')
        secret_values = get_secret_values(red_data)

        red_validation(red_data, False)

        if 'batches' not in red_data:
            wrapped_print([
                'ERROR: REDFILE does not contain batches.'
            ], error=True)
            return 1

        for batch in range(len(red_data['batches'])):
            batch_data = convert_batch_experiment(red_data, batch)
            dumped_batch_file = '{}batch_{}.{}'.format(prefix, batch, ext)
            dump(batch_data, fmt, dumped_batch_file)
    except Exception as e:
        print_exception(e, secret_values)
        result['debugInfo'] = exception_format(secret_values)
        result['state'] = 'failed'

    return result
