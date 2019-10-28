import sys
from argparse import ArgumentParser

from cc_core.commons.schema_map import schemas
from cc_core.commons.files import dump_print


DESCRIPTION = 'Write a jsonschema to stdout.'


def attach_args(parser):
    parser.add_argument(
        'schema', action='store', type=str, metavar='SCHEMA',
        help='SCHEMA as in "faice schemas list".'
    )
    parser.add_argument(
        '--format', action='store', type=str, metavar='FORMAT', choices=['json', 'yaml', 'yml'], default='yaml',
        help='Specify FORMAT for generated data as one of [json, yaml, yml]. Default is yaml.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()
    return run(**args.__dict__, fmt=args.format)


def run(schema, fmt, **_):
    if schema not in schemas:
        print('Schema "{}" not found. Use "faice schema list" for available schemas.'.format(schema), file=sys.stderr)
        return 1

    dump_print(schemas[schema], fmt)
    return 0
