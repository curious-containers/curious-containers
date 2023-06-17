from argparse import ArgumentParser

from cc_core.commons.schema_map import schemas
from cc_core.commons.files import dump_print


DESCRIPTION = 'List of all available jsonschemas defined in cc-core.'


def attach_args(parser):
    parser.add_argument(
        '--format', action='store', type=str, metavar='FORMAT', choices=['json', 'yaml', 'yml'], default='yaml',
        help='Specify FORMAT for generated data as one of [json, yaml, yml]. Default is yaml.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()
    run(fmt=args.format)
    return 0


def run(fmt):
    dump_print({'schemas': list(schemas.keys())}, fmt)
