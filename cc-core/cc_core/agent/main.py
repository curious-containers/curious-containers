import sys
from collections import OrderedDict
from argparse import ArgumentParser

from cc_core.version import VERSION

from cc_core.agent.blue.__main__ import main as blue_main
from cc_core.agent.blue.__main__ import DESCRIPTION as BLUE_DESCRIPTION

SCRIPT_NAME = 'ccagent'

DESCRIPTION = 'CC-Agent Copyright (C) 2018  Christoph Jansen. This software is distributed under the AGPL-3.0 ' \
              'LICENSE and is part of the Curious Containers project (https://curious-containers.github.io/).'

MODES = OrderedDict([
    ('blue', {'main': blue_main, 'description': BLUE_DESCRIPTION})
])


def main():
    sys.argv[0] = SCRIPT_NAME

    parser = ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '-v', '--version', action='version', version=VERSION
    )
    subparsers = parser.add_subparsers(title='modes')

    sub_parser = None
    for key, val in MODES.items():
        sub_parser = subparsers.add_parser(key, help=val['description'], add_help=False)

    if len(sys.argv) < 2:
        parser.print_help()
        exit()

    _ = parser.parse_known_args()
    sub_args = sub_parser.parse_known_args()

    mode = MODES[sub_args[1][0]]['main']
    sys.argv[0] = '{} {}'.format(SCRIPT_NAME, sys.argv[1])
    del sys.argv[1]
    exit(mode())
