import sys
from collections import OrderedDict
from argparse import ArgumentParser

from cc_agency.version import VERSION

from cc_agency.tools.create_db_user.main import main as create_db_user_main
from cc_agency.tools.create_broker_user.main import main as create_broker_user_main
from cc_agency.tools.drop_db_collections.main import main as drop_db_collections_main

from cc_agency.tools.create_db_user.main import DESCRIPTION as CREATE_DB_USER_DESCRIPTION
from cc_agency.tools.create_broker_user.main import DESCRIPTION as CREATE_BROKER_USER_DESCRIPTION
from cc_agency.tools.drop_db_collections.main import DESCRIPTION as DROP_DB_COLLECTIONS_DESCRIPTION


SCRIPT_NAME = 'ccagency'

DESCRIPTION = 'CC-Agency Copyright (C) 2018  Christoph Jansen. This software is distributed under the AGPL-3.0 ' \
              'LICENSE and is part of the Curious Containers project (https://curious-containers.github.io/).'

MODES = OrderedDict([
    ('create-db-user', {'main': create_db_user_main, 'description': CREATE_DB_USER_DESCRIPTION}),
    ('create-broker-user', {'main': create_broker_user_main, 'description': CREATE_BROKER_USER_DESCRIPTION}),
    ('drop-db-collections', {'main': drop_db_collections_main, 'description': DROP_DB_COLLECTIONS_DESCRIPTION})
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
