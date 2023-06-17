from argparse import ArgumentParser

from cc_agency.commons.conf import Conf
from cc_agency.commons.db import Mongo

DESCRIPTION = 'Drop MongoDB collections.'


def attach_args(parser):
    parser.add_argument(
        '-c', '--conf-file', action='store', type=str, metavar='CONF_FILE',
        help='CONF_FILE (yaml) as local path.'
    )
    parser.add_argument(
        action='store', type=str, nargs='+', metavar='COLLECTIONS', dest='collections',
        choices=['experiments', 'batches', 'users', 'tokens', 'block_entries', 'callback_tokens'],
        help='Collections to be dropped.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()

    return run(**args.__dict__)


def run(conf_file, collections):
    conf = Conf(conf_file)
    mongo = Mongo(conf)

    for collection in collections:
        mongo.db[collection].drop()
