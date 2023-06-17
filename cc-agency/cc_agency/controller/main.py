import os
from argparse import ArgumentParser
import atexit

import zmq
import pymongo

from cc_core.version import VERSION as CORE_VERSION
from cc_agency.version import VERSION as AGENCY_VERSION
from red_val.version import VERSION as RED_VAL_VERSION
from cc_agency.commons.conf import Conf
from cc_agency.commons.db import Mongo
from cc_agency.commons.secrets import TrusteeClient
from cc_agency.controller.scheduler import Scheduler


DESCRIPTION = 'CC-Agency Controller'


def main():
    print('CC-Agency Version:', AGENCY_VERSION)
    print('CC-Core Version:', CORE_VERSION)
    print('RED-Val Version:', RED_VAL_VERSION)

    parser = ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '-c', '--conf-file', action='store', type=str, metavar='CONF_FILE',
        help='CONF_FILE (yaml) as local path.'
    )
    args = parser.parse_args()

    # Singletons
    conf = Conf(args.conf_file)
    mongo = Mongo(conf)

    # MongoDB indexes
    mongo.db['batches'].create_index([('state', pymongo.ASCENDING)])
    mongo.db['batches'].create_index([('protectedKeysVoided', pymongo.ASCENDING)])
    mongo.db['batches'].create_index([('notificationsSent', pymongo.ASCENDING)])
    mongo.db['batches'].create_index([('experimentId', pymongo.ASCENDING)])
    mongo.db['batches'].create_index([('username', pymongo.ASCENDING)])

    # print('MongoDB Indexes:')
    # pprint(list(mongo.db['experiments'].list_indexes()) + list(mongo.db['batches'].list_indexes()))

    # Singletons
    trustee_client = TrusteeClient(conf)
    scheduler = Scheduler(conf, mongo, trustee_client)

    # ZeroMQ socket
    bind_socket_path = os.path.expanduser(conf.d['controller']['bind_socket_path'])
    bind_socket_dir, _ = os.path.split(bind_socket_path)

    os.makedirs(bind_socket_dir, exist_ok=True)

    old_umask = os.umask(0o077)
    context = zmq.Context()
    # noinspection PyUnresolvedReferences
    socket = context.socket(zmq.PULL)
    socket.bind('ipc://{}'.format(bind_socket_path))
    os.umask(old_umask)

    atexit.register(socket.close)

    while True:
        data = socket.recv_json()

        if 'destination' not in data:
            continue

        destination = data['destination']
        if destination == 'scheduler':
            scheduler.schedule()
