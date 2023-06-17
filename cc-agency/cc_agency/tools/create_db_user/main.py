import os
import json
from argparse import ArgumentParser
from subprocess import call
from time import sleep
from ruamel.yaml import YAML

yaml = YAML(typ='safe')

DESCRIPTION = 'Create a MongoDB admin user with read and write access, as specified in cc-agency configuration.'


def attach_args(parser):
    parser.add_argument(
        '-c', '--conf-file', action='store', type=str, metavar='CONF_FILE',
        help='CONF_FILE (yaml) as local path.'
    )
    parser.add_argument(
        '--host', action='store', type=str, metavar='HOST',
        help='Overwrite MongoDB host specified in CONF_FILE.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()

    return run(**args.__dict__)


def run(conf_file, host):
    if not conf_file:
        conf_file = os.path.join('~', '.config', 'cc-agency.yml')

    conf_file = os.path.expanduser(conf_file)

    with open(conf_file) as f:
        conf = yaml.load(f)

    host = host if host else conf['mongo'].get('host', 'localhost')
    port = conf['mongo'].get('port', 27017)
    db = conf['mongo']['db']
    username = conf['mongo']['username']
    password = conf['mongo']['password']

    data = {
        'pwd': password,
        'roles': [{
            'role': 'readWrite',
            'db': db
        }]
    }

    dumped = json.dumps(data)

    update_command = 'mongo --host {host} --port {port} --eval \'database = db.getSiblingDB("{db}"); database.updateUser("{username}", {dumped})\''.format(
        host=host,
        port=port,
        db=db,
        username=username,
        dumped=dumped
    )

    data['user'] = username

    dumped = json.dumps(data)

    create_command = 'mongo --host {host} --port {port} --eval \'database = db.getSiblingDB("{db}"); database.createUser({dumped})\''.format(
        host=host,
        port=port,
        db=db,
        dumped=dumped
    )

    for _ in range(10):
        code = call(update_command, shell=True)
        if code == 0:
            break
        else:
            code = call(create_command, shell=True)
            if code == 0:
                break
        sleep(1)
