from argparse import ArgumentParser
from getpass import getpass

from cc_agency.commons.conf import Conf
from cc_agency.commons.db import Mongo
from cc_agency.broker.auth import Auth

DESCRIPTION = 'Create a broker user to authenticate with the web API.'


def attach_args(parser):
    parser.add_argument(
        '-c', '--conf-file', action='store', type=str, metavar='CONF_FILE',
        help='CONF_FILE (yaml) as local path.'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()

    return run(**args.__dict__)


def run(conf_file):
    conf = Conf(conf_file)
    mongo = Mongo(conf)
    auth = Auth(conf, mongo)

    print('You are in the process of creating a user account.')
    print('ATTENTION: an already existing user with the exact same username will be updated with new settings!')
    input('Hit [ENTER] to proceed...')

    username = input('Username: ')
    if not username:
        print('ABORT: username must not be empty.')
        exit(1)

    password = getpass('Password: ')
    if not password:
        print('ABORT: password must not be empty.')

    is_admin = input('Grant admin rights [y/N]: ')
    is_admin = is_admin.lower()
    is_admin = is_admin == 'yes' or is_admin == 'y'

    if is_admin:
        print('Admin privileges GRANTED!')
    else:
        print('Admin privileges NOT granted!')

    auth.create_user(username, password, is_admin=is_admin)
    print('Done!')
