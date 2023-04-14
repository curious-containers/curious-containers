import os
import requests

import bcrypt
from argparse import ArgumentParser
from getpass import getpass
from ..utility import getAuth

DESCRIPTION = 'Creates a user in the agency.'
REQ_TYPE = 'createuser'

def attach_args(parser):
    parser.add_argument(
        '-c', '--conf-file', action='store', type=str, metavar='CONF_FILE',
        help='CONF_FILE (yaml) as local path.'
    )
    parser.add_argument('--agency-url', type=str, metavar='AGENCY_URL',
                        default=os.environ.get('AGENCY_URL'), help='The url of the agency to test')
    parser.add_argument('--account', type=str, metavar='ACCOUNT', 
                        help='The login account to the agency')
    parser.add_argument(
        '--keyring-service', action='store', type=str, metavar='KEYRING_SERVICE', default='red',
        help='Keyring service to resolve template values, default is "red".'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()
    
    if not hasattr(args, 'account') or args.account is None:
        print('ERROR: the following arguments are required: --account')
        return 1

    auth = getAuth(args.agency_url, args.account, args.keyring_service)

    return run(auth, args.agency_url, args.account, args.conf_file)


def run(auth, agency_url, account, config_file=None):
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
        
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
    
    data = {
        'username': username,
        'password': hashed_password,
        'is_admin': is_admin
    }
    
    files = {}
    if config_file and os.path.exists(config_file):
        files['conf_file'] = open(config_file, 'rb')
        
    url = '{}/{}'.format(agency_url, REQ_TYPE)
    
    response = requests.post(url, auth=auth, data=data, files=files, verify=False)

    if response.status_code == 200:
        print('User created successfully!')
        print('Done!')
    else:
        print('An error occurred while creating the user.')
        
    
