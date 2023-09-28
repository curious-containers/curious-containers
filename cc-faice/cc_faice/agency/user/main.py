import os
import requests

from argparse import ArgumentParser
from getpass import getpass
from ..utility import get_auth

DESCRIPTION = 'Creates a user in the agency.'
REQ_TYPE = 'createuser'

def attach_args(parser):
    parser.add_argument('--agency-url', type=str, metavar='AGENCY_URL',
                        default=os.environ.get('AGENCY_URL'), help='The url of the agency to test')
    parser.add_argument('--account', type=str, metavar='ACCOUNT', required=True,
                        help='The login account to the agency')
    parser.add_argument(
        '--keyring-service', action='store', type=str, metavar='KEYRING_SERVICE', default='red',
        help='Keyring service to resolve template values, default is "red".'
    )


def main():
    parser = ArgumentParser(description=DESCRIPTION)
    attach_args(parser)
    args = parser.parse_args()

    auth = get_auth(args.agency_url, args.account, args.keyring_service)

    return run(auth, args.agency_url)


def run(auth, agency_url):
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
    
    data = {
        'username': username,
        'password': password,
        'is_admin': is_admin
    }
        
    url = '{}/{}'.format(agency_url, REQ_TYPE)
    
    response = requests.post(url, auth=auth, json=data)

    if response.status_code == 201:
        print('User created successfully!')
        print('Done!')
    else:
        print('An error occurred while creating the user.')
        
    
