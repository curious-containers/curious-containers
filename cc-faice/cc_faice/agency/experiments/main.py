import requests
import sys
import certifi
import os

from argparse import ArgumentParser

from ..utility import getAuth
from ..utility import show

DESCRIPTION = 'Get the list of experiments.'
REQ_TYPE = 'experiments'

def attach_args(parser):
    parser.add_argument('--raw', action='store_true',
                        default=False, help='Show raw json object')
    parser.add_argument('--limit', action='store_true',
                        default=5, help='Maximum items to show')
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

    if auth is None:
        raise ValueError(
            'Could not find authentication for host: {}'.format(args.agency_url))

    url = '{}/{}'.format(args.agency_url, REQ_TYPE)

    params = {
        'limit': args.limit
    }

    try:
        r = requests.get(url, auth=auth, params=params, verify=False)
    except requests.exceptions.SSLError as e:
        print('ERROR: Could not connect to agency {}:\n{}'.format(url, repr(e)))
        print('\nVisit "{}" with your browser and download the certificate chain PEM file. Add the content of this file to "{}"'.format(
            url, certifi.where()))
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        print('ERROR: Could not connect to agency {}:\n{}'.format(url, repr(e)))
        sys.exit(1)
    if not r.ok:
        print('ERROR: {}\nurl: {}'.format(r.status_code, url))
    else:
        show(args, REQ_TYPE, r)
