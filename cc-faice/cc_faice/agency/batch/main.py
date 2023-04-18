import requests
import sys
import certifi
import os
from argparse import ArgumentParser

from ..utility import get_auth
from ..utility import show

DESCRIPTION = 'Get batch details with batch #id'
REQ_TYPE = 'batches'


def attach_args(parser):
    parser.add_argument('id', type=str, metavar='ID', required=True,
                        help='The id of the experiment/batch.')
    parser.add_argument('--raw', action='store_true',
                        default=False, help='Show raw json object')    
    parser.add_argument('--agency-url', type=str, metavar='AGENCY_URL',
                        default=os.environ.get('AGENCY_URL'), help='The url of the agency to test')
    parser.add_argument('--show-file', type=str, metavar='OUTFILE',
                        default=None, help='Show content of stdout/stderr')
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

    if auth is None:
        raise ValueError(
            'Could not find authentication for host: {}'.format(args.agency_url))

    url = '{}/{}'.format(args.agency_url, REQ_TYPE)
    url = '{}/{}'.format(url, args.id)
    if args.show_file is not None:
        url = '{}/{}'.format(url, args.show_file)

    try:
        r = requests.get(url, auth=auth)
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
