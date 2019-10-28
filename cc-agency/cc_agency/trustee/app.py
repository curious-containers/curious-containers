from hmac import compare_digest
from argparse import ArgumentParser

from flask import Flask, jsonify, request
from werkzeug.exceptions import Unauthorized

from cc_agency.commons.conf import Conf

DESCRIPTION = 'CC-Agency Trustee.'

app = Flask('trustee')
application = app

parser = ArgumentParser(description=DESCRIPTION)
parser.add_argument(
    '-c', '--conf-file', action='store', type=str, metavar='CONF_FILE',
    help='CONF_FILE (yaml) as local path.'
)
args = parser.parse_args()


conf = Conf(args.conf_file)
username = conf.d['trustee']['username']
password = conf.d['trustee']['password']


secrets = {}


def _verify_user(auth):
    if not auth:
        raise Unauthorized()

    if not compare_digest(auth.username, username):
        raise Unauthorized()

    if not compare_digest(auth.password, password):
        raise Unauthorized()


@app.route('/', methods=['GET'])
def get_root():
    _verify_user(request.authorization)

    return jsonify({
        'state': 'success'
    })


@app.route('/secrets', methods=['POST'])
def post_secrets():
    _verify_user(request.authorization)

    data = request.json

    existing_keys = []

    for key in data:
        if key in secrets:
            existing_keys.append(key)

    if existing_keys:
        return jsonify({
            'state': 'failed',
            'debug_info': 'Keys already exist: {}'.format(existing_keys),
            'disable_retry': False,
            'inspect': False
        })

    secrets.update(data)

    return jsonify({
        'state': 'success'
    })


@app.route('/secrets', methods=['GET'])
def get_secrets():
    _verify_user(request.authorization)

    data = request.json

    collected = {}
    missing_keys = []

    for key in data:
        try:
            collected[key] = secrets[key]
        except KeyError:
            missing_keys.append(key)

    if missing_keys:
        return jsonify({
            'state': 'failed',
            'debug_info': 'Could not collect keys: {}'.format(missing_keys),
            'disable_retry': True,
            'inspect': False
        })

    return jsonify({
        'state': 'success',
        'secrets': collected
    })


@app.route('/secrets', methods=['DELETE'])
def delete_secrets():
    _verify_user(request.authorization)

    data = request.json

    for key in data:
        try:
            del secrets[key]
        except KeyError:
            pass

    return jsonify({
        'state': 'success'
    })
