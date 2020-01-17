import base64
from os import urandom
from binascii import hexlify
from time import time

import flask
from flask import request, stream_with_context
from bson.objectid import ObjectId
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from gridfs import GridOut


USER_SPECIFIED_STDOUT_KEY = 'usedSpecifiedStdout'
USER_SPECIFIED_STDERR_KEY = 'usedSpecifiedStderr'
STDOUT_FILE_KEY = 'cliStdout'
STDERR_FILE_KEY = 'cliStderr'


def str_to_bool(s):
    """
    Converts the given string into a boolean.

    Values resulting in True:
    - a string representing an integer != 0
    - the string 'true' ignoring upper/lower case
    - the string 'y' or 'yes' ignoring upper/lower case

    Other values will result in False as return value.

    :param s: The string to convert
    :type s: str
    :return: A boolean value
    :rtype: bool
    """
    if isinstance(s, bool):
        return s

    if not isinstance(s, str):
        return False

    if s.lower() in ('true', 'yes', 'y'):
        return True

    try:
        i = int(s)
        if i != 0:
            return True
    except ValueError:
        pass

    return False


def decode_authentication_cookie(cookie_value):
    """
    Extracts the username and value from the given cookie value.

    The value of the cookie should match the following format: base64(username):identifier

    :param cookie_value: The value of the authentication cookie.
    :type cookie_value: str
    :return: A tuple (username, token) read from the given cookie value
    :rtype: tuple[str, str]
    """
    username_base64, token = cookie_value.split(':', maxsplit=1)
    username = base64.b64decode(username_base64.encode('utf-8')).decode('utf-8')
    return username, str(token)


def encode_authentication_cookie(username, token):
    """
    Encodes the given username and the given token into one bytes object of the following form:

    base64(username):token

    :param username: The username to encode
    :type username: str
    :param token: The token to encode
    :type token: str
    :return: A str that contains username and token
    :rtype: str
    """
    return '{}:{}'.format(
        base64.b64encode(username.encode('utf-8')).decode('utf-8'),
        token
    )


def get_gridfs_filename(batch_id, file_identifier):
    """
    Converts the given batch_id and the stdout/stderr string into a GridFS filename.

    :param batch_id: The batch id this stdout/stderr file is associated with
    :type batch_id: str
    :param file_identifier: The identifier of the file
    :type file_identifier: str
    :return: A string representing the stdout/stderr filename for the given batch
    :rtype: str
    """
    return '{}_{}'.format(batch_id, file_identifier)


def create_file_flask_response(source_file, auth, authentication_cookie=None):
    """
    Creates a flask response object, containing the given data given by source_file as plain text and the given
    authentication cookie.

    :param source_file: The data to send back
    :type source_file: str or bytes or GridOut
    :param auth: The auth object to use
    :param authentication_cookie: The value of the authentication cookie
    :return: A flask response object
    """
    flask_response = flask.Response(
        stream_with_context(source_file),
        content_type='text/plain',
        status='200'
    )

    if authentication_cookie:
        flask_response.set_cookie(
            authentication_cookie[0],
            authentication_cookie[1],
            expires=time() + auth.tokens_valid_for_seconds
        )
    return flask_response


def create_flask_response(data, auth, authentication_cookie=None):
    """
    Creates a flask response object, containing the given json data and the given authentication cookie.

    :param data: The data to send as json object
    :param auth: The auth object to use
    :param authentication_cookie: The value for the authentication cookie
    :return: A flask response object
    """
    flask_response = flask.make_response(
        flask.jsonify(data),
        200
    )
    if authentication_cookie:
        flask_response.set_cookie(
            authentication_cookie[0],
            authentication_cookie[1],
            expires=time() + auth.tokens_valid_for_seconds
        )
    return flask_response


def get_ip():
    headers = ['HTTP_X_FORWARDED_FOR', 'HTTP_X_REAL_IP', 'REMOTE_ADDR']
    ip = None
    for header in headers:
        ip = request.environ.get(header)
        if ip:
            break
    if not ip:
        ip = '127.0.0.1'
    return ip


def generate_secret():
    return hexlify(urandom(24)).decode('utf-8')


def create_kdf(salt):
    return PBKDF2HMAC(
        algorithm=SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )


def batch_failure(
        mongo,
        batch_id,
        debug_info,
        ccagent,
        current_state,
        disable_retry_if_failed=False,
        docker_stats=None
):
    """
    Changes the db entry of the given batch to failed, if disable_retry_if_failed is set to True or if the maximal
    number of retries is exceeded. Otherwise the new state of the given batch is set to registered.

    :param mongo: The mongodb client to update
    :param batch_id: The batch id specifying the batch to fail
    :type batch_id: str
    :param debug_info: The debug info to write to the db
    :param ccagent: The ccagent to write to the db
    :param current_state: The expected current state of the batch to cancel. If this state does not match the batch from
                          the db, the db entry is not updated.
    :type current_state: str
    :param disable_retry_if_failed: If set to True, the batch is failed immediately, without giving another attempt
    :param docker_stats: The optional stats of the docker container, that will written under the "docker_stats" key in
                         the history of this batch.
                         This feature is not implemented at the moment.
    :type docker_stats: dict
    """
    if current_state in ['succeeded', 'failed', 'cancelled']:
        return

    bson_id = ObjectId(batch_id)

    batch = mongo.db['batches'].find_one(
        {'_id': bson_id},
        {'attempts': 1, 'node': 1, 'experimentId': 1}
    )

    if batch is None:
        raise BatchNotFoundException('Batch "{}" could not be found.'.format(batch_id))

    timestamp = time()
    attempts = batch['attempts']
    node_name = batch['node']

    new_state = 'registered'
    new_node = None

    if attempts >= 2 or disable_retry_if_failed:
        new_state = 'failed'
        new_node = node_name
    else:
        experiment_id = batch['experimentId']
        bson_experiment_id = ObjectId(experiment_id)
        experiment = mongo.db['experiments'].find_one(
            {'_id': bson_experiment_id},
            {'execution.settings.retryIfFailed': 1}
        )
        if not (experiment and experiment.get('execution', {}).get('settings', {}).get('retryIfFailed')):
            new_state = 'failed'
            new_node = node_name

    # dont use docker stats, because they do not contain useful information
    del docker_stats

    mongo.db['batches'].update_one(
        {'_id': bson_id, 'state': current_state},
        {
            '$set': {
                'state': new_state,
                'node': new_node
            },
            '$push': {
                'history': {
                    'state': new_state,
                    'time': timestamp,
                    'debugInfo': debug_info,
                    'node': new_node,
                    'ccagent': ccagent,
                    # 'dockerStats': docker_stats
                }
            }
        }
    )


class BatchNotFoundException(Exception):
    pass
