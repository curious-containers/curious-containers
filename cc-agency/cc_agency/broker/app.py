import os
from argparse import ArgumentParser

from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_jwt_extended import JWTManager
import zmq

from cc_agency.commons.conf import Conf
from cc_agency.commons.db import Mongo
from cc_agency.commons.secrets import TrusteeClient
from cc_agency.commons.cloud_proxy import CloudProxy
from cc_agency.broker.auth import Auth
from cc_agency.broker.routes.red import red_routes
from cc_agency.broker.jwt_token import configure_jwt


DESCRIPTION = 'CC-Agency Broker.'

app = Flask('broker')

jwt = JWTManager(app)
cors = CORS(app, supports_credentials=True)
application = app

parser = ArgumentParser(description=DESCRIPTION)
parser.add_argument(
    '-c', '--conf-file', action='store', type=str, metavar='CONF_FILE',
    help='CONF_FILE (yaml) as local path.'
)
args = parser.parse_args()

conf = Conf(args.conf_file)
configure_jwt(app, conf)
mongo = Mongo(conf)
auth = Auth(conf, mongo)
trustee_client = TrusteeClient(conf)
cloud_proxy = CloudProxy(conf, mongo, auth)

bind_socket_path = os.path.expanduser(conf.d['controller']['bind_socket_path'])
bind_socket = 'ipc://{}'.format(bind_socket_path)

context = zmq.Context()
# noinspection PyUnresolvedReferences
controller = context.socket(zmq.PUSH)
controller.connect(bind_socket)


@app.route('/', methods=['GET'])
def get_root():
    return jsonify({'Hello': 'World'})


red_routes(app, jwt, mongo, auth, controller, trustee_client, cloud_proxy)

controller.send_json({'destination': 'scheduler'})
