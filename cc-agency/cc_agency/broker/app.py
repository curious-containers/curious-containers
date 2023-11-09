import os
from argparse import ArgumentParser
from datetime import timedelta

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
from cc_agency.broker.routes.nodes import nodes_routes


DESCRIPTION = 'CC-Agency Broker.'

app = Flask('broker')
app.config["JWT_SECRET_KEY"] = "super-secret"  # TODO generate random secret key
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=15)
app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(hours=3)


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
nodes_routes(app, mongo, auth)

controller.send_json({'destination': 'scheduler'})
