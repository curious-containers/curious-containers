import pytest
import subprocess
import requests
import time
import json
import re
import os
import shutil
from flask import Flask, request, jsonify
from werkzeug.serving import make_server
import threading

DOCKER_HOST_IP = '172.17.0.1'
SSH_PORT = '2222'
SSH_USER = 'test_user'
SSH_PASSWORD = 'test_password'

AGENCY_URL = 'http://localhost:8080'
AGENCY_USER = 'agency_user'
AGENCY_PASSWORD = 'agency_password'
MAX_RETRIES = 48
RETRY_DELAY = 5
NOTIFICATION_PORT = 8090

OUTPUT_DIR = './output'


class ServerThread(threading.Thread):

    def __init__(self, app):
        threading.Thread.__init__(self)
        self.server = make_server(DOCKER_HOST_IP, NOTIFICATION_PORT, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


def read_red_file(filename):
    with open(filename, 'r') as f:
        red_file = f.read()
        
    red_file = re.sub('{{agency_url}}', AGENCY_URL, red_file)
    red_file = re.sub('{{agency_user}}', AGENCY_USER, red_file)
    red_file = re.sub('{{agency_password}}', AGENCY_PASSWORD, red_file)
    red_file = re.sub('{{host_ip}}', DOCKER_HOST_IP, red_file)
    red_file = re.sub('"{{ssh_port}}"', SSH_PORT, red_file)
    red_file = re.sub('{{ssh_username}}', SSH_USER, red_file)
    red_file = re.sub('{{ssh_password}}', SSH_PASSWORD, red_file)
    
    return json.loads(red_file)


def find_batch_id(response):
    batch_ids = find_batch_ids(response)
    if len(batch_ids) > 0:
        return batch_ids[0]
    else:
        return ''


def find_batch_ids(response):
    data = response.json()
    experiment_id = data['experimentId']
    batches = requests.get(AGENCY_URL + '/batches', auth=(AGENCY_USER, AGENCY_PASSWORD)).json()
    
    batch_ids = []
    for batch in batches:
        if batch['experimentId'] == experiment_id:
            batch_ids.append(batch['_id'])
    
    return batch_ids


def fetch_final_batch_state(batch_id):
    final_states = ['succeeded', 'failed', 'cancelled']
    current_state = ''
    
    for _ in range(MAX_RETRIES):
        batch_response = requests.get(AGENCY_URL + '/batches/' + batch_id, auth=(AGENCY_USER, AGENCY_PASSWORD))
        batch_data = batch_response.json()
        current_state = batch_data['state']
        
        if current_state in final_states:
            return current_state
        else:
            time.sleep(RETRY_DELAY)
    
    pytest.fail("The experiment exceeded the specified timeout. It could not be verified if the experiment has reached a final state.")


def is_server_up(url):
    try:
        response = requests.get(url)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def wait_for_agency_boot_up():
    for _ in range(MAX_RETRIES):
        if is_server_up(AGENCY_URL):
            return
        time.sleep(RETRY_DELAY)
    pytest.fail("The agency did not start within the specified timeout. Integration tests aborted.")


@pytest.fixture(scope="module")
def setup_agency():
    # setup cc-agency
    try:
        os.mkdir(OUTPUT_DIR)
    except FileExistsError:
        pass
    
    subprocess.Popen(["sh", "setup/build_docker_images.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    subprocess.Popen(["sh", "setup/start_agency.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    wait_for_agency_boot_up()

    # execute testcases
    yield

    # teardown cc-agency
    subprocess.Popen(["sh", "setup/stop_agency.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    shutil.rmtree(OUTPUT_DIR)


@pytest.fixture
def notification_webserver():
    # setup webserver
    app = Flask(__name__)
    app.logger.disabled = True
    server = ServerThread(app)
    server.start()
    
    received_hooks = []

    @app.route('/notification_hook', methods=['POST'])
    def notification_hook():
        data = request.json
        if len(data['batches']) > 0:
            received_hooks.append(data)
        return jsonify(success=True)

    # execute testcases
    yield received_hooks
    
    # stop webserver
    server.shutdown()
