import pytest
import subprocess
import requests
import time
import json
import re

DOCKER_HOST_IP = '172.17.0.1'
SSH_USER = 'test_user'
SSH_PASSWORD = 'test_password'

AGENCY_URL = 'http://localhost:8080'
AGENCY_USER = 'agency_user'
AGENCY_PASSWORD = 'agency_password'
MAX_RETRIES = 20
RETRY_DELAY = 5


def read_red_file(filename):
    with open(filename, 'r') as f:
        red_file = f.read()
        
    red_file = re.sub('{{agency_url}}', AGENCY_URL, red_file)
    red_file = re.sub('{{agency_user}}', AGENCY_USER, red_file)
    red_file = re.sub('{{agency_password}}', AGENCY_PASSWORD, red_file)
    red_file = re.sub('{{host_ip}}', DOCKER_HOST_IP, red_file)
    red_file = re.sub('{{ssh_username}}', SSH_USER, red_file)
    red_file = re.sub('{{ssh_password}}', SSH_PASSWORD, red_file)
    
    return json.loads(red_file)


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
    
    return current_state


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
    subprocess.Popen(["sh", "build_docker_images.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    subprocess.Popen(["sh", "start_agency.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    wait_for_agency_boot_up()

    # execute testcases
    yield

    # teardown cc-agency
    subprocess.Popen(["sh", "stop_agency.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()


def test_broker_no_auth_required(setup_agency):
    response = requests.get(AGENCY_URL)
    statuscode = response.status_code
    assert statuscode == 200


@pytest.mark.parametrize(
    'endpoint',
    [
        '/version',
        '/experiments/count',
        '/experiments',
        '/batches/count',
        '/batches',
        '/nodes'
    ]
)
def test_broker_endpoints_authorized(setup_agency, endpoint):
    response = requests.get(AGENCY_URL + endpoint, auth=(AGENCY_USER, AGENCY_PASSWORD))
    statuscode = response.status_code
    assert statuscode == 200


@pytest.mark.parametrize(
    'endpoint',
    [
        '/version',
        '/experiments/count',
        '/experiments',
        '/batches/count',
        '/batches',
        '/nodes'
    ]
)
def test_broker_endpoints_unauthorized(setup_agency, endpoint):
    response = requests.get(AGENCY_URL + endpoint)
    statuscode = response.status_code
    assert statuscode == 401


def test_broker_endpoints_red_authorized(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    statuscode = response.status_code
    assert statuscode == 200


def test_broker_no_valid_experiment(setup_agency):
    response = requests.get(AGENCY_URL + '/experiments/' + '651e', auth=(AGENCY_USER, AGENCY_PASSWORD))
    statuscode = response.status_code
    assert statuscode == 400


def test_broker_no_valid_batch(setup_agency):
    response = requests.get(AGENCY_URL + '/batches/' + '651e', auth=(AGENCY_USER, AGENCY_PASSWORD))
    statuscode = response.status_code
    assert statuscode == 400


def test_broker_endpoints_red_unauthorized(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', json=red_json)
    
    statuscode = response.status_code
    assert statuscode == 401


def test_start_experiment_response(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    data = response.json()
    experiment_id = data['experimentId']
    
    assert not experiment_id == ''


def test_broker_experiments_by_id(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    data = response.json()
    experiment_id = data['experimentId']
    exp_response = requests.get(AGENCY_URL + '/experiments/' + experiment_id, auth=(AGENCY_USER, AGENCY_PASSWORD))
    exp_data = exp_response.json()
    response_experiment_id = exp_data['_id']
    
    assert response_experiment_id == experiment_id


def test_experiment_batch_exists(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    data = response.json()
    experiment_id = data['experimentId']
    batches = requests.get(AGENCY_URL + '/batches', auth=(AGENCY_USER, AGENCY_PASSWORD)).json()
    
    batch_id = ''
    for batch in batches:
        if batch['experimentId'] == experiment_id:
            batch_id = batch['_id']
            break
    
    assert not batch_id == ''


def test_experiment_batch_by_id(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    data = response.json()
    experiment_id = data['experimentId']
    batches = requests.get(AGENCY_URL + '/batches', auth=(AGENCY_USER, AGENCY_PASSWORD)).json()
    
    batch_id = ''
    for batch in batches:
        if batch['experimentId'] == experiment_id:
            batch_id = batch['_id']
            break
    batch_response = requests.get(AGENCY_URL + '/batches/' + batch_id, auth=(AGENCY_USER, AGENCY_PASSWORD))
    batch_data = batch_response.json()
    response_batch_id = batch_data['_id']
    
    assert response_batch_id == batch_id


def test_batch_status_succeeded(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    data = response.json()
    experiment_id = data['experimentId']
    batches = requests.get(AGENCY_URL + '/batches', auth=(AGENCY_USER, AGENCY_PASSWORD)).json()
    
    batch_id = ''
    for batch in batches:
        if batch['experimentId'] == experiment_id:
            batch_id = batch['_id']
            break
    
    final_state = fetch_final_batch_state(batch_id)
    
    assert final_state == 'succeeded'


def test_batch_status_cancelled(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    data = response.json()
    experiment_id = data['experimentId']
    batches = requests.get(AGENCY_URL + '/batches', auth=(AGENCY_USER, AGENCY_PASSWORD)).json()
    
    batch_id = ''
    for batch in batches:
        if batch['experimentId'] == experiment_id:
            batch_id = batch['_id']
            break
    
    batches = requests.delete(AGENCY_URL + '/batches/' + batch_id, auth=(AGENCY_USER, AGENCY_PASSWORD)).json()
    final_state = fetch_final_batch_state(batch_id)
    
    assert final_state == 'cancelled'
