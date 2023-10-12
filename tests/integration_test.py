import pytest
import subprocess
import requests
import time
import json
import re
import os
import shutil

DOCKER_HOST_IP = '172.17.0.1'
SSH_PORT = '2222'
SSH_USER = 'test_user'
SSH_PASSWORD = 'test_password'

AGENCY_URL = 'http://localhost:8080'
AGENCY_USER = 'agency_user'
AGENCY_PASSWORD = 'agency_password'
MAX_RETRIES = 48
RETRY_DELAY = 5

OUTPUT_DIR = './output'


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
    try:
        os.mkdir(OUTPUT_DIR)
    except FileExistsError:
        pass
    
    subprocess.Popen(["sh", "build_docker_images.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    subprocess.Popen(["sh", "start_agency.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    wait_for_agency_boot_up()

    # execute testcases
    yield

    # teardown cc-agency
    subprocess.Popen(["sh", "stop_agency.sh"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    shutil.rmtree(OUTPUT_DIR)


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
    
    batch_id = find_batch_id(response)
    
    assert not batch_id == ''


def test_experiment_batch_by_id(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    
    batch_response = requests.get(AGENCY_URL + '/batches/' + batch_id, auth=(AGENCY_USER, AGENCY_PASSWORD))
    batch_data = batch_response.json()
    response_batch_id = batch_data['_id']
    
    assert response_batch_id == batch_id


@pytest.mark.parametrize(
    'red_file',
    [
        'red/minimal.json',
        'red/input_output.json',
        'red/stdout.json'
    ]
)
def test_batch_status_succeeded(setup_agency, red_file):
    red_json = read_red_file(red_file)
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    final_state = fetch_final_batch_state(batch_id)
    
    assert final_state == 'succeeded'


def test_batch_status_cancelled(setup_agency):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    
    requests.delete(AGENCY_URL + '/batches/' + batch_id, auth=(AGENCY_USER, AGENCY_PASSWORD)).json()
    final_state = fetch_final_batch_state(batch_id)
    
    assert final_state == 'cancelled'


def test_input_output_connector(setup_agency):
    red_json = read_red_file('red/input_output.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    fetch_final_batch_state(batch_id)
    
    results = ''
    expected_results = ''
    with open('output/count_results.txt', 'r') as file:
        results = file.read()
    with open('input/count_results.txt', 'r') as file:
        expected_results = file.read()
    
    assert results == expected_results


def test_experiment_with_multiple_batches(setup_agency):
    red_json = read_red_file('red/multi_batch.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_ids = find_batch_ids(response)
    final_states = []
    for batch_id in batch_ids:
        final_states.append(fetch_final_batch_state(batch_id))
    
    assert final_states == ['succeeded', 'succeeded', 'succeeded']


def test_experiment_stdout(setup_agency):
    red_json = read_red_file('red/stdout.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    fetch_final_batch_state(batch_id)
    
    stdout_response = requests.get(AGENCY_URL + '/batches/' + batch_id + '/stdout', auth=(AGENCY_USER, AGENCY_PASSWORD))
    stdout = stdout_response.text
    
    assert stdout == 'test\n'


def test_red_format_error(setup_agency):
    red_json = read_red_file('red/format_error.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    statuscode = response.status_code
    
    assert statuscode == 400
