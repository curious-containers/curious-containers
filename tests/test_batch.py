import pytest
import requests
from test_setup import AGENCY_URL, AGENCY_USER, AGENCY_PASSWORD, OUTPUT_DIR, read_red_file, \
    find_batch_id, find_batch_ids, fetch_final_batch_state, setup_agency


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


def test_batch_status_failed(setup_agency):
    red_json = read_red_file('red/failed.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    final_state = fetch_final_batch_state(batch_id)
    
    assert final_state == 'failed'


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
    with open(f"{OUTPUT_DIR}/count_results.txt", 'r') as file:
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


def test_batch_stdout(setup_agency):
    red_json = read_red_file('red/stdout.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    fetch_final_batch_state(batch_id)
    
    stdout_response = requests.get(AGENCY_URL + '/batches/' + batch_id + '/stdout', auth=(AGENCY_USER, AGENCY_PASSWORD))
    stdout = stdout_response.text
    
    assert stdout == 'test\n'
