import pytest
import requests
from test_setup import AGENCY_URL, AGENCY_USER, AGENCY_PASSWORD, read_red_file, find_batch_id, setup_agency


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
