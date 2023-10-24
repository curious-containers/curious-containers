import pytest
import requests
from test_setup import AGENCY_URL, AGENCY_USER, AGENCY_PASSWORD, read_red_file, setup_agency


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


def test_red_format_error(setup_agency):
    red_json = read_red_file('red/format_error.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    statuscode = response.status_code
    
    assert statuscode == 400
