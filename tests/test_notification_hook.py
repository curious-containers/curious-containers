import pytest
import requests
import unittest
import time
from test_setup import AGENCY_URL, AGENCY_USER, AGENCY_PASSWORD, read_red_file, find_batch_id, \
    find_batch_ids, fetch_final_batch_state, setup_agency, notification_webserver

@pytest.mark.parametrize(
    'red_file',
    [
        'red/minimal.json',
        'red/failed.json',
    ]
)
def test_notification_hooks(setup_agency, notification_webserver, red_file):
    red_json = read_red_file(red_file)
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    final_state = fetch_final_batch_state(batch_id)
    time.sleep(70)
    
    expected_notification = [{
        'batches': [{
            'batchId': batch_id,
            'state': final_state
        }]
    }]
    
    assert notification_webserver == expected_notification


def test_notification_hooks_cancelled(setup_agency, notification_webserver):
    red_json = read_red_file('red/minimal.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_id = find_batch_id(response)
    
    requests.delete(AGENCY_URL + '/batches/' + batch_id, auth=(AGENCY_USER, AGENCY_PASSWORD)).json()
    fetch_final_batch_state(batch_id)
    time.sleep(70)
    
    expected_notification = [{
        'batches': [{
            'batchId': batch_id,
            'state': 'cancelled'
        }]
    }]
    
    assert notification_webserver == expected_notification


def test_notification_hooks_batches(setup_agency, notification_webserver):
    case = unittest.TestCase()
    red_json = read_red_file('red/multi_batch.json')
    response = requests.post(AGENCY_URL + '/red', auth=(AGENCY_USER, AGENCY_PASSWORD), json=red_json)
    
    batch_ids = find_batch_ids(response)
    expected_notifications = [{'batches': []}]
    for batch_id in batch_ids:
        expected_notifications[0]['batches'].append({
            'batchId': batch_id,
            'state': fetch_final_batch_state(batch_id)
        })
    time.sleep(70)
    
    case.assertCountEqual(notification_webserver, expected_notifications)
