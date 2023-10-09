import pytest
import subprocess
import requests
import time


AGENCY_URL = 'http://localhost:8080'
AGENCY_USER = 'agency_user'
AGENCY_PASSWORD = 'agency_password'
MAX_RETRIES = 5
RETRY_DELAY = 5


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


def test_broker_authorized(setup_agency):
    response = requests.get(AGENCY_URL + '/version', auth=(AGENCY_USER, AGENCY_PASSWORD))
    statuscode = response.status_code
    assert statuscode == 200


def test_broker_unauthorized(setup_agency):
    response = requests.get(AGENCY_URL + '/version')
    statuscode = response.status_code
    assert statuscode == 401
