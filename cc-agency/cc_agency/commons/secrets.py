import os
import json
from copy import deepcopy
from uuid import uuid4

import requests


_RECEIVE_TIMEOUT = 2000


def separate_secrets_batch(batch):
    batch = deepcopy(batch)
    secrets = {}
    reversed_secrets = {}  # only for deduplication

    for io in ['inputs', 'outputs']:
        for cwl_key, cwl_val in batch[io].items():
            if not isinstance(cwl_val, dict):
                continue
            secret = cwl_val['connector']['access']
            dumped = json.dumps(secret, sort_keys=True)
            if dumped in reversed_secrets:
                key = reversed_secrets[dumped]
            else:
                key = str(uuid4())
                reversed_secrets[dumped] = key
                secrets[key] = secret

            cwl_val['connector']['access'] = key

    return batch, secrets


def separate_secrets_experiment(experiment):
    experiment = deepcopy(experiment)
    secrets = {}
    reversed_secrets = {}  # only for deduplication

    if 'auth' in experiment['container']['settings']['image']:
        key = str(uuid4())
        secret = experiment['container']['settings']['image']['auth']
        experiment['container']['settings']['image']['auth'] = key
        secrets[key] = secret
        reversed_secrets[json.dumps(secret, sort_keys=True)] = key

    return experiment, secrets


def get_batch_secret_keys(batch):
    keys = []
    for io in ['inputs', 'outputs']:
        for cwl_key, cwl_val in batch[io].items():
            if not isinstance(cwl_val, dict):
                continue
            keys.append(cwl_val['connector']['access'])
    return keys


def fill_batch_secrets(batch, secrets):
    batch = deepcopy(batch)
    for io in ['inputs', 'outputs']:
        for cwl_key, cwl_val in batch[io].items():
            if not isinstance(cwl_val, dict):
                continue

            key = cwl_val['connector']['access']
            secret = secrets[key]
            cwl_val['connector']['access'] = secret
    return batch


def get_experiment_secret_keys(experiment):
    keys = []
    if 'auth' in experiment['container']['settings']['image']:
        keys.append(experiment['container']['settings']['image']['auth'])
    return keys


def fill_experiment_secrets(experiment, secrets):
    experiment = deepcopy(experiment)
    if 'auth' in experiment['container']['settings']['image']:
        key = experiment['container']['settings']['image']['auth']
        secret = secrets[key]
        experiment['container']['settings']['image']['auth'] = secret
    return experiment


class TrusteeClient:
    def __init__(self, conf):
        self._url = conf.d['trustee']['internal_url'].rstrip('/')
        self._auth = (conf.d['trustee']['username'], conf.d['trustee']['password'])

    def store(self, secrets):
        r = requests.post(
            '{}/secrets'.format(self._url),
            auth=self._auth,
            json=secrets
        )
        return self._evaluate_request(r)

    def delete(self, keys):
        r = requests.delete(
            '{}/secrets'.format(self._url),
            auth=self._auth,
            json=keys
        )
        return self._evaluate_request(r)

    def collect(self, keys):
        r = requests.get(
            '{}/secrets'.format(self._url),
            auth=self._auth,
            json=keys
        )
        return self._evaluate_request(r)

    def inspect(self):
        r = requests.get(
            '{}/'.format(self._url),
            auth=self._auth
        )
        return self._evaluate_request(r)

    @staticmethod
    def _evaluate_request(r):
        try:
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            debug_info = '{}:{}{}'.format(repr(e), os.linesep, e)
            return {
                'state': 'failed',
                'debug_info': debug_info,
                'disable_retry': False,
                'inspect': True
            }

        return data
