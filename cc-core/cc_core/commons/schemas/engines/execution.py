from cc_core.commons.schemas.auth import auth_schema

ccfaice_schema = {
    'type': 'object',
    'properties': {},
    'additionalProperties': False
}

ccagency_schema = {
    'definitions': {
        'auth': auth_schema
    },
    'type': 'object',
    'properties': {
        'access': {
            'type': 'object',
            'properties': {
                'url': {'type': 'string'},
                'auth': {'$ref': '#/definitions/auth'}
            },
            'additionalProperties': False,
            'required': ['url']
        },
        'retryIfFailed': {'type': 'boolean'},
        'batchConcurrencyLimit': {'type': 'integer', 'minimum': 1}
        # disablePull might be data breach, if another users image has been pulled to host already
        # 'disablePull': {'type': 'boolean'}
    },
    'additionalProperties': False
}

execution_engines = {
    'ccfaice': ccfaice_schema,
    'ccagency': ccagency_schema
}
