auth_schema = {
    'oneOf': [{
        'type': 'object',
        'properties': {
            'username': {'type': 'string'},
            'password': {'type': 'string'}
        },
        'additionalProperties': False,
        'required': ['username', 'password']
    }, {
        'type': 'object',
        'properties': {
            '_username': {'type': 'string'},
            'password': {'type': 'string'}
        },
        'additionalProperties': False,
        'required': ['_username', 'password']
    }, {
        'type': 'object',
        'properties': {
            'username': {'type': 'string'},
            '_password': {'type': 'string'}
        },
        'additionalProperties': False,
        'required': ['username', '_password']
    }, {
        'type': 'object',
        'properties': {
            '_username': {'type': 'string'},
            '_password': {'type': 'string'}
        },
        'additionalProperties': False,
        'required': ['_username', '_password']
    }]
}
