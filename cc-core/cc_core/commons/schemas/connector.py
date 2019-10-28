connector_schema = {
    'type': 'object',
    'properties': {
        'command': {'type': 'string'},
        'access': {'type': 'object'},
        'mount': {'type': 'boolean'},
        'doc': {'type': 'string'}
    },
    'additionalProperties': False,
    'required': ['command', 'access']
}