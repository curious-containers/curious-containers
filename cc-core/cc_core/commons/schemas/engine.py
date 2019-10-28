engine_schema = {
    'type': 'object',
    'properties': {
        'engine': {'type': 'string'},
        'settings': {'type': 'object'},
        'doc': {'type': 'string'}
    },
    'additionalProperties': False,
    'required': ['engine', 'settings']
}
