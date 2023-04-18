agent_result_schema = {
    'type': 'object',
    'properties': {
        'command': {
            'type': 'array',
            'items': {'type': 'string'}
        },
        'returnCode': {'type': ['integer', 'null']},
        'executed': {'type': 'boolean'},
        'stdout': {'type': ['string', 'null']},
        'stderr': {'type': ['string', 'null']},
        'debugInfo': {
            'oneOf': [
                {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                {'type': 'null'}
            ]
        },
        'inputs': {
            'oneOf': [
                {'type': 'object'},  # this could be more precise
                {'type': 'null'}
            ]
        },
        'outputs': {
            'oneOf': [
                {'type': 'object'},  # this could be more precise
                {'type': 'null'}
            ]
        },
        'state': {'enum': ['succeeded', 'failed']}
    },
    'required': ['command','inputs', 'outputs', 'state']
}
inputconnector_result_schema = {
    'type': 'object',
    'properties': {
        'debugInfo': {
            'oneOf': [
                {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                {'type': 'null'}
            ]
        },
        'inputs': {
            'oneOf': [
                {'type': 'object'},  # this could be more precise
                {'type': 'null'}
            ]
        },
        'state': {'enum': ['succeeded', 'failed']}
    },
    'required': ['inputs', 'state']
}
outputconnector_result_schema = {
    'type': 'object',
    'properties': {
        'debugInfo': {
            'oneOf': [
                {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                {'type': 'null'}
            ]
        },
        'outputs': {
            'oneOf': [
                {'type': 'object'},  # this could be more precise
                {'type': 'null'}
            ]
        },
        'state': {'enum': ['succeeded', 'failed']}
    },
    'required': ['outputs', 'state']
}
