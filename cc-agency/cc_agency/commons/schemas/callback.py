agent_result_schema = {
    'type': 'object',
    'properties': {
        'command': {
            'type': 'array',
            'items': {'type': 'string'}
        },
        'process': {
            'type': 'object',
            'properties': {
                'returnCode': {'type': ['integer', 'null']},
                'executed': {'type': 'boolean'},
                'stdout': {'type': ['string', 'null']},
                'stderr': {'type': ['string', 'null']}
            },
            'additionalProperties': False
        },
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
    'required': ['command', 'process', 'inputs', 'outputs', 'state']
}
