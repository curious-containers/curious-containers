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
                'returnCode': {'type': 'integer'},
                'executed': {'type': 'boolean'},
                'stdout': {'type': 'string'},
                'stderr': {'type': 'string'}
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
        'inputs': {'type': 'object'},  # this could be more precise
        'outputs': {'type': 'object'},  # this could be more precise
        'state': {'enum': ['succeeded', 'failed']}
    },
    'additionalProperties': True,
    'required': ['command', 'process', 'inputs', 'outputs', 'state']
}
