# TODO: Incomplete
agent_result_schema = {
    'type': 'object',
    'properties': {
        'state': {'enum': ['succeeded', 'failed']}
    },
    'additionalProperties': True,
    'required': ['state']
}
