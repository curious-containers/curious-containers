from cc_core.commons.schemas import PATTERN_KEY


URL_SCHEME_IDENTIFIER = 'path'

CWL_INPUT_TYPES = ['File', 'Directory', 'string', 'int', 'long', 'float', 'double', 'boolean']
CWL_INPUT_TYPES += ['{}[]'.format(t) for t in CWL_INPUT_TYPES[:]]
CWL_INPUT_TYPES += ['{}?'.format(t) for t in CWL_INPUT_TYPES[:]]

CWL_OUTPUT_TYPES = ['File', 'Directory']
CWL_OUTPUT_TYPES += ['{}?'.format(t) for t in CWL_OUTPUT_TYPES[:]]


cli_schema = {
    'type': 'object',
    'properties': {
        'cwlVersion': {'type': ['string', 'number']},
        'class': {'enum': ['CommandLineTool']},
        'baseCommand': {
            'oneOf': [
                {'type': 'string'},
                {
                    'type': 'array',
                    'items': {'type': 'string'}
                }
            ]
        },
        'inputs': {
            'type': 'object',
            'patternProperties': {
                PATTERN_KEY: {
                    'type': 'object',
                    'properties': {
                        'type': {'enum': CWL_INPUT_TYPES},
                        'inputBinding': {
                            'type': 'object',
                            'properties': {
                                'prefix': {'type': 'string'},
                                'separate': {'type': 'boolean'},
                                'position': {'type': 'integer', 'minimum': 0},
                                'itemSeparator': {'type': 'string'}
                            },
                            'additionalProperties': False,
                        },
                        'doc': {'type': 'string'}
                    },
                    'additionalProperties': False,
                    'required': ['type', 'inputBinding']
                }
            }
        },
        'outputs': {
            'type': 'object',
            'patternProperties': {
                PATTERN_KEY: {
                    'oneOf': [{
                        'type': 'object',
                        'properties': {
                            'type': {'enum': CWL_OUTPUT_TYPES},
                            'outputBinding': {
                                'type': 'object',
                                'properties': {
                                    'glob': {'type': 'string'},
                                },
                                'additionalProperties': False,
                                'required': ['glob']
                            },
                            'doc': {'type': 'string'}
                        },
                        'additionalProperties': False,
                        'required': ['type', 'outputBinding']
                    }, {
                        'type': 'object',
                        'properties': {
                            'type': {'enum': ['stdout', 'stderr']},
                        },
                        'additionalProperties': False,
                        'required': ['type']
                    }]
                }
            }
        },
        'stdout': {'type': 'string'},
        'stderr': {'type': 'string'},
        'doc': {'type': 'string'}
    },
    'additionalProperties': False,
    'required': ['cwlVersion', 'class', 'baseCommand', 'inputs', 'outputs']
}
