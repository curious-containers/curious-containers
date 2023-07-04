from red_val.schemas.cli import cli_schema, PATTERN_KEY
from red_val.schemas.connector import connector_schema
from red_val.schemas.engine import engine_schema

red_schema = {
    'definitions': {
        'redVersion': {'enum': ['9']},
        'cli': cli_schema,
        'connector': connector_schema,
        'engine': engine_schema,
        'listingFile': {
            'type': 'object',
            'properties': {
                'class': {'enum': ['File']},
                'basename': {'type': 'string'},
                'checksum': {'type': 'string'},
                'size': {'type': 'integer'}
            },
            'required': ['class', 'basename'],
            'additionalProperties': False
        },
        'listingDirectory': {
            'type': 'object',
            'properties': {
                'class': {'enum': ['Directory']},
                'basename': {'type': 'string'},
                'listing': {'$ref': '#/definitions/listing'}
            },
            'additionalProperties': False,
            'required': ['class', 'basename']
        },
        'listing': {
            'type': 'array',
            'items': {
                'oneOf': [{'$ref': '#/definitions/listingFile'}, {'$ref': '#/definitions/listingDirectory'}]
            }
        },
        'inputFile': {
            'type': 'object',
            'properties': {
                'class': {'enum': ['File']},
                'connector': {'$ref': '#/definitions/connector'},
                'basename': {'type': 'string'},
                'dirname': {'type': 'string'},
                'checksum': {'type': 'string'},
                'size': {'type': 'integer'},
                'doc': {'type': 'string'}
            },
            'additionalProperties': False,
            'required': ['class', 'connector']
        },
        'inputDirectory': {
            'type': 'object',
            'properties': {
                'class': {'enum': ['Directory']},
                'connector': {'$ref': '#/definitions/connector'},
                'basename': {'type': 'string'},
                'listing': {'$ref': '#/definitions/listing'},
                'doc': {'type': 'string'}
            },
            'additionalProperties': False,
            'required': ['class', 'connector']
        },
        'outputFile': {
            'type': 'object',
            'properties': {
                'class': {'enum': ['File', 'stdout', 'stderr']},
                'checksum': {'type': 'string'},
                'size': {'type': 'integer'},
                'connector': {'$ref': '#/definitions/connector'},
                'doc': {'type': 'string'}
            },
            'additionalProperties': False,
            'required': ['class', 'connector']
        },
        'outputDirectory': {
            'type': 'object',
            'properties': {
                'class': {'enum': ['Directory']},
                'connector': {'$ref': '#/definitions/connector'},
                'listing': {'$ref': '#/definitions/listing'},
                'doc': {'type': 'string'}
            },
            'additionalProperties': False,
            'required': ['class', 'connector']
        },
        'inputs': {
            'type': 'object',
            'patternProperties': {
                PATTERN_KEY: {
                    'anyOf': [
                        {'type': 'string'},
                        {'type': 'number'},
                        {'type': 'boolean'},
                        {
                            'type': 'array',
                            'items': {
                                'oneOf': [
                                    {'type': 'string'},
                                    {'type': 'number'},
                                    {'type': 'boolean'},
                                    {'$ref': '#/definitions/inputFile'},
                                    {'$ref': '#/definitions/inputDirectory'}
                                ]
                            }
                        },
                        {'$ref': '#/definitions/inputFile'},
                        {'$ref': '#/definitions/inputDirectory'}
                    ]
                }
            },
            'additionalProperties': False
        },
        'outputs': {
            'type': 'object',
            'patternProperties': {
                PATTERN_KEY: {
                    'anyOf': [
                        {'$ref': '#/definitions/outputFile'},
                        {'$ref': '#/definitions/outputDirectory'}
                    ]
                }
            },
            'additionalProperties': False
        },
        'cloud': {
            'type': 'object',
            'properties': {
                'enable': {'type': 'boolean'},
                'mountProtocol': {'enum': ['ssh']},
                'mountDir': {'type': 'string'}
            },
            'additionalProperties': False,
            'required': ['enable', 'mountDir']
        }
    },
    'oneOf': [{
        'type': 'object',
        'properties': {
            'redVersion': {'$ref': '#/definitions/redVersion'},
            'cli': {'$ref': '#/definitions/cli'},
            'inputs': {'$ref': '#/definitions/inputs'},
            'outputs': {'$ref': '#/definitions/outputs'},
            'cloud': {'$ref': '#/definitions/cloud'},
            'container': {'$ref': '#/definitions/engine'},
            'execution': {'$ref': '#/definitions/engine'},
            'doc': {'type': 'string'}
        },
        'additionalProperties': False,
        'required': ['redVersion', 'cli', 'inputs', 'container']
    }, {
        'type': 'object',
        'properties': {
            'redVersion': {'$ref': '#/definitions/redVersion'},
            'cli': {'$ref': '#/definitions/cli'},
            'batches': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'inputs': {'$ref': '#/definitions/inputs'},
                        'outputs': {'$ref': '#/definitions/outputs'},
                        'cloud': {'$ref': '#/definitions/cloud'},
                        'doc': {'type': 'string'}
                    },
                    'additionalProperties': False,
                    'required': ['inputs']
                }
            },
            'container': {'$ref': '#/definitions/engine'},
            'execution': {'$ref': '#/definitions/engine'},
            'doc': {'type': 'string'}
        },
        'additionalProperties': False,
        'required': ['redVersion', 'cli', 'batches', 'container']
    }]
}
