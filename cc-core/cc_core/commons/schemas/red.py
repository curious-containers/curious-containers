from cc_core.commons.schemas import PATTERN_KEY
from cc_core.commons.schemas.cli import cli_schema
from cc_core.commons.schemas.connector import connector_schema
from cc_core.commons.schemas.engine import engine_schema

red_schema = {
    'definitions': {
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
        }
    },
    'oneOf': [{
        'type': 'object',
        'properties': {
            'redVersion': {'type': 'string'},
            'cli': {'$ref': '#/definitions/cli'},
            'inputs': {'$ref': '#/definitions/inputs'},
            'outputs': {'$ref': '#/definitions/outputs'},
            'container': {'$ref': '#/definitions/engine'},
            'execution': {'$ref': '#/definitions/engine'},
            'doc': {'type': 'string'}
        },
        'additionalProperties': False,
        'required': ['redVersion', 'cli', 'inputs', 'container']
    }, {
        'type': 'object',
        'properties': {
            'redVersion': {'type': 'string'},
            'cli': {'$ref': '#/definitions/cli'},
            'batches': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'inputs': {'$ref': '#/definitions/inputs'},
                        'outputs': {'$ref': '#/definitions/outputs'},
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
