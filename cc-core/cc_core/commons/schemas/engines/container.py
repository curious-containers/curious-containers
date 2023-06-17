from cc_core.commons.schemas.auth import auth_schema


docker_schema = {
    'definitions': {
        'auth': auth_schema,
        'image': {
            'type': 'object',
            'properties': {
                'url': {'type': 'string'},
                'auth': {'$ref': '#/definitions/auth'},
                'source': {
                    'type': 'object',
                    'properties': {
                        'url': {'type': 'string'}
                    },
                    'additionalProperties': False,
                    'required': ['url']
                }
            },
            'additionalProperties': False,
            'required': ['url']
        },
        'vendors': {'enum': ['nvidia']},
        'ram': {'type': 'integer', 'minimum': 256},
        'gpus': {
            'oneOf': [
                {
                    'type': 'object',
                    'properties': {
                        'vendor': {'$ref': '#/definitions/vendors'},
                        'count': {'type': 'integer'},
                    },
                    'additionalProperties': False,
                    'required': ['vendor', 'count']
                },
                {
                    'type': 'object',
                    'properties': {
                        'vendor': {'$ref': '#/definitions/vendors'},
                        'devices': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'properties': {
                                    'vramMin': {'$ref': '#/definitions/ram'}
                                },
                                'additionalProperties': False
                            }
                        }
                    },
                    'additionalProperties': False,
                    'required': ['vendor', 'devices']
                }
            ]
        }
    },
    'type': 'object',
    'properties': {
        'version': {'type': 'string'},
        'image': {'$ref': '#/definitions/image'},
        'gpus': {'$ref': '#/definitions/gpus'},
        'ram': {'$ref': '#/definitions/ram'}
    },
    'additionalProperties': False,
    'required': ['image']
}


container_engines = {
    'docker': docker_schema,
}
