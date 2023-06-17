conf_schema = {
    'type': 'object',
    'properties': {
        'broker': {
            'type': 'object',
            'properties': {
                'auth': {
                    'type': 'object',
                    'properties': {
                        'num_login_attempts': {'type': 'integer'},
                        'block_for_seconds': {'type': 'integer'},
                        'tokens_valid_for_seconds': {'type': 'integer'}
                    },
                    'additionalProperties': False,
                    'required': ['num_login_attempts', 'block_for_seconds', 'tokens_valid_for_seconds']
                }
            },
            'additionalProperties': False,
            'required': ['auth']
        },
        'controller': {
            'type': 'object',
            'properties': {
                'bind_socket_path': {'type': 'string'},
                'docker': {
                    'type': 'object',
                    'properties': {
                        'nodes': {
                            'type': 'object',
                            'patternProperties': {
                                '^[a-zA-Z0-9_-]+$': {
                                    'type': 'object',
                                    'properties': {
                                        'base_url': {'type': 'string'},
                                        'tls': {
                                            'type': 'object',
                                            'properties': {
                                                'verify': {'type': 'string'},
                                                'client_cert': {
                                                    'type': 'array',
                                                    'items': {'type': 'string'}
                                                },
                                                'assert_hostname': {'type': ['boolean', 'string']}
                                            },
                                            'additionalProperties': True
                                        },
                                        'environment': {
                                            'type': 'object',
                                            'patternProperties': {
                                                '^[a-zA-Z0-9_-]+$': {'type': 'string'}
                                            },
                                            'additionalProperties': False
                                        },
                                        'network': {'type': 'string'},
                                        'hardware': {
                                            'type': 'object',
                                            'properties': {
                                                'gpu_blacklist': {
                                                    'type': 'array',
                                                    'items': {'type': 'integer'}
                                                }
                                            },
                                            'additionalProperties': False
                                        }
                                    },
                                    'required': ['base_url'],
                                    'additionalProperties': False
                                }
                            },
                            'additionalProperties': False
                        },
                        'allow_insecure_capabilities': {'type': 'boolean'},
                        'image_prune_duration': {'type': 'number'}
                    },
                    'additionalProperties': False,
                    'required': ['nodes']
                },
                'notification_hooks': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'url': {'type': 'string'},
                            'auth': {
                                'type': 'object',
                                'properties': {
                                    'username': {'type': 'string'},
                                    'password': {'type': 'string'}
                                },
                                'additionalProperties': False,
                                'required': ['username', 'password']
                            }
                        },
                        'additionalProperties': False,
                        'required': ['url']
                    }
                }
            },
            'additionalProperties': False,
            'required': ['bind_socket_path', 'docker']
        },
        'trustee': {
            'type': 'object',
            'properties': {
                'internal_url': {'type': 'string'},
                'username': {'type': 'string'},
                'password': {'type': 'string'}
            },
            'additionalProperties': False,
            'required': ['internal_url', 'username', 'password']
        },
        'mongo': {
            'type': 'object',
            'properties': {
                'host': {'type': 'string'},
                'port': {'type': 'integer'},
                'db': {'type': 'string'},
                'username': {'type': 'string'},
                'password': {'type': 'string'}
            },
            'additionalProperties': False,
            'required': ['db', 'username', 'password']
        }
    },
    'additionalProperties': False,
    'required': ['broker', 'controller', 'trustee', 'mongo']
}
