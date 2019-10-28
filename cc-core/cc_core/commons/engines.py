import jsonschema
from jsonschema.exceptions import ValidationError

from cc_core.commons.exceptions import EngineError
from cc_core.commons.schemas.engines.container import container_engines
from cc_core.commons.schemas.engines.execution import execution_engines


ENGINES = {
    'container': container_engines,
    'execution': execution_engines
}

DEFAULT_DOCKER_RUNTIME = 'runc'
NVIDIA_DOCKER_RUNTIME = 'nvidia'


def engine_validation(red_data, engine_type, supported, optional=False):
    if engine_type not in ENGINES:
        raise EngineError('invalid engine type "{}"'.format(engine_type))

    if engine_type not in red_data:
        if optional:
            return

        raise EngineError('engine type "{}" required in RED_FILE'.format(engine_type))

    engine = red_data[engine_type]['engine']
    settings = red_data[engine_type]['settings']

    # special case to note that nvidia-docker is not supported anymore
    if engine_type == 'container' and engine == 'nvidia-docker':
        raise EngineError('The nvidia-docker engine is not longer supported. Use the "docker" engine instead.')

    if engine not in supported:
        raise EngineError('{}-engine "{}" not supported'.format(engine_type, engine))

    if engine not in ENGINES[engine_type]:
        raise EngineError('no schema available for {}-engine "{}" in cc_core'.format(engine_type, engine))

    schema = ENGINES[engine_type][engine]
    try:
        jsonschema.validate(settings, schema)
    except ValidationError as e:
        where = '/'.join([str(s) for s in e.absolute_path]) if e.absolute_path else '/'
        raise EngineError(
            '{}-engine "{}" specification in REDFILE does not comply with jsonschema:\n'
            '\tkey in engine settings: {}\n'
            '\treason: {}'
            .format(engine_type, engine, where, e.message)
        )


def engine_to_runtime(engine):
    """
    Returns the docker runtime string depending on which engine is present

    :param engine: On of 'docker' or 'nvidia-docker'
    :return: 'nvidia' for engine=='nvidia-docker', otherwise 'runc'
    """

    runtime = DEFAULT_DOCKER_RUNTIME
    if engine == 'nvidia-docker':
        runtime = NVIDIA_DOCKER_RUNTIME

    return runtime
