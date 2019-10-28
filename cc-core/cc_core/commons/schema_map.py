from collections import OrderedDict

from cc_core.commons.schemas.red import red_schema
from cc_core.commons.schemas.engines.container import container_engines
from cc_core.commons.schemas.engines.execution import execution_engines


schemas = OrderedDict([
    ('red', red_schema)
])

for e, s in container_engines.items():
    schemas['red-engine-container-{}'.format(e)] = s

for e, s in execution_engines.items():
    schemas['red-engine-execution-{}'.format(e)] = s
