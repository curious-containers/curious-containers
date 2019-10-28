from collections import OrderedDict

from cc_faice.version import VERSION
from cc_faice.commons.compatibility import version_validation

from cc_faice.agent.main import main as agent_main
from cc_faice.exec.main import main as exec_main
from cc_faice.schema.main import main as schema_main
from cc_faice.convert.main import main as convert_main

from cc_faice.agent.main import DESCRIPTION as AGENT_DESCRIPTION
from cc_faice.exec.main import DESCRIPTION as EXEC_DESCRIPTION
from cc_faice.schema.main import DESCRIPTION as SCHEMA_DESCRIPTION
from cc_faice.convert.main import DESCRIPTION as CONVERT_DESCRIPTION

from cc_core.commons.cli_modes import cli_modes


SCRIPT_NAME = 'faice'
TITLE = 'tools'
DESCRIPTION = 'FAICE Copyright (C) 2018  Christoph Jansen. This software is distributed under the AGPL-3.0 ' \
              'LICENSE and is part of the Curious Containers project (https://www.curious-containers.cc).'
MODES = OrderedDict([
    ('agent', {'main': agent_main, 'description': AGENT_DESCRIPTION}),
    ('exec', {'main': exec_main, 'description': EXEC_DESCRIPTION}),
    ('schema', {'main': schema_main, 'description': SCHEMA_DESCRIPTION}),
    ('convert', {'main': convert_main, 'description': CONVERT_DESCRIPTION}),
])


def main():
    version_validation()
    cli_modes(SCRIPT_NAME, TITLE, DESCRIPTION, MODES, VERSION)
