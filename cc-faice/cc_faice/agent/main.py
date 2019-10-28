from collections import OrderedDict

from cc_faice.agent.red.main import main as red_main
from cc_faice.agent.red.main import DESCRIPTION as RED_DESCRIPTION

from cc_core.commons.cli_modes import cli_modes


SCRIPT_NAME = 'faice agent'
TITLE = 'modes'
DESCRIPTION = 'Run a RED experiment.'
MODES = OrderedDict([
    ('red', {'main': red_main, 'description': RED_DESCRIPTION}),
])


def main():
    cli_modes(SCRIPT_NAME, TITLE, DESCRIPTION, MODES)
