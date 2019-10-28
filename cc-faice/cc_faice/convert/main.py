from collections import OrderedDict

from cc_faice.convert.batches.main import main as batches_main
from cc_faice.convert.format.main import main as format_main
from cc_faice.convert.cwl.main import main as cwl_main

from cc_faice.convert.batches.main import DESCRIPTION as BATCHES_DESCRIPTION
from cc_faice.convert.format.main import DESCRIPTION as FORMAT_DESCRIPTION
from cc_faice.convert.cwl.main import DESCRIPTION as CWL_DESCRIPTION

from cc_core.commons.cli_modes import cli_modes


SCRIPT_NAME = 'faice convert'
TITLE = 'modes'
DESCRIPTION = 'File conversion utilities.'
MODES = OrderedDict([
    ('batches', {'main': batches_main, 'description': BATCHES_DESCRIPTION}),
    ('format', {'main': format_main, 'description': FORMAT_DESCRIPTION}),
    ('cwl', {'main': cwl_main, 'description': CWL_DESCRIPTION}),
])


def main():
    cli_modes(SCRIPT_NAME, TITLE, DESCRIPTION, MODES)
