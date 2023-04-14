from collections import OrderedDict

from cc_faice.agency.batches.main import main as batches_main
from cc_faice.agency.batch.main import main as batch_main
from cc_faice.agency.experiments.main import main as experiments_main
from cc_faice.agency.experiment.main import main as experiment_main
from cc_faice.agency.nodes.main import main as nodes_main
from cc_faice.agency.user.main import main as user_main

from cc_faice.agency.batches.main import DESCRIPTION as BATCHES_DESCRIPTION
from cc_faice.agency.batch.main import DESCRIPTION as BATCH_DESCRIPTION
from cc_faice.agency.experiments.main import DESCRIPTION as EXPERIMENTS_DESCRIPTION
from cc_faice.agency.experiment.main import DESCRIPTION as EXPERIMENT_DESCRIPTION
from cc_faice.agency.nodes.main import DESCRIPTION as NODES_DESCRIPTION
from cc_faice.agency.user.main import DESCRIPTION as USER_DESCRIPTION

from cc_core.commons.cli_modes import cli_modes
from dotenv import load_dotenv

load_dotenv()


SCRIPT_NAME = 'faice convert'
TITLE = 'modes'
DESCRIPTION = 'Agency utitilies'
MODES = OrderedDict([
    ('batches', {'main': batches_main, 'description': BATCHES_DESCRIPTION}),
    ('batch', {'main': batch_main, 'description': BATCH_DESCRIPTION}),
    ('experiments', {'main': experiments_main,
     'description': EXPERIMENTS_DESCRIPTION}),
    ('experiment', {'main': experiment_main,
     'description': EXPERIMENT_DESCRIPTION}),
    ('nodes', {'main': nodes_main, 'description': NODES_DESCRIPTION}),
    ('create_user', {'main': user_main, 'description': USER_DESCRIPTION})
])


def main():
    cli_modes(SCRIPT_NAME, TITLE, DESCRIPTION, MODES)
