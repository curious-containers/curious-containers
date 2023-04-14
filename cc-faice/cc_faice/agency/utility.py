import pprint
from datetime import datetime


AUTHENTICATION = {
    'localhost': ('agency_user', 'agency_password'),
    'souvemed-agency.f4.htw-berlin.de': ('Bruno', 'HeyHeyTest'),
}

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    GRAY = '\033[90m'
    YELLOW = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
        

def _print_experiments(experiments):
    format_string = '{expid:<24} | {start:<19} | {user:<10} '
    title_line = format_string.format(expid='ID', start='Start', user='User')
    print(title_line)
    print('-' * len(title_line))
    for experiment in experiments:
        start_time = _format_timestamp(experiment['registrationTime'])
        line = format_string.format(
            expid=experiment['_id'], start=start_time, user=experiment['username'])
        print(line)


def _format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%d.%m.%Y %H:%M:%S')


def _color_state(state):
    if state == 'succeeded':
        state = bcolors.OKGREEN + state + bcolors.ENDC + ' '
    elif state == 'failed':
        state = bcolors.FAIL + state + bcolors.ENDC + '    '
    elif state == 'cancelled':
        state = bcolors.GRAY + state + bcolors.ENDC + ' '
    elif state == 'scheduled':
        state = bcolors.OKCYAN + state + bcolors.ENDC + ' '
    elif state == 'registered':
        state = bcolors.OKBLUE + state + bcolors.ENDC
    elif state == 'processing':
        state = bcolors.YELLOW + state + bcolors.ENDC
    return state


def _print_batches(batches):
    format_string = '{batchid:<24} | {state:<10} | {experiment:<24} | {start:<19} | {node:<10} | {user:<10} '
    title_line = format_string.format(
        batchid='ID', state='State', experiment='ExpID', start='Start', node='Node', user='User')
    print(title_line)
    print('-' * len(title_line))
    for batch in batches:
        state = _color_state(batch['state'])
        start_time = _format_timestamp(batch['registrationTime'])
        node = batch['node']
        if node is None:
            node = ''
        line = format_string.format(
            batchid=batch['_id'], state=state, experiment=batch['experimentId'], start=start_time, node=node, user=batch['username'])
        print(line)


def _print_nodes(nodes):
    format_string = '{name:<10} | {state:<8} | {cpus:<4} | {ram:<5} | {gpus:<4} | {batches:<7} '
    title_line = format_string.format(
        name='Name', state='State', cpus='CPUs', ram='RAM', gpus='GPUs', batches='Batches')
    print(title_line)
    print('-' * len(title_line))
    for node in nodes:
        gpus = len(node['gpus']) if node['gpus'] is not None else 0
        state = node['state']
        if state == 'online':
            state = bcolors.OKGREEN + state + bcolors.ENDC + '  '
        if state == 'offline':
            state = bcolors.FAIL + state + bcolors.ENDC + ' '
        print(format_string.format(name=node['nodeName'], state=state, cpus=node['cpus'],
              ram=node['ram'], gpus=gpus, batches=len(node['currentBatches'])))


def _print_experiment(experiment):
    print('experiment-id:', experiment['_id'])
    print('base-command :', experiment['cli']['baseCommand'])
    print('image        :',
          experiment['container']['settings']['image']['url'])
    print('start time   :', _format_timestamp(experiment['registrationTime']))
    print('user         :', experiment['username'])


def _print_batch(batch):
    start_time = _format_timestamp(batch['registrationTime'])
    print('batch-id     :', batch['_id'])
    print('experiment-id:', batch['experimentId'])
    print('state        :', _color_state(batch['state']))
    print('start time   :', start_time)
    print('node         :', batch['node'])
    print('history      : ', end='')
    debug_info = None
    for index, hist_entry in enumerate(batch['history']):
        if index != 0:
            print(' ' * 15, end='')
        print(_format_timestamp(hist_entry['time']), '  ', _color_state(
            hist_entry['state']), sep='')

        if hist_entry['debugInfo']:
            debug_info = hist_entry['debugInfo']

    if debug_info:
        print('debug_info:')
        print(debug_info)


# maps (request_type, has_id) to print function
TO_PRINT_FUNC = {
    ('experiments', False): _print_experiments,
    ('experiments', True): _print_experiment,
    ('batches', False): _print_batches,
    ('batches', True): _print_batch,
    ('nodes', False): _print_nodes,
}

def getAuth(agency_url):
    auth = None
    for host, a in AUTHENTICATION.items():
        if host in agency_url:
            auth = a
            break
    return auth
    
def show(args, request_type, r):
    if hasattr(args, 'show_file') and args.show_file is not None:
            print(r.content.decode('utf-8'))
    else:
        if args.raw:
            pprint.pprint(r.json())
        else:
            if hasattr(args, 'id'):
                print_function = TO_PRINT_FUNC[(
                    request_type, args.id is not None)]
            else:
                print_function = TO_PRINT_FUNC[(
                    request_type, False)]
            print_function(r.json())
