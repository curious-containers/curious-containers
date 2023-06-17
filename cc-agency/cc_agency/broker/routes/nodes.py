from flask import request
from bson.objectid import ObjectId

from cc_agency.commons.helper import create_flask_response


def _get_node_gpu_info(conf_nodes, node_name):
    node = conf_nodes.get(node_name)
    if node:
        hardware = node.get('hardware')
        if hardware:
            gpus = hardware.get('gpus')
            if gpus:
                return gpus
    return None


def nodes_routes(app, mongo, auth):
    @app.route('/nodes', methods=['GET'])
    def get_nodes():
        user = auth.verify_user(request.authorization, request.cookies, request.remote_addr)

        cursor = mongo.db['nodes'].find()

        nodes = list(cursor)
        node_names = [node['nodeName'] for node in nodes]

        cursor = mongo.db['batches'].find(
            {
                'node': {'$in': node_names},
                'state': {'$in': ['scheduled', 'processing']}
            },
            {'experimentId': 1, 'node': 1}
        )
        batches = list(cursor)
        experiment_ids = list(set([ObjectId(b['experimentId']) for b in batches]))

        cursor = mongo.db['experiments'].find(
            {'_id': {'$in': experiment_ids}},
            {'container.settings.ram': 1}
        )
        experiments = {str(e['_id']): e for e in cursor}

        for node in nodes:
            batches_ram = [
                {
                    'batchId': str(b['_id']),
                    'ram': experiments[b['experimentId']]['container']['settings']['ram']
                }
                for b in batches
                if b['node'] == node['nodeName']
            ]
            node['currentBatches'] = batches_ram
            del node['_id']

        return create_flask_response(nodes, auth, user.authentication_cookie)
