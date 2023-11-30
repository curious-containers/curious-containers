
def _get_node_gpu_info(conf_nodes, node_name):
    node = conf_nodes.get(node_name)
    if node:
        hardware = node.get('hardware')
        if hardware:
            gpus = hardware.get('gpus')
            if gpus:
                return gpus
    return None
