from distributed import get_worker
from distributed.comm.addressing import parse_host_port
from dask.distributed import default_client
from ucp.endpoint_reuse import EndpointReuse


def set_id_mappings_on_worker(mapping):
    worker = get_worker()
    worker.ucx_addresses = {}
    for worker_address in mapping:
        if worker_address != get_worker().address:
            worker.ucx_addresses[worker_address] = mapping[worker_address]


def initialize_listeners():
    def callback_init(ep):
        return 0
    worker = get_worker()
    ip, port = parse_host_port(get_worker().address)
    #worker.ucp_listener = EndpointReuse.create_listener(callback_init, port)

    return "ucx://%s:%s" % (ip, port)


async def initialize_endpoints():
    worker = get_worker()
    addresses = worker.ucx_addresses
    worker.ucp_endpoints = {}
    for address in addresses.values():
        ip, port = parse_host_port(address)
        worker.ucp_endpoints[address] = await EndpointReuse.create_endpoint(
            ip, port)


def listen(client=None):
    client = client if client is not None else default_client()
    worker_id_maps = client.run(initialize_listeners)
    client.run(set_id_mappings_on_worker, worker_id_maps)
    client.run(initialize_endpoints, wait=True)
    return worker_id_maps
