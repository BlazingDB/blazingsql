import errno
import random
import socket

import netifaces as ni
from dask.distributed import default_client
from distributed import get_worker
from distributed.comm.addressing import parse_host_port
from distributed.comm.ucx import UCXConnector, UCXListener
from ucp.endpoint_reuse import EndpointReuse


def set_id_mappings_on_worker(mapping):
    worker = get_worker()
    worker.ucx_addresses = mapping


def checkSocket(socketNum):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    socket_free = False
    try:
        s.bind(("127.0.0.1", socketNum))
        socket_free = True
        s.close()
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            socket_free = False
        else:
            # something else raised the socket.error exception
            print("ERROR: Something happened when checking socket " + str(socketNum))

    return socket_free


def get_communication_port(network_interface):
    ralCommunicationPort = random.randint(10000, 32000)
    workerIp = ni.ifaddresses(network_interface)[ni.AF_INET][0]["addr"]
    while checkSocket(ralCommunicationPort) is False:
        ralCommunicationPort = random.randint(10000, 32000)
    return {"port": ralCommunicationPort, "ip": workerIp}


def listen(client, network_interface=""):
    worker_id_maps = client.run(get_communication_port, network_interface, wait=True)
    client.run(set_id_mappings_on_worker, worker_id_maps, wait=True)
    return worker_id_maps
