
import ucp
from distributed import get_worker

from distributed.comm.ucx import UCXListener
from distributed.comm.ucx import UCXConnector
from distributed.comm.addressing import parse_host_port

import asyncio


class BlazingMessage:
    def __init__(self, metadata, data):
        self.metadata = metadata
        self.data = data

    def is_valid(self):
        return ("query_id" in self.metadata and
                "cache_id" in self.metadata and
                "worker_ids" in self.metadata and
                len(self.metadata["worker_ids"]) > 0 and
                self.data is not None)


class UCXClient:

    def __init__(self, client=None):
        self.workers = client.run(self.get_ip_port)
        self.closed = True
        self.ip, self.port = self.get_ip_port()

    def close(self):
        self.closed = True

    async def listen(self, callback_func):

        self.closed = False

        ep_listeners = []
        for remote_addr in self.workers:
            ep = UCX.get(self.addr).get_endpoint(remote_addr)
            ep_listeners.append(self.recv(ep))

        while not self.closed:
            for ep in ep_listeners:
                if not ep.closed():
                    await asyncio.create_task(self.recv(ep, callback_func))

    @staticmethod
    def get_ip_port():
        """
        Returns the local UCX listener port. The port will be created
        if it doesn't yet exist
        """
        ip, port = parse_host_port(get_worker().address)
        return ip, UCX.get().listener_port()

    async def send(self, blazing_msg,
                   serializers=("cuda", "dask", "pickle", "error")):
        """
        Send a BlazingMessage to the workers specified in `worker_ids`
        field of metadata
        """
        for addr in blazing_msg.worker_ids.values():
            ep = UCX.get().get_endpoint(addr)

            msg = {"meta": blazing_msg.metadata, "data": blazing_msg.data}
            await self.write(ep, msg=msg, serializers=serializers)

    async def recv(self, ep, callback_func,
                   deserializers=("cuda", "dask", "pickle", "error")):
        """
        Handles received message
        """
        # Receive message metadata
        msg = await ep.read(deserializers)
        callback_func(BlazingMessage(*msg))


class UCX:
    """
    Singleton UCX context to encapsulate all interactions with the
    UCX-py API and guarantee only a single listener & endpoints are
    created by cuML on a single process.
    """

    __instance = None

    def __init__(self, listener_callback):

        self.listener_callback = listener_callback
        self._endpoints = {}
        self._listener = None

        assert UCX.__instance is None

        UCX.__instance = self

    @staticmethod
    def get(listener_callback=None):
        if UCX.__instance is None:
            UCX(listener_callback)
        return UCX.__instance

    @staticmethod
    def get_ucp_worker():
        return ucp.get_ucp_worker()

    async def start_listener(self):
        self._listener = UCXListener(get_worker().address)
        self._listener.start()

    def listener_port(self):
        return self._listener.port()

    async def _create_endpoint(self, addr):
        ep = await UCXConnector().connect(addr)
        self._endpoints[addr] = ep
        return ep

    async def get_endpoint(self, addr):
        if addr not in self._endpoints:
            ep = await self._create_endpoint(addr)
        else:
            ep = self._endpoints[addr]

        return ep

    def stop_endpoints(self):
        for addr, ep in self._endpoints.items():
            if not ep.closed():
                ep.abort()
            del ep
        self._endpoints = {}

    def stop_listener(self):
        if self._listener is not None:
            self._listener.stop()

    def __del__(self):
        self.stop_endpoints()
        self.stop_listener()
