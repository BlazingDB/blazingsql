
import ucp
from distributed import get_worker

from distributed.comm.ucx import UCXListener
from distributed.comm.ucx import UCXConnector
from distributed.comm.addressing import parse_host_port

from dask.distributed import default_client

import cudf.comm.serialize  # noqa: F401

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


async def listen(callback_func, client=None):
    client = client if client is not None else default_client()
    return await client.run(start_listener, callback_func, wait=True)


async def start_listener(listener_callback):
    return await UCX.get(listener_callback).start_listener()


class UCXClient:
    def __init__(self, client=None):
        self.closed = True


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

        async def handle_comm(comm):
            msg = await comm.read()
            self.listener_callback(BlazingMessage(*msg))

        ip, port = parse_host_port(get_worker().address)

        self._listener = await UCXListener(ip, handle_comm)
        await self._listener.start()

        return "ucx://%s:%s" % (ip, self._listener.port)

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

    async def send(self, blazing_msg,
                   serializers=("cuda", "dask", "pickle", "error")):
        """
        Send a BlazingMessage to the workers specified in `worker_ids`
        field of metadata
        """

        for addr in blazing_msg.metadata["worker_ids"]:
            ep = await self.get_endpoint(addr)

            msg = {"meta": blazing_msg.metadata, "data": blazing_msg.data}
            await ep.write(msg=msg, serializers=serializers)

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
