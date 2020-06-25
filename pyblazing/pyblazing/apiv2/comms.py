import ucp
from distributed import get_worker

from distributed.comm.ucx import UCXListener
from distributed.comm.ucx import UCXConnector
from distributed.comm.addressing import parse_host_port
from distributed.protocol.serialize import to_serialize

from dask.distributed import default_client

serde = ("dask", "cuda", "pickle", "error")


def route_message(msg):
    worker = get_worker()
    if msg.metadata["add_to_specific_cache"]:
        cache = worker.query_graphs[msg.metadata["query_id"]].get_kernel_output_cache(
            msg.metadata["kernel_id"],
            cache_id=msg.metadata["cache_id"]
        )
        cache.add_to_cache(msg.data)
    else:
        cache = worker.input_cache
        cache.add_to_cache_with_meta(msg.data,msg.metadata)


async def run_polling_thread():  # doctest: +SKIP
    dask_worker = get_worker()
    import asyncio
    while True:
        with nogil:
            df, metadata = await dask_worker.output_cache.pull_from_cache()
        await  UCX.get().send(BlazingMessage(df, metadata))
        await asyncio.sleep(0)

CTRL_STOP = "stopit"


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


async def listen(callback=route_message, client=None):
    client = client if client is not None else default_client()
    return await client.run(UCX.start_listener_on_worker, callback, wait=True)


async def cleanup(client=None):
    async def kill_ucx():
        await UCX.get().stop_endpoints()
        UCX.get().stop_listener()

    client = client if client is not None else default_client()
    return await client.run(kill_ucx, wait=True)



class UCX:
    """
    Singleton UCX context to encapsulate all interactions with the
    UCX-py API and guarantee only a single listener & endpoints are
    created by cuML on a single process.
    """

    __instance = None

    def __init__(self):

        self.callback = None
        self._endpoints = {}
        self._listener = None

        assert UCX.__instance is None

        UCX.__instance = self

    @staticmethod
    def get():
        if UCX.__instance is None:
            UCX()
        return UCX.__instance

    @staticmethod
    async def start_listener_on_worker(callback):
        UCX.get().callback = callback
        return await UCX.get().start_listener()

    @staticmethod
    def get_ucp_worker():
        return ucp.get_ucp_worker()

    async def start_listener(self):

        async def handle_comm(comm):

            should_stop = False
            while not comm.closed() and not should_stop:
                msg = await comm.read()
                if msg == CTRL_STOP:
                    should_stop = True
                else:
                    msg = BlazingMessage(**{k: v.deserialize()
                                            for k, v in msg.items()})
                    await self.callback(msg)

        ip, port = parse_host_port(get_worker().address)

        self._listener = await UCXListener(ip, handle_comm)
        await self._listener.start()

        return "ucx://%s:%s" % (ip, self.listener_port())

    def listener_port(self):
        return self._listener.port

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

    async def send(self, blazing_msg):
        """
        Send a BlazingMessage to the workers specified in `worker_ids`
        field of metadata
        """
        for addr in blazing_msg.metadata["worker_ids"]:
            ep = await self.get_endpoint(addr)
            to_ser = {"metadata": to_serialize(blazing_msg.metadata),
                      "data": to_serialize(blazing_msg.data)}
            await ep.write(msg=to_ser, serializers=serde)

    def abort_endpoints(self):
        for addr, ep in self._endpoints.items():
            if not ep.closed():
                ep.abort()
            del ep
        self._endpoints = {}

    async def stop_endpoints(self):
        for addr, ep in self._endpoints.items():
            if not ep.closed():
                await ep.write(msg=CTRL_STOP, serializers=serde)
                await ep.close()
            del ep
        self._endpoints = {}

    def stop_listener(self):
        if self._listener is not None:
            self._listener.stop()

    def __del__(self):
        self.abort_endpoints()
        self.stop_listener()
