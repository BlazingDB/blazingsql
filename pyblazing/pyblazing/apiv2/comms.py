from tornado.ioloop import PeriodicCallback
import ucp
from distributed import get_worker

from distributed.comm.ucx import UCXListener
from distributed.comm.ucx import UCXConnector
from distributed.comm.addressing import parse_host_port
from distributed.protocol.serialize import to_serialize
import concurrent.futures

from dask.distributed import default_client

serde = ("cuda", "dask", "pickle", "error")

async def route_message(msg):
    
    worker = get_worker()
    if msg.metadata["add_to_specific_cache"] == "true":
        graph = worker.query_graphs[int(msg.metadata["query_id"])]
        cache = graph.get_kernel_output_cache(
            int(msg.metadata["kernel_id"]),
            msg.metadata["cache_id"]
        )
        cache.add_to_cache(msg.data)
    else:
        cache = worker.input_cache
        if(msg.data is None):
            import cudf
            msg.data = cudf.DataFrame()
        cache.add_to_cache_with_meta(msg.data, msg.metadata)


class PollingPlugin:
    def __init__(self, *args, **kwargs):
        pass

    def setup(self, worker=None):
        self._worker = worker
        self._pc = PeriodicCallback(callback=self.async_run_polling, callback_time=100)
        self._pc.start()
        get_worker().polling = False


    async def async_run_polling(self):
        import asyncio, os

        worker = get_worker()
        if worker.polling == True:
            return
        worker.polling = True
        
        while self._worker.output_cache.has_next_now():            
            df, metadata = self._worker.output_cache.pull_from_cache()
            if metadata["add_to_specific_cache"] == "false" and len(df) == 0:
                df = None
            await UCX.get().send(BlazingMessage(metadata, df))
        worker.polling = False




CTRL_STOP = "stopit"


class BlazingMessage:
    def __init__(self, metadata, data=None):
        self.metadata = metadata
        self.data = data

    def is_valid(self):
        return ("query_id" in self.metadata and
                "cache_id" in self.metadata and
                "worker_ids" in self.metadata and
                len(self.metadata["worker_ids"]) > 0 and
                self.data is not None)

def set_id_mappings_on_worker(mapping):
    get_worker().ucx_addresses = mapping


async def init_endpoints():
    for addr in get_worker().ucx_addresses.values():
        await UCX.get().get_endpoint(addr)

async def listen_async(callback=route_message, client=None):
    client = client if client is not None else default_client()
    worker_id_maps = await client.run(UCX.start_listener_on_worker, callback, wait=True)
    await client.run(set_id_mappings_on_worker, worker_id_maps, wait=True)
    return worker_id_maps



def listen(callback=route_message, client=None):
    client = client if client is not None else default_client()
    worker_id_maps = client.run(UCX.start_listener_on_worker, callback, wait=True)
    client.run(set_id_mappings_on_worker, worker_id_maps, wait=True)
    client.run(UCX.init_handlers,wait=True)
    return worker_id_maps


def cleanup(client=None):
    async def kill_ucx():
        await UCX.get().stop_endpoints()
        UCX.get().stop_listener()

    client = client if client is not None else default_client()
    return client.run(kill_ucx, wait=True)


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
        self.received = 0
        self.sent = 0

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
    async def init_handlers():
        addresses = get_worker().ucx_addresses
        eps = []
        for address in addresses.values():
            ep = await UCX.get().get_endpoint(address)

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
                    self.received += 1
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
        local_dask_addr = get_worker().ucx_addresses[get_worker().address]
        if blazing_msg.metadata["sender_worker_id"] in blazing_msg.metadata["worker_ids"]:
            print("Error! Sending message to self. Check launch configuration!")
        for dask_addr in blazing_msg.metadata["worker_ids"]:
            # Map Dask address to internal ucx endpoint address
            addr = get_worker().ucx_addresses[dask_addr]
            ep = await self.get_endpoint(addr)
            try:
                to_ser = {"metadata": to_serialize(blazing_msg.metadata)}
                if blazing_msg.data is not None:
                    to_ser["data"] = to_serialize(blazing_msg.data)
            except:
                print("An error occurred in serialization")

            try:
                await ep.write(msg=to_ser, serializers=serde)
            except:
                print("Error occurred during write")
            self.sent += 1


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

