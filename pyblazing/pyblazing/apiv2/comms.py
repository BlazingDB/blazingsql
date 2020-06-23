
import ucp
from distributed import get_worker

from distributed.comm.ucx import UCXListener
from distributed.comm.ucx import UCXConnector
from distributed.comm.addressing import parse_host_port
from distributed.protocol.serialize import serialize, deserialize

from dask.distributed import default_client


def register_serialization():

    import cudf.comm.serialize  # noqa: F401
    from distributed.protocol import register_generic

    from distributed.protocol import dask_deserialize, dask_serialize
    from distributed.protocol.cuda import cuda_deserialize, cuda_serialize

    register_generic(BlazingMessage, 'cuda',
                     cuda_serialize, cuda_deserialize)

    register_generic(BlazingMessage, 'dask',
                     dask_serialize, dask_deserialize)


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


async def listen(callback, client=None):
    client = client if client is not None else default_client()
    return await client.run(start_listener, callback, wait=True)


async def start_listener(callback):
    UCX.get().callback = callback
    return await UCX.get().start_listener()


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
    def get_ucp_worker():
        return ucp.get_ucp_worker()

    async def start_listener(self):

        async def handle_comm(comm):
            print("Comm request: %s" % comm)

            while not comm.closed():
                msg = await comm.read()
                #msg = deserialize(*msg,
                      #            deserializers=("dask", "cuda", "pickle", "error"))
                print("Message Received: %s" % msg)
                await self.callback(msg)

        ip, port = parse_host_port(get_worker().address)

        self._listener = await UCXListener(ip, handle_comm)
        await self._listener.start()

        return "ucx://%s:%s" % (ip, self._listener.port)

    def listener_port(self):
        return self._listener.port()

    async def _create_endpoint(self, addr):

        print("Connecting to %s" % addr)
        ep = await UCXConnector().connect(addr)
        print("Done connecting to %s" % addr)
        self._endpoints[addr] = ep
        return ep

    async def get_endpoint(self, addr):
        if addr not in self._endpoints:
            print("Creating endpoint from %s to %s" % (get_worker().address, addr))
            ep = await self._create_endpoint(addr)
        else:
            print("Using endpoint from %s to %s" % (get_worker().address, addr))
            ep = self._endpoints[addr]

        return ep

    async def send(self, blazing_msg):
        """
        Send a BlazingMessage to the workers specified in `worker_ids`
        field of metadata
        """
        for addr in blazing_msg.metadata["worker_ids"]:
            ep = await self.get_endpoint(addr)
            #msg = serialize(blazing_msg,
            #                serializers=("dask", "cuda", "pickle", "error"))

            msg = "Hello World!"
            await ep.write(msg=msg,
                           serializers=("dask", "cuda", "pickle", "error"))
            print("Sent")

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
