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
    print("calling route")
    worker = get_worker()
    if msg.metadata["add_to_specific_cache"] == "true":
        graph = worker.query_graphs[int(msg.metadata["query_id"])]
        #print(msg.metadata)
        print("Cacheid = " + msg.metadata["cache_id"])
        cache = graph.get_kernel_output_cache(
            int(msg.metadata["kernel_id"]),
            msg.metadata["cache_id"]
        )
        print("this is the route ")
        print(msg.data)
        cache.add_to_cache(msg.data)
    else:
        print("going into alt")
        cache = worker.input_cache
        if(msg.data is None):
            import cudf
            msg.data = cudf.DataFrame()
        cache.add_to_cache_with_meta(msg.data, msg.metadata)
    print("done routing message")


# async def run_comm_thread():  # doctest: +SKIP
#    dask_worker = get_worker()
#    import asyncio
#    while True:
#        df, metadata = dask_worker.output_cache.pull_from_cache()
#        await UCX.get().send(BlazingMessage(df, metadata))
#        await asyncio.sleep(1)

def run_comm_thread():  # doctest: +SKIP
    dask_worker = get_worker()
    import asyncio

    try:
        loop = asyncio.get_event_loop()
        print("Starting listeners")
        dask_worker.queue.put(loop.run_until_complete(UCX.start_listener_on_worker(route_message)))

        # wait for address mapping to be avaiable
        cv_have_map = dask_worker.cv_have_map
        with cv_have_map:
            cv_have_map.wait()

        set_id_mappings_on_worker(dask_worker.worker_addr_map)
        loop.run_until_complete(UCX.init_handlers())

        async def work():
            with concurrent.futures.ThreadPoolExecutor() as pool:
                while True:
                    # print("Pull_from_cache")
                    df, metadata = await loop.run_in_executor(pool,
                        dask_worker.output_cache.pull_from_cache)
                    if metadata["add_to_specific_cache"] == "false":
                        #print("Should never get here!")
                        #print(metadata)
                        df = None
                    print(df)
                    await UCX.get().send(BlazingMessage(metadata, df))

        # run this coroutine
        loop.run_until_complete(work())
    except Exception as e:
        print('Communication thread down: {}'.format(repr(e)))


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
    print("Invoked async listener")
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
        print("addresses: "+ str(addresses))
        eps = []
        for address in addresses.values():
            ep = await UCX.get().get_endpoint(address)
            print(ep)

    @staticmethod
    def get_ucp_worker():
        return ucp.get_ucp_worker()

    async def start_listener(self):

        async def handle_comm(comm):
            print("handling comm")
            should_stop = False
            while not comm.closed() and not should_stop:
                print("%s- Listening!" % get_worker().address)
                msg = await comm.read()
                print("%s- got msg: %s" % (get_worker().address, msg))
                if msg == CTRL_STOP:
                    print("SHOULD STOP SET!")
                    should_stop = True
                else:
                    msg = BlazingMessage(**{k: v.deserialize()
                                            for k, v in msg.items()})
                    self.received += 1
                    print("%d messages received on %s" % (self.received, get_worker().address))
                    if "message_id" in msg.metadata:
                        print("Finished receiving message id: "+ str(msg.metadata["message_id"]))
                    else:
                        print("No message_id")
                    print("Invoking callback")
                    await self.callback(msg)
                    print("Done invoting callback")

            print("Listener shutting down")

        ip, port = parse_host_port(get_worker().address)

        self._listener = await UCXListener(ip, handle_comm)

        print("Starting listener on worker")
        await self._listener.start()

        print("Started listener on port " + str(self.listener_port()))

        return "ucx://%s:%s" % (ip, self.listener_port())

    def listener_port(self):
        return self._listener.port

    async def _create_endpoint(self, addr):
        ep = await UCXConnector().connect(addr)
        self._endpoints[addr] = ep
        print("Created endpoint: " + str(ep))
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
        print("calling send: "+ str(blazing_msg.metadata))

        local_dask_addr = get_worker().ucx_addresses[get_worker().address]
        for dask_addr in blazing_msg.metadata["worker_ids"]:
            # Map Dask address to internal ucx endpoint address
            addr = get_worker().ucx_addresses[dask_addr]
            print("dask_addr=%s mapped to blazing_ucx_addr=%s" %(dask_addr, addr))

            print("local_worker=%s, remote_worker=%s" % (local_dask_addr, addr))
            
            ep = await self.get_endpoint(addr)
            try:
                to_ser = {"metadata": to_serialize(blazing_msg.metadata)}
            
                if blazing_msg.data is not None:
                    to_ser["data"] = to_serialize(blazing_msg.data)
                     
                    print(str(blazing_msg.data))
            except:
                print("An error occurred in serialization")

            try:
                await ep.write(msg=to_ser, serializers=serde)
            except:
                print("Error occurred during write")
            self.sent += 1
            print("%d messages sent on %s" % (self.sent, get_worker().address))
            print("seems like it wrote")

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
        print("Cleaning up")
        self.abort_endpoints()
        self.stop_listener()

