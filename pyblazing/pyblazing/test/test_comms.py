import asyncio
import pytest

import ucp
import cudf

from dask_cuda import LocalCUDACluster
from distributed import Client, Worker, Scheduler, wait, get_worker
from distributed.comm import ucx, listen, connect
from ..apiv2.comms import listen, BlazingMessage, UCX, register_serialization
from distributed.utils_test import cleanup  # noqa: 401

enable_tcp_over_ucx = True
enable_nvlink = False
enable_infiniband = False


try:
    HOST = ucp.get_address()
except Exception:
    HOST = "127.0.0.1"


async def mock_msg_callback(msg):

    """
    Mocks the real callback which gets invoked each time
    a message is received on an endpoint.
    """

    print("Invoked w/ %s" % msg)
    if not hasattr(get_worker(), "_test_msgs_received"):
        get_worker()._test_msgs_received = []

    print("Setting callback")
    get_worker()._test_msgs_received.append(msg)


async def gather_results():
    """
    For testing purposes only- gathers the received
    messages on each worker.
    """
    return get_worker()._test_msgs_received


async def send(msg):
    """
    Just for testing- wrapper function/task to send
    a message on a worker.
    """
    await UCX.get().send(msg)


class PyBlazingCache():
    def add_to_cache_with_meta(self,cudf_data,metadata):
        pass

    def add_to_cache(self,cudf_data):
        pass


@pytest.mark.asyncio
async def test_ucx_localcluster( cleanup):
    async with LocalCUDACluster(
        protocol="ucx",

        dashboard_address=None,
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        asynchronous=True,
        enable_tcp_over_ucx=enable_tcp_over_ucx,
        enable_nvlink=enable_nvlink,
        enable_infiniband=enable_infiniband
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:

            register_serialization()
            await client.run(register_serialization, wait=True)

            ips_ports = await listen(mock_msg_callback, client)

            assert len(ips_ports) == len(client.scheduler_info()["workers"])
            for k, v in ips_ports.items():
                assert v is not None

            # Build message for one worker to send to the other
            for dask_addr, blazing_addr in ips_ports.items():
                meta = {"worker_ids": list(ips_ports.values())}
                data = cudf.DataFrame()
                data["a"] = cudf.Series([0, 1, 2, 3, 4])
                msg = BlazingMessage(meta, data)

                await client.run(send, msg, workers=[dask_addr], wait=True)

            received = await client.run(gather_results, wait=True)
            print("MSGS: " + str(received))

            for worker_addr, msgs in received.items():
                assert len(msgs) == len(ips_ports)
