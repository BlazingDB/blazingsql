import asyncio
import pytest

import ucp

from dask_cuda import LocalCUDACluster
from distributed import Client, Worker, Scheduler, wait, get_worker
from distributed.comm import ucx, listen, connect
from distributed.comm.registry import backends, get_backend
from distributed.comm import ucx, parse_address
from distributed.protocol import to_serialize
from distributed.deploy.local import LocalCluster
from dask.dataframe.utils import assert_eq
from distributed.utils_test import gen_test, loop, inc, cleanup, popen  # noqa: 401

from ..apiv2.comms import listen, BlazingMessage, UCX

import cudf

try:
    HOST = ucp.get_address()
except Exception:
    HOST = "127.0.0.1"


def mock_callback(msg):
    if not hasattr(get_worker(), "_test_msgs_received"):
        get_worker()._test_msgs_received = []

    print("Setting callback")
    get_worker()._test_msgs_received.append(msg)


async def gather_results():
    return get_worker()._test_msgs_received

async def send(msg):
    await UCX.get().send(msg)

@pytest.mark.asyncio
async def test_ucx_localcluster(cleanup):
    async with LocalCUDACluster(
        protocol="ucx",
        dashboard_address=None,
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        asynchronous=True,
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:

            ips_ports = await listen(mock_callback, client)

            assert len(ips_ports) == len(client.scheduler_info()["workers"])
            for k, v in ips_ports.items():
                assert v is not None

            # Build message for one worker to send to the other
            for dask_addr, blazing_addr in ips_ports.items():
                meta = {"worker_ids": [blazing_addr]}
                data = "THIS IS DATA"
                msg = BlazingMessage(meta, data)

                await client.run(send, msg, workers=[dask_addr], wait=True)

            received = await client.run(gather_results, wait=True)

            print(str(received))













