import asyncio
import pytest

import ucp
import cudf

from dask_cuda import LocalCUDACluster
from distributed import Client, Worker, Scheduler, wait, get_worker
from distributed.comm import ucx, listen, connect
import numpy as np
from ..apiv2.comms import listen, BlazingMessage, UCX, register_callbacks, register_serialization
from distributed.utils_test import gen_test, loop, inc, cleanup, popen  # noqa: 401

try:
    HOST = ucp.get_address()
except Exception:
    HOST = "127.0.0.1"


async def mock_msg_callback(msg):

    print("Invoked w/ %s" % msg)
    if not hasattr(get_worker(), "_test_msgs_received"):
        get_worker()._test_msgs_received = []

    print("Setting callback")
    get_worker()._test_msgs_received.append(msg)


async def gather_results():
    return get_worker()._test_msgs_received


async def send(msg):
    await UCX.get().send(msg)

@pytest.mark.asyncio
async def test_ucx_localcluster( cleanup):
    async with LocalCUDACluster(
        protocol="ucx",
        dashboard_address=None,
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        asynchronous=True,
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:

            register_serialization()
            await client.run(register_serialization, wait=True)

            ips_ports = await listen(client)

            assert len(ips_ports) == len(client.scheduler_info()["workers"])
            for k, v in ips_ports.items():
                assert v is not None

            await register_callbacks(mock_msg_callback, list(ips_ports.values()), client)

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


def test_serialize_blazingmsg():
    from distributed.protocol.serialize import serialize, deserialize

    meta = {"worker_ids": [0, 1, 2, 3]}
    data = cudf.DataFrame()
    data["a"] = cudf.Series([0, 1, 2, 3, 4])
    msg = BlazingMessage(meta, data)

    ser = serialize(msg, serializers=("cuda", "dask"))


    print(str(ser))

    de = deserialize(*ser)
