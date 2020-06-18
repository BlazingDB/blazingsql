
import ucp
from distributed import get_worker

from distributed.comm.utils import to_frames
from distributed.comm.utils import from_frames
from distributed.protocol import to_serialize
from distributed.utils import nbytes

import asyncio
import rmm
import numpy as np


class BlazingMessage():
    def __init__(self, metadata, data):
        self.metadata = metadata
        self.data = data

    def is_valid(self):
        return ("query_id" in self.metadata and
                "cache_id" in self.metadata and
                "worker_ids" in self.metadata and
                len(self.metadata["worker_ids"]) > 0 and
                self.data is not None)


class CommsClient:

    def __init__(self, client=None):
        self.workers = client.run(self.get_ip_port)
        self.closed = True

    def close(self):
        self.closed = True

    async def listen(self):

        self.closed = False

        ep_listeners = []
        for ip, port in self.workers:
            ep = UCX().get_endpoint(ip, port)
            ep_listeners.append(self.recv(ep))

        while not self.closed:
            [await asyncio.create_task(self.recv(ep))
             for ep in ep_listeners]

    @staticmethod
    def get_ip_port():
        """
        Returns the local UCX listener port. The port will be created
        if it doesn't yet exist
        """
        ip, port = parse_host_port(get_worker().address)
        return ip, UCX().listener_port()

    async def send(self, workers, blazing_msg,
                   serializers={"cuda", "dask", "pickle"}):

        for ip, port in workers:
            ep = UCX().get_endpoint(ip, port)

            # Send Metadata
            meta_msg = {"data": to_serialize(blazing_msg.metadata)}
            frames = await to_frames(meta_msg, serializers)

            await self.send_frames(ep, frames)

            # Send payload
            payload_msg = {"data": to_serialize(blazing_msg.data)}
            frames = await to_frames(payload_msg, serializers)
            await self.send_frames(ep, frames)

    @staticmethod
    async def send_frames(ep, frames):
        await ep.send(np.array([len(frames)], dtype=np.uint64))
        await ep.send(
            np.array(
                [hasattr(f, "__cuda_array_interface__") for f in frames], dtype=np.bool
            )
        )
        await ep.send(np.array([nbytes(f) for f in frames], dtype=np.uint64))
        # Send frames
        for frame in frames:
            if nbytes(frame) > 0:
                await ep.send(frame)

    @staticmethod
    async def recv(ep, callback_func):
        """
        Handles received message
        """
        frames = await CommsClient.recv_frames(ep)
        callback_func(frames)

    @staticmethod
    async def recv_frames(ep):
        try:
            # Recv meta data
            nframes = np.empty(1, dtype=np.uint64)
            await ep.recv(nframes)
            is_cudas = np.empty(nframes[0], dtype=np.bool)
            await ep.recv(is_cudas)
            sizes = np.empty(nframes[0], dtype=np.uint64)
            await ep.recv(sizes)
        except (ucp.exceptions.UCXCanceled, ucp.exceptions.UCXCloseError) as e:
            raise e("An error occurred receiving frames")

        # Recv frames
        frames = []
        for is_cuda, size in zip(is_cudas.tolist(), sizes.tolist()):
            if size > 0:
                if is_cuda:
                    frame = cuda_array(size)
                else:
                    frame = np.empty(size, dtype=np.uint8)
                await ep.recv(frame)
                frames.append(frame)
            else:
                if is_cuda:
                    frames.append(cuda_array(size))
                else:
                    frames.append(b"")

        msg = await from_frames(frames)
        return frames, msg


async def _connection_func(ep):
    return 0


def parse_host_port(address):
    """
    Given a string address with host/port, build a tuple(host, port)
    :param address: string address to parse
    :return: tuple(host, port)
    """
    if '://' in address:
        address = address.rsplit('://', 1)[1]
    host, port = address.split(':')
    port = int(port)
    return host, port


def cuda_array(size):
    return rmm.DeviceBuffer(size=size)


class UCX:
    """
    Singleton UCX context to encapsulate all interactions with the
    UCX-py API and guarantee only a single listener & endpoints are
    created by cuML on a single process.
    """

    __instance = None

    def __init__(self, listener_callback):

        self.listener_callback = listener_callback

        self._create_listener()
        self._endpoints = {}

        assert UCX.__instance is None

        UCX.__instance = self

    @staticmethod
    def get(listener_callback=_connection_func):
        if UCX.__instance is None:
            UCX(listener_callback)
        return UCX.__instance

    def get_ucp_worker(self):
        return ucp.get_ucp_worker()

    def _create_listener(self):
        self._listener = ucp.create_listener(self.listener_callback)

    def listener_port(self):
        return self._listener.port

    async def _create_endpoint(self, ip, port):
        ep = await ucp.create_endpoint(ip, port)
        self._endpoints[(ip, port)] = ep
        return ep

    async def get_endpoint(self, ip, port):
        if (ip, port) not in self._endpoints:
            ep = await self._create_endpoint(ip, port)
        else:
            ep = self._endpoints[(ip, port)]

        return ep

    def __del__(self):
        for ip_port, ep in self._endpoints.items():
            if not ep.closed():
                ep.abort()
            del ep

        self._listener.close()
