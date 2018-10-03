# coding=utf-8

import struct
import blazingdb.protocol

class Connector:
    
    def __init__(self):
        self._orchestatorConnection = blazingdb.protocol.UnixSocketConnection(
            '/tmp/blazingdb-orchestator.socket')
        self._interpreterConnection = blazingdb.protocol.UnixSocketConnection(
            '/tmp/blazingdb-interpreter.socket')

    def open(self):
        self._orchestatorClient = blazingdb.protocol.Client(self._orchestatorConnection)
        self._interpreterClient = blazingdb.protocol.Client(self._interpreterConnection)

    def close(self):
        self._orchestatorClient = None
        self._interpreterClient = None
    