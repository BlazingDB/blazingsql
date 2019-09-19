from collections import OrderedDict
from enum import Enum

from urllib.parse import urlparse

from .bridge import internal_api

from .filesystem import FileSystem
from .sql import SQL
from .sql import ResultSet
from .datasource import build_datasource
import time
import socket, errno
import subprocess
import os


def checkSocket(socketNum):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_free = False
    try:
        s.bind(("127.0.0.1", socketNum))
        socket_free = True
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            socket_free = False
        else:
            # something else raised the socket.error exception
            print("ERROR: Something happened when checking socket " + str(socketNum))
            print(e)
    s.close()
    return socket_free


def runEngine(network_interface = 'lo', processes = None):
    process = None
    devnull = open(os.devnull, 'w')
    if(checkSocket(9001)):
        process = subprocess.Popen(['blazingsql-engine', '1', '0' , '127.0.0.1', '9100', '127.0.0.1', '9001', '8891', network_interface], stdout = devnull, stderr = devnull)
    else:
        print("WARNING: blazingsql-engine was not automativally started, its probably already running")

    if (processes is not None):
        processes.append(process)
    return processes


def setupDask(dask_client):
    dask_client.run(runEngine,network_interface = 'eth0', processes = None)


def runAlgebra(processes = None):
    process = None
    devnull = open(os.devnull, 'w')
    if(checkSocket(8890)):
        if(os.getenv("CONDA_PREFIX") == None):
            process = subprocess.Popen(['java', '-jar', '/usr/local/lib/blazingsql-algebra.jar', '-p', '8890'])
        else:
            process = subprocess.Popen(['java', '-jar', os.getenv("CONDA_PREFIX") + '/lib/blazingsql-algebra.jar', '-p', '8890'], stdout = devnull, stderr = devnull)
    else:
        print("WARNING: blazingsql-algebra was not automativally started, its probably already running")

    if (processes is not None):
        processes.append(process)
    return processes


def runOrchestrator(processes = None):
    process = None
    devnull = open(os.devnull, 'w')
    if(checkSocket(9100)):
        process = subprocess.Popen(['blazingsql-orchestrator', '9100', '8889', '127.0.0.1', '8890'], stdout = devnull, stderr = devnull)
    else:
        print("WARNING: blazingsql-orchestrator was not automativally started, its probably already running")

    if (processes is not None):
        processes.append(process)
    return processes


class BlazingContext(object):

    def __init__(self, connection = 'localhost:8889', dask_client = None, run_orchestrator = True, run_engine = True, run_algebra = True, network_interface = 'lo', leave_processes_running = False):
        """
        :param connection: BlazingSQL cluster URL to connect to
            (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
        """
        processes = None
        if not leave_processes_running:
            processes = []

        if(dask_client is None):
            if run_orchestrator:
                processes = runOrchestrator(processes = processes)
                time.sleep(2)  # lets the orchestrator start before we start the other processes
            if run_engine:
                processes = runEngine(network_interface = network_interface, processes = processes)
            if run_algebra:
                processes = runAlgebra(processes = processes)
            if run_engine or run_algebra:
                time.sleep(3)  # lets the engine and algebra processes start before we continue
        else:
            if run_orchestrator:
                processes = runOrchestrator(processes = processes)
                time.sleep(1)  # lets the orchestrator start before we start the other processes
            setupDask(dask_client)
            if run_algebra:
                processes = runAlgebra(processes=processes)
                time.sleep(3) # lets the engine and algebra processes start before we continue

        # NOTE ("//"+) is a neat trick to handle ip:port cases
        parse_result = urlparse("//" + connection)
        orchestrator_host_ip = parse_result.hostname
        orchestrator_port = parse_result.port
        internal_api.SetupOrchestratorConnection(orchestrator_host_ip, orchestrator_port)

        # TODO percy handle errors (see above)
        self.connection = connection
        self.client = internal_api._get_client()
        self.fs = FileSystem()
        self.sqlObject = SQL()
        self.dask_client = dask_client
        self.processes = processes

    def __del__(self):
        # TODO percy clean next time
        # del self.sqlObject
        # del self.fs
        # del self.client
        if (self.processes is not None):
            for process in self.processes:
                if (process is not None):
                    process.terminate()

        pass

    def __repr__(self):
        return "BlazingContext('%s')" % (self.connection)

    def __str__(self):
        return self.connection

    # BEGIN FileSystem interface

    def localfs(self, prefix, **kwargs):
        return self.fs.localfs(self.client, prefix, **kwargs)

    def hdfs(self, prefix, **kwargs):
        return self.fs.hdfs(self.client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        return self.fs.s3(self.client, prefix, **kwargs)

    def gcs(self, prefix, **kwargs):
        return self.fs.gcs(self.client, prefix, **kwargs)

    def show_filesystems(self):
        print(self.fs)

    # END  FileSystem interface

    # BEGIN SQL interface

    # remove
    def create_table(self, table_name, input, **kwargs):
        ds = build_datasource(self.client, input, table_name, **kwargs)
        table = self.sqlObject.create_table(ds)

        return table

    def drop_table(self, table_name):
        return self.sqlObject.drop_table(table_name)

    # async
    def sql(self, sql, table_list = []):
        if (len(table_list) > 0):
            print("NOTE: You no longer need to send a table list to the .sql() funtion")
        return self.sqlObject.run_query(self.client, sql, self.dask_client)

    # END SQL interface


def make_context(connection = 'localhost:8889'):
    """
    :param connection: BlazingSQL cluster URL to connect to
           (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
    """
    bc = BlazingContext(connection)
    return bc

