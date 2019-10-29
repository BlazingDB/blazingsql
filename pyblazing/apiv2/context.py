from collections import OrderedDict
from enum import Enum

from urllib.parse import urlparse

from .bridge import internal_api

from .filesystem import FileSystem
from .sql import SQL
from .sql import ResultSet
from .datasource import build_datasource
import time
import datetime
import socket, errno
import subprocess
import os
import re


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


def runEngine(processes = None, network_interface = 'lo', orchestrator_ip = '127.0.0.1', orchestrator_port=9100, log_path=None):
    if log_path is not None:
        process_stdout= open(log_path + "_stdout.log", "w")
        process_stderr= open(log_path + "_stderr.log", "w")
    else:
        process_stdout = open(os.devnull, 'w')
        process_stderr = open(os.devnull, 'w')

    process = None    
    if(checkSocket(9001)):
        process = subprocess.Popen(['blazingsql-engine', '1', '0' , orchestrator_ip, str(orchestrator_port), '127.0.0.1', '9001', '8891', network_interface], stdout = process_stdout, stderr = process_stderr)
    else:
        print("WARNING: blazingsql-engine was not automativally started, its probably already running")

    if (processes is not None):
        processes['engine'] = process
    return processes


def setupDask(dask_client, network_interface = 'eth0', orchestrator_ip = None, orchestrator_port=9100, log_path=None):
    if (orchestrator_ip is None):
        orchestrator_ip = re.findall(r'(?:\d+\.){3}\d+', dask_client.scheduler_info().get('address'))[0]
    dask_client.run(runEngine, processes = None, network_interface = network_interface, orchestrator_ip=orchestrator_ip, orchestrator_port=orchestrator_port, log_path = log_path)


def runAlgebra(processes = None, log_path=None):
    if log_path is not None:
        process_stdout= open(log_path + "_stdout.log", "w")
        process_stderr= open(log_path + "_stderr.log", "w")
    else:
        process_stdout = open(os.devnull, 'w')
        process_stderr = open(os.devnull, 'w')

    process = None    
    if(checkSocket(8890)):
        if(os.getenv("CONDA_PREFIX") == None):
            process = subprocess.Popen(['java', '-jar', '/usr/local/lib/blazingsql-algebra.jar', '-p', '8890'])
        else:
            process = subprocess.Popen(['java', '-jar', os.getenv("CONDA_PREFIX") + '/lib/blazingsql-algebra.jar', '-p', '8890'], stdout = process_stdout, stderr = process_stderr)
    else:
        print("WARNING: blazingsql-algebra was not automativally started, its probably already running")

    if (processes is not None):
        processes['algebra'] = process
    return processes


def runOrchestrator(processes = None, log_path=None):
    if log_path is not None:
        process_stdout= open(log_path + "_stdout.log", "w")
        process_stderr= open(log_path + "_stderr.log", "w")
    else:
        process_stdout = open(os.devnull, 'w')
        process_stderr = open(os.devnull, 'w')

    process = None    
    if(checkSocket(9100)):
        process = subprocess.Popen(['blazingsql-orchestrator', '9100', '8889', '127.0.0.1', '8890'], stdout = process_stdout, stderr = process_stderr)
    else:
        print("WARNING: blazingsql-orchestrator was not automativally started, its probably already running")

    if (processes is not None):
        processes['orchestrator'] = process
    return processes

def waitForPingSuccess(client):
    ping_success = False
    num_tries = 0
    while (num_tries < 60 and not ping_success):
        ping_success = client.ping()
        num_tries = num_tries + 1
        if not ping_success:
            time.sleep(0.4)
    return ping_success


class BlazingContext(object):

    def __init__(self, connection = 'localhost:8889', dask_client = None, run_orchestrator = True, run_engine = True, run_algebra = True, network_interface = None, leave_processes_running = False, orchestrator_ip = None, orchestrator_port=9100, logs_destination = None):
        """
        :param connection: BlazingSQL cluster URL to connect to
            (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
        """

        self.need_shutdown= (not leave_processes_running) and (run_orchestrator or run_engine or run_algebra)

        logs_folder = None
        if logs_destination is not None:
            logs_folder = logs_destination # right now logs would go into files in a folder, but evetually we can use the same variable to define a network address for logging to be sent to
            try :
                if not os.path.isabs(logs_folder)  and os.getenv("CONDA_PREFIX") is not None:  # lets manage relative paths
                    logs_folder = os.path.join(os.getenv("CONDA_PREFIX"), logs_folder)   

                if not os.path.exists(logs_folder): # if folder does not exist, lets create it
                    os.mkdir(logs_folder)
            except Exception as error:
                print("WARNING: could not establish logs_folder. " + str(error))
                logs_folder = None
        
        timestamp_str = str(datetime.datetime.now()).replace(' ','_')        

        self.processes = None
        if not leave_processes_running:
            self.processes = {}

        if(dask_client is None):
            if network_interface is None:
                network_interface = 'lo'
            if orchestrator_ip is None:
                orchestrator_ip = '127.0.0.1'

            if run_orchestrator:
                path = None if logs_folder is None else os.path.join(logs_folder, timestamp_str + "_blazingsql_orchestrator")
                self.processes = runOrchestrator(processes = self.processes, log_path = path)
            if run_engine:
                path = None if logs_folder is None else os.path.join(logs_folder, timestamp_str + "_blazingsql_engine")
                self.processes = runEngine(processes = self.processes, network_interface = network_interface, orchestrator_ip = orchestrator_ip, orchestrator_port = orchestrator_port, log_path = path)
            if run_algebra:
                path = None if logs_folder is None else os.path.join(logs_folder, timestamp_str + "_blazingsql_algebra")
                self.processes = runAlgebra(processes = self.processes, log_path = path)
        else:
            if network_interface is None:
                network_interface = 'eth0'
                
            if run_orchestrator:
                path = None if logs_folder is None else os.path.join(logs_folder, timestamp_str + "_blazingsql_orchestrator")
                self.processes = runOrchestrator(processes = self.processes, log_path = path)
            if run_engine:
                path = None if logs_folder is None else os.path.join(logs_folder, timestamp_str + "_blazingsql_engine")
                setupDask(dask_client, network_interface=network_interface, orchestrator_ip = orchestrator_ip, orchestrator_port = orchestrator_port, log_path = path)
            if run_algebra:
                path = None if logs_folder is None else os.path.join(logs_folder, timestamp_str + "_blazingsql_algebra")
                self.processes = runAlgebra(processes=self.processes, log_path = path)
        
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
        services_ready = waitForPingSuccess(self.client)
        if services_ready:
            print("BlazingContext ready")
        else:
            print("Timedout waiting for services to be ready")

    def ready(self, wait=False):
        if wait:
            waitForPingSuccess(self.client)
            return True
        else:
            return self.client.ping()

    def shutdown(self, process_names=None):

        if process_names is None:
            process_names = list(self.processes.keys())
            if self.dask_client is not None:
                process_names.append("engine")

        if process_names is not None:
            self.client.call_shutdown(process_names)
            time.sleep(1) # lets give it a sec before we guarantee the processes are shutdown

        if self.processes is not None:
            for process in list(self.processes.values()): # this should not be necessary, but it guarantees that the processes are shutdown
                if (process is not None):
                    process.terminate()
        self.need_shutdown=False
                    

    def __del__(self):
        if self.need_shutdown:
            self.shutdown()
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
        ds = build_datasource(self.client,
                              input,
                              table_name,
                              dask_client=self.dask_client,
                              **kwargs)
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


def make_context(connection='localhost:8889',
                 dask_client=None,
                 network_interface='lo',
                 run_orchestrator=True,
                 run_algebra=True,
                 run_engine=True):
    """
    :param connection: BlazingSQL cluster URL to connect to
           (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
    """
    bc = BlazingContext(connection,
                        dask_client=dask_client,
                        network_interface=network_interface,
                        run_orchestrator=run_orchestrator,
                        run_algebra=run_algebra,
                        run_engine=run_engine)
    return bc

