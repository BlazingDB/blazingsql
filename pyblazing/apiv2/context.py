from collections import OrderedDict
from enum import Enum

from urllib.parse import urlparse

from .bridge import internal_api

from threading import  Lock
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
import pandas
import pyarrow
from urllib.parse import urlparse
from urllib.parse import ParseResult
from pathlib import PurePath
import cio
import pyblazing
import cudf
import dask_cudf
import dask
import jpype
import netifaces as ni
from random import seed
from random import randint

jpype.addClassPath( os.path.join(os.getenv("CONDA_PREFIX"), 'lib/blazingsql-algebra.jar'))
jpype.addClassPath(os.path.join(os.getenv("CONDA_PREFIX"), 'lib/blazingsql-algebra-core.jar'))

jpype.startJVM(jpype.getDefaultJVMPath(), '-ea',convertStrings=False)

ArrayClass = jpype.JClass('java.util.ArrayList')
ColumnTypeClass = jpype.JClass('com.blazingdb.calcite.catalog.domain.CatalogColumnDataType')
dataType = ColumnTypeClass.fromString("GDF_INT8")
ColumnClass = jpype.JClass('com.blazingdb.calcite.catalog.domain.CatalogColumnImpl')
TableClass = jpype.JClass('com.blazingdb.calcite.catalog.domain.CatalogTableImpl')
DatabaseClass = jpype.JClass('com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl')
BlazingSchemaClass = jpype.JClass('com.blazingdb.calcite.schema.BlazingSchema')
RelationalAlgebraGeneratorClass = jpype.JClass('com.blazingdb.calcite.application.RelationalAlgebraGenerator')


seed(11243)




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

def initializeBlazing(ralId = 0, networkInterface = 'lo'):
    print(networkInterface)
    workerIp = ni.ifaddresses(networkInterface)[ni.AF_INET][0]['addr']
    ralCommunicationPort = randint(10000,40000)
    while checkSocket(ralCommunicationPort) == False:
        ralCommunicationPort = randint(10000,40000)
    cio.initializeCaller(ralId, 0, networkInterface.encode(), workerIp.encode(), ralCommunicationPort)
    return ralCommunicationPort, workerIp


class BlazingTable(object):
    def __init__(self, input,fileType, files=None, calcite_to_file_indices=None, num_row_groups=None):
        self.input = input
        self.calcite_to_file_indices = calcite_to_file_indices
        self.files = files
        self.num_row_groups = num_row_groups
        self.fileType = fileType

    def getSlices(self,numSlices):
        nodeFilesList = []
        if self.files is None:
            for i in range(0,numSlices):
                nodeFilesList.append(BlazingTable(self.input,self.fileType))
            return nodeFilesList
        remaining = len(self.files)
        startIndex = 0
        for i in range(0,numSlices):
            batchSize = int(remaining / (numSlices - i))
            print(batchSize)
            print(startIndex)
            tempFiles=self.files[startIndex : startIndex + batchSize]
            if self.num_row_groups is not None:
                nodeFilesList.append(BlazingTable(self.input,self.fileType,files=tempFiles, calcite_to_file_indices=self.calcite_to_file_indices, num_row_groups=self.num_row_groups[startIndex : startIndex + batchSize]))
            else:
                nodeFilesList.append(BlazingTable(self.input,self.fileType,files=tempFiles, calcite_to_file_indices=self.calcite_to_file_indices))
            startIndex = startIndex + batchSize
            remaining = remaining - batchSize
        return nodeFilesList




class BlazingContext(object):

    def __init__(self, dask_client = None, run_orchestrator = True, run_engine = True, run_algebra = True, network_interface = None, leave_processes_running = False, orchestrator_ip = None, orchestrator_port=9100, logs_destination = None):
        """
        :param connection: BlazingSQL cluster URL to connect to
            (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
        """
        self.lock = Lock()
        self.dask_client = dask_client
        self.nodes = []

        if(dask_client is not None):
            if network_interface is None:
                network_interface = 'eth0'
            dask_futures = []
            masterIndex = 0
            i = 0
            print(network_interface)
            for worker in list(self.dask_client.scheduler_info()["workers"]):
                dask_futures.append(self.dask_client.submit(  initializeBlazing,ralId = i, networkInterface = network_interface, workers = [worker]))
                i = i + 1
            for connection in dask_futures:
                ralPort, ralIp = connection.result()
                node = {}
                node['ip'] = ralIp
                node['communication_port'] = ralPort
                self.nodes.append(node)
        else:
            ralPort, ralIp = initializeBlazing(ralId = 0, networkInterface = 'lo')
            node = {}
            node['ip'] = ralIp
            node['communication_port'] = ralPort
            self.nodes.append(node)

        # NOTE ("//"+) is a neat trick to handle ip:port cases
        #internal_api.SetupOrchestratorConnection(orchestrator_host_ip, orchestrator_port)

        self.fs = FileSystem()

        self.db = DatabaseClass("main")
        self.schema = BlazingSchemaClass(self.db)
        self.generator = RelationalAlgebraGeneratorClass(self.schema)
        self.tables = {}

        self.PARQUET_FILE_TYPE = 0
        self.ORC_FILE_TYPE = 1
        self.CSV_FILE_TYPE = 2
        self.JSON_FILE_TYPE = 3
        self.CUDF_TYPE = 4
        self.DASK_CUDF_TYPE = 5
        #waitForPingSuccess(self.client)
        print("BlazingContext ready")

    def ready(self, wait=False):
        if wait:
            waitForPingSuccess(self.client)
            return True
        else:
            return self.client.ping()



    def __del__(self):
        pass

    def __repr__(self):
        return "BlazingContext('%s')" % (self.connection)

    def __str__(self):
        return self.connection

    # BEGIN FileSystem interface

    def localfs(self, prefix, **kwargs):
        return self.fs.localfs(self.dask_client, prefix, **kwargs)

    def hdfs(self, prefix, **kwargs):
        return self.fs.hdfs(self.dask_client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        return self.fs.s3(self.dask_client, prefix, **kwargs)

    def gcs(self, prefix, **kwargs):
        return self.fs.gcs(self.dask_client, prefix, **kwargs)

    def show_filesystems(self):
        print(self.fs)

    # END  FileSystem interface
    def _to_url(self, str_input):
        url = urlparse(str_input)
        return url

    def _to_path(self, url):
        path = PurePath(url.path)
        return path
    # BEGIN SQL interface
    def type_from_path(self, file, file_format):

        url = self._to_url(file)
        path = self._to_path(url)

        if file_format is not None:
            if not any([type == file_format for type in ['parquet','orc','csv','json']]):
                print("WARNING: file_format does not match any of the supported types: 'parquet','orc','csv','json'")

        if file_format == 'parquet' or path.suffix == '.parquet':
            return self.PARQUET_FILE_TYPE

        if file_format == 'csv' or path.suffix == '.csv' or path.suffix == '.psv' or path.suffix == '.tbl':
            return self.CSV_FILE_TYPE

        if file_format == 'json' or path.suffix == '.json':
            return self.JSON_FILE_TYPE

        if file_format == 'orc' or path.suffix == '.orc':
            return self.ORC_FILE_TYPE

        return None

    def explain(self,sql):
        return str(self.generator.getRelationalAlgebraString(sql))

    def add_remove_table(self,tableName,addTable,table=None):
        self.lock.acquire()
        try:
            if(addTable):
                self.db.removeTable(tableName)
                self.tables[tableName] = table
                arr = ArrayClass()
                order = 0
                for column in table.input.columns:
                    dataframe_column = table.input._cols[column]
                    data_sz = len(dataframe_column)
                    dtype = pyblazing.api.get_np_dtype_to_gdf_dtype_str(dataframe_column.dtype)
                    dataType = ColumnTypeClass.fromString(dtype)
                    column = ColumnClass(column,dataType,order);
                    arr.add(column)
                    order = order + 1
                tableJava = TableClass(tableName,self.db,arr)
                self.db.addTable(tableJava)
                self.schema = BlazingSchemaClass(self.db)
                self.generator = RelationalAlgebraGeneratorClass(self.schema)
            else:
                self.db.removeTable(tableName)
                self.schema = BlazingSchemaClass(self.db)
                self.generator = RelationalAlgebraGeneratorClass(self.schema)
                del self.tables[tableName]
        finally:
            self.lock.release()



    def create_table(self, table_name, input, **kwargs):
        table = None
        if type(input) == str:
            input = [input,]
        file_format = kwargs.get('file_format', None)
        if type(input) == pandas.DataFrame:
            table = BlazingTable(cudf.DataFrame.from_pandas(input),self.CUDF_TYPE)
        elif type(input) == pyarrow.Table:
            table = BlazingTable(cudf.DataFrame.from_arrow(input),self.CUDF_TYPE)
        elif type(input) == cudf.DataFrame:
            table = BlazingTable(input,self.CUDF_TYPE)
        elif type(input) == list:
            file_type = self.type_from_path(input[0],file_format)
            parsedSchema = cio.parseSchemaCaller(input,file_type)
            table = BlazingTable(parsedSchema['columns'],file_type,files=parsedSchema['files'],calcite_to_file_indices=parsedSchema['calcite_to_file_indices'],num_row_groups=parsedSchema['num_row_groups'])
        elif type(input) == dask_cudf.core.DataFrame:
            print("not supported")
        if table is not None:
            self.add_remove_table(table_name,True,table)
        return table

    def drop_table(self, table_name):
        self.add_remove_table(table_name,False)


    def sql(self, sql, table_list = []):
        # TODO: remove hardcoding
        masterIndex = 0
        nodeTableList =  [{} for _ in range(len(self.nodes))]
        fileTypes = []
        for table in self.tables:
            fileTypes.append(self.tables[table].fileType)
            currentTableNodes = self.tables[table].getSlices(len(self.nodes))
            j = 0
            for nodeList in nodeTableList:
                nodeList[table] = currentTableNodes[j]
                j = j + 1

        ctxToken = randint(0,64000)
        accessToken = 0
        if (len(table_list) > 0):
            print("NOTE: You no longer need to send a table list to the .sql() funtion")
        algebra = self.explain(sql)
        if self.dask_client is None:
            result = cio.runQueryCaller(masterIndex,self.nodes,self.tables,fileTypes,ctxToken,algebra,accessToken)
        else:
            dask_futures = []
            i = 0
            for worker in list(self.dask_client.scheduler_info()["workers"]):
                dask_futures.append(self.dask_client.submit(    cio.runQueryCaller,masterIndex,self.nodes,nodeTableList[i],fileTypes,ctxToken,algebra,accessToken, workers = [worker]))
                i = i + 1
            result = dask.dataframe.from_delayed(dask_futures)
        return result

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
