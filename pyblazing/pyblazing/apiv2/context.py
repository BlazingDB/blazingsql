# NOTE WARNING NEVER CHANGE THIS FIRST LINE!!!! NEVER EVER
import cudf

from collections import OrderedDict
from enum import Enum

from urllib.parse import urlparse

from threading import Lock
from weakref import ref
from pyblazing.apiv2.filesystem import FileSystem
from pyblazing.apiv2 import DataType
import pandas as pd

from .hive import *
import time
import datetime
import socket
import errno
import subprocess
import os
import re
import pandas
import numpy as np
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
import dask.distributed
import netifaces as ni

import random

jpype.addClassPath(
    os.path.join(
        os.getenv("CONDA_PREFIX"),
        'lib/blazingsql-algebra.jar'))
jpype.addClassPath(
    os.path.join(
        os.getenv("CONDA_PREFIX"),
        'lib/blazingsql-algebra-core.jar'))

jpype.startJVM(jpype.getDefaultJVMPath(), '-ea', convertStrings=False)

ArrayClass = jpype.JClass('java.util.ArrayList')
ColumnTypeClass = jpype.JClass(
    'com.blazingdb.calcite.catalog.domain.CatalogColumnDataType')
dataType = ColumnTypeClass.fromString("GDF_INT8")
ColumnClass = jpype.JClass(
    'com.blazingdb.calcite.catalog.domain.CatalogColumnImpl')
TableClass = jpype.JClass(
    'com.blazingdb.calcite.catalog.domain.CatalogTableImpl')
DatabaseClass = jpype.JClass(
    'com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl')
BlazingSchemaClass = jpype.JClass('com.blazingdb.calcite.schema.BlazingSchema')
RelationalAlgebraGeneratorClass = jpype.JClass(
    'com.blazingdb.calcite.application.RelationalAlgebraGenerator')


def get_np_dtype_to_gdf_dtype_str(dtype):
    dtypes = {
        np.dtype('float64'): 'GDF_FLOAT64',
        np.dtype('float32'): 'GDF_FLOAT32',
        np.dtype('int64'): 'GDF_INT64',
        np.dtype('int32'): 'GDF_INT32',
        np.dtype('int16'): 'GDF_INT16',
        np.dtype('int8'): 'GDF_INT8',
        np.dtype('bool_'): 'GDF_BOOL8',
        np.dtype('datetime64[s]'): 'GDF_DATE64',
        np.dtype('datetime64[ms]'): 'GDF_DATE64',
        np.dtype('datetime64[ns]'): 'GDF_TIMESTAMP',
        np.dtype('datetime64[us]'): 'GDF_TIMESTAMP',
        np.dtype('datetime64'): 'GDF_DATE64',
        np.dtype('object_'): 'GDF_STRING',
        np.dtype('str_'): 'GDF_STRING',
        np.dtype('<M8[s]'): 'GDF_DATE64',
        np.dtype('<M8[ms]'): 'GDF_DATE64',
        np.dtype('<M8[ns]'): 'GDF_TIMESTAMP',
        np.dtype('<M8[us]'): 'GDF_TIMESTAMP'
    }
    ret = dtypes[np.dtype(dtype)]
    return ret


def checkSocket(socketNum):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
            #print(e)
    s.close()
    return socket_free


def initializeBlazing(ralId=0, networkInterface='lo', singleNode=False,
        allocator="managed", pool=True,initial_pool_size=None, enable_logging=False):

    #print(networkInterface)
    workerIp = ni.ifaddresses(networkInterface)[ni.AF_INET][0]['addr']
    ralCommunicationPort = random.randint(10000, 32000) + ralId
    while checkSocket(ralCommunicationPort) == False:
        ralCommunicationPort = random.randint(10000, 32000) + ralId

    cudf.set_allocator(allocator=allocator,
                        pool=pool,
                        initial_pool_size=initial_pool_size,# Default is 1/2 total GPU memory
                        enable_logging=enable_logging)

    cio.initializeCaller(
        ralId,
        0,
        networkInterface.encode(),
        workerIp.encode(),
        ralCommunicationPort,
        singleNode)
    cwd = os.getcwd()
    return ralCommunicationPort, workerIp, cwd


def getNodePartitions(df, client):
    df = df.persist()
    workers = client.scheduler_info()['workers']
    connectionToId = {}
    for worker in workers:
        connectionToId[worker] = workers[worker]['name']
    dask.distributed.wait(df)
    #print(client.who_has(df))
    worker_part = client.who_has(df)
    worker_partitions = {}
    for key in worker_part:
        worker = worker_part[key][0]
        partition = int(key[key.find(",") + 2:(len(key) - 1)])
        if connectionToId[worker] not in worker_partitions:
            worker_partitions[connectionToId[worker]] = []
        worker_partitions[connectionToId[worker]].append(partition)
    #print("worker partitions")
    #print(worker_partitions)
    return worker_partitions


def collectPartitionsRunQuery(
        masterIndex,
        nodes,
        tables,
        fileTypes,
        ctxToken,
        algebra,
        accessToken):
    import dask.distributed
    worker_id = dask.distributed.get_worker().name
    for table_name in tables:
        if(isinstance(tables[table_name].input, dask_cudf.core.DataFrame)):
            partitions = tables[table_name].get_partitions(worker_id)
            if (len(partitions) == 0):
                tables[table_name].input = tables[table_name].input.get_partition(
                    0).head(0)
            elif (len(partitions) == 1):
                tables[table_name].input = tables[table_name].input.get_partition(
                    partitions[0]).compute(scheduler='threads')
            else:
                table_partitions = []
                for partition in partitions:
                    table_partitions.append(
                        tables[table_name].input.get_partition(partition).compute())
                tables[table_name].input = cudf.concat(table_partitions)
    return cio.runQueryCaller(
        masterIndex,
        nodes,
        tables,
        fileTypes,
        ctxToken,
        algebra,
        accessToken)

# returns a map of table names to the indices of the columns needed. If there are more than one table scan for one table, it merged the needed columns
# if the column list is empty, it means we want all columns
def mergeTableScans(tableScanInfo):
    table_names = tableScanInfo.keys()
    table_columns = {}
    for table_name in table_names:
        table_columns[table_name] = []

    for table_name in table_names:
        for index in range(0, len(tableScanInfo[table_name]['table_columns'])):
            if len(tableScanInfo[table_name]['table_columns'][index]) > 0:
                table_columns[table_name] = list(set(table_columns[table_name] + tableScanInfo[table_name]['table_columns'][index]))
                table_columns[table_name].sort()
            else: # if the column list is empty, it means we want all columns
                table_columns[table_name] = []
                break

    return table_columns

def modifyAlegebraAndTablesForArrowBasedOnColumnUsage(algebra, tableScanInfo, originalTables, table_columns_in_use):
    newTables={}
    for table_name in tableScanInfo:
        if originalTables[table_name].fileType == DataType.ARROW:
            newTables[table_name] = originalTables[table_name].filterAndRemapColumns(table_columns_in_use[table_name])
            for index in range(0,len(tableScanInfo[table_name]['table_scans'])):
                orig_scan = tableScanInfo[table_name]['table_scans'][index]
                orig_col_indexes = tableScanInfo[table_name]['table_columns'][index]
                table_columns_we_want = table_columns_in_use[table_name]

                new_col_indexes = []
                if len(table_columns_we_want) > 0:
                    if orig_col_indexes == table_columns_we_want:
                        new_col_indexes = list(range(0, len(orig_col_indexes)))
                    else:
                        for new_index, merged_col_index in enumerate(table_columns_we_want):
                            if merged_col_index in orig_col_indexes:
                                new_col_indexes.append(new_index)

                orig_project = 'projects=[' + str(orig_col_indexes) + ']'
                new_project = 'projects=[' + str(new_col_indexes) + ']'
                new_scan = orig_scan.replace(orig_project, new_project)
                algebra = algebra.replace(orig_scan, new_scan)
        else:
            newTables[table_name] = originalTables[table_name]

    return newTables, algebra


def parseHiveMetadataFor(curr_table, file_subset, partitions):
    metadata = {}
    names = []
    n_cols = len(curr_table.input.columns)
    dtypes = curr_table.input.dtypes
    columns = curr_table.input.columns
    n_files = len(file_subset)
    col_indexes = {} 
    for index in range(n_cols):
        col_name = columns[index]
        names.append('min_' + str(index) + '_' + col_name) 
        names.append('max_' + str(index) + '_' + col_name) 
        col_indexes[col_name] = index
        
    names.append('file_handle_index') 
    names.append('row_group_index') 
    minmax_metadata_table = [[] for _ in range(2 * n_cols + 2)]
    table_partition = {}
    for file_index, partition_name  in enumerate(partitions):
        curr_table = partitions[partition_name]
        for col_name, col_value_id in curr_table:
            table_partition.setdefault(col_name, []).append(col_value_id)
        minmax_metadata_table[len(minmax_metadata_table) - 2].append(file_index)
        minmax_metadata_table[len(minmax_metadata_table) - 1].append(0)

    for index in range(n_cols):
        col_name = columns[index]
        if col_name in table_partition:
            col_value_ids = table_partition[col_name]
            index = col_indexes[col_name] 
            minmax_metadata_table[2*index] = col_value_ids
            minmax_metadata_table[2*index+1] = col_value_ids
        else:
            if dtypes[col_name] == np.object or dtypes[col_name] == np.dtype('datetime64[ms]') or dtypes[col_name] == np.datetime64:
                return cudf.DataFrame({})
            minmax_metadata_table[2*index] = [np.iinfo(dtypes[col_name]).min] * n_files
            minmax_metadata_table[2*index+1] = [np.iinfo(dtypes[col_name]).max] * n_files

    series = []
    for index in range(n_cols):
        col_name = columns[index]
        col1 = pd.Series(minmax_metadata_table[2*index], dtype=dtypes[col_name], name=names[2*index])
        col2 = pd.Series(minmax_metadata_table[2*index+1], dtype=dtypes[col_name], name=names[2*index+1])
        series.append(col1)
        series.append(col2)
    index = n_cols 

    col1 = pd.Series(minmax_metadata_table[2*index], dtype=dtypes[col_name], name=names[2*index])
    col2 = pd.Series(minmax_metadata_table[2*index+1], dtype=dtypes[col_name], name=names[2*index+1])
    series.append(col1)
    series.append(col2)

    frame = OrderedDict(((key,value) for (key,value) in zip(names, series)))
    metadata = cudf.DataFrame(frame)
    return metadata


def mergeMetadataFor(curr_table, fileMetadata, hiveMetadata, extra_columns):
    result = fileMetadata
    columns = curr_table.input.columns
    n_cols = len(curr_table.input.columns)

    names = []
    col_indexes = {}
    counter = 0
    for index in range(n_cols):
        col_name = columns[index]
        names.append('min_' + str(index) + '_' + col_name) 
        names.append('max_' + str(index) + '_' + col_name) 
        col_indexes[col_name] = index
    names.append('file_handle_index') 
    names.append('row_group_index') 

    for (col_name, col_dtype) in extra_columns:
        index = col_indexes[col_name]
        min_name = 'min_' + str(index) + '_' + col_name
        max_name = 'max_' + str(index) + '_' + col_name
        if result.shape[0] == hiveMetadata.shape[0]:
            result[min_name] = hiveMetadata[min_name]
            result[max_name] = hiveMetadata[max_name]
        else:
            print('TODO: duplicate data based on rowgroups and file_index info')
            return hiveMetadata 
    # reorder dataframes using original min_max col_name order
    series = []
    for col_name in names:
        col = result[col_name]
        series.append(col)

    frame = OrderedDict(((key,value) for (key,value) in zip(names, series)))
    result = cudf.DataFrame(frame)
    return result
    
class BlazingTable(object):
    def __init__(
            self,
            input,
            fileType,
            files=None,
            datasource=[],
            calcite_to_file_indices=None,
            num_row_groups=None,
            args={},
            convert_gdf_to_dask=False,
            convert_gdf_to_dask_partitions=1,
            client=None,
            uri_values=[],
            in_file=[],
            force_conversion=False,
            metadata=None):
        self.fileType = fileType
        if fileType == DataType.ARROW:
            if force_conversion:
                #converts to cudf for querying
                self.input = cudf.DataFrame.from_arrow(input)
                self.fileType = DataType.CUDF
            else:
                self.input = cudf.DataFrame.from_arrow(input.schema.empty_table())
                self.arrow_table = input
        else:
            self.input = input

        self.calcite_to_file_indices = calcite_to_file_indices
        self.files = files

        self.datasource = datasource
        # TODO, cc @percy, @cristian!
        # num_row_groups: this property is computed in create_table.parse_schema, but not used in run_query.
        self.num_row_groups = num_row_groups

        self.args = args
        if fileType == DataType.CUDF or DataType.DASK_CUDF:
            if(convert_gdf_to_dask and isinstance(self.input, cudf.DataFrame)):
                self.input = dask_cudf.from_cudf(
                    self.input, npartitions=convert_gdf_to_dask_partitions)
            if(isinstance(self.input, dask_cudf.core.DataFrame)):
                self.dask_mapping = getNodePartitions(self.input, client)
        self.uri_values = uri_values
        self.in_file = in_file
        
        # slices, this is computed in create table, and then reused in sql method
        self.slices = None 
        # metadata, this is computed in create table, after call get_metadata
        self.metadata = metadata 
        # row_groups_ids, vector<vector<int>> one vector of row_groups per file
        self.row_groups_id = []
        # a pair of values with the startIndex and batchSize info for each slice
        self.offset = (0,0)


    def has_metadata(self) :
        if isinstance(self.metadata, dask_cudf.core.DataFrame):
            return not self.metadata.compute().empty
        if self.metadata is not None :
            return not self.metadata.empty
        return False

    def filterAndRemapColumns(self,tableColumns):
        #only used for arrow
        if len(tableColumns) == 0: # len = 0 means all columns
            return BlazingTable(self.arrow_table,DataType.ARROW,force_conversion=True)
        
        new_table = self.arrow_table

        columns = []
        names = []
        i = 0
        for column in new_table.itercolumns():
            for index in tableColumns:
                if i == index:
                    names.append(self.arrow_table.field(i).name)
                    columns.append(column)
            i = i + 1
        new_table = pyarrow.Table.from_arrays(columns,names=names)
        new_table = BlazingTable(new_table,DataType.ARROW,force_conversion=True)

        return new_table

    def convertForQuery(self):
        return BlazingTable(self.arrow_table,DataType.ARROW,force_conversion=True)

# until this is implemented we cant do self join with arrow tables
#    def unionColumns(self,otherTable):

    def getSlices(self, numSlices):
        nodeFilesList = []
        if self.files is None:
            for i in range(0, numSlices):
                nodeFilesList.append(BlazingTable(self.input, self.fileType))
            return nodeFilesList
        remaining = len(self.files)
        startIndex = 0
        for i in range(0, numSlices):
            batchSize = int(remaining / (numSlices - i))
            tempFiles = self.files[startIndex: startIndex + batchSize]
            uri_values = self.uri_values[startIndex: startIndex + batchSize]
            if isinstance(self.metadata, cudf.DataFrame) or self.metadata is None:
                slice_metadata = self.metadata
            else:
                slice_metadata = self.metadata.get_partition(i).compute()

            if self.num_row_groups is not None:
                bt = BlazingTable(self.input,
                                                  self.fileType,
                                                  files=tempFiles,
                                                  calcite_to_file_indices=self.calcite_to_file_indices,
                                                  num_row_groups=self.num_row_groups[startIndex: startIndex + batchSize],
                                                  uri_values=uri_values,
                                                  args=self.args,
                                                  metadata=slice_metadata,
                                                  in_file=self.in_file)
                bt.offset = (startIndex, batchSize)
                nodeFilesList.append(bt)
            else:
                bt = BlazingTable(
                        self.input,
                        self.fileType,
                        files=tempFiles,
                        calcite_to_file_indices=self.calcite_to_file_indices,
                        uri_values=uri_values,
                        args=self.args,
                        metadata=slice_metadata,
                        in_file=self.in_file)
                bt.offset = (startIndex, batchSize)
                nodeFilesList.append(bt)
            startIndex = startIndex + batchSize
            remaining = remaining - batchSize
        return nodeFilesList 

    def get_partitions(self, worker):
        return self.dask_mapping[worker]

class BlazingContext(object):

    def __init__(self, 
                dask_client=None, # if None, it will run in single node
                network_interface=None, 
                allocator="managed", # options are "default" or "managed". Where "managed" uses Unified Virtual Memory (UVM) and may use system memory if GPU memory runs out
                pool=True, # if True, it will allocate a memory pool in the beginning. This can greatly improve performance
                initial_pool_size=None, # Initial size of memory pool in bytes (if pool=True). If None, it will default to using half of the GPU memory
                enable_logging=False): # If set to True the memory allocator logging will be enabled, but can negatively impact perforamance
        """
        :param connection: BlazingSQL cluster URL to connect to
            (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
        """
        self.lock = Lock()
        self.finalizeCaller = ref(cio.finalizeCaller)
        self.dask_client = dask_client
        self.nodes = []
        self.node_cwds = []
        self.finalizeCaller = lambda: NotImplemented

        if(dask_client is not None):
            if network_interface is None:
                network_interface = 'eth0'

            worker_list = []
            dask_futures = []
            masterIndex = 0
            i = 0
            ##print(network_interface)
            for worker in list(self.dask_client.scheduler_info()["workers"]):
                dask_futures.append(
                    self.dask_client.submit(
                        initializeBlazing,
                        ralId=i,
                        networkInterface=network_interface,
                        singleNode=False,
                        allocator=allocator,
                        pool=pool,
                        initial_pool_size=initial_pool_size,
                        enable_logging=enable_logging,
                        workers=[worker]))
                worker_list.append(worker)
                i = i + 1
            i = 0
            for connection in dask_futures:
                ralPort, ralIp, cwd = connection.result()
                node = {}
                node['worker'] = worker_list[i]
                node['ip'] = ralIp
                node['communication_port'] = ralPort
                #print("ralport is")
                #print(ralPort)
                self.nodes.append(node)
                self.node_cwds.append(cwd)
                i = i + 1
        else:
            ralPort, ralIp, cwd = initializeBlazing(
                ralId=0, networkInterface='lo', singleNode=True, 
                allocator=allocator, pool=pool, initial_pool_size=initial_pool_size, enable_logging=enable_logging)
            node = {}
            node['ip'] = ralIp
            node['communication_port'] = ralPort
            self.nodes.append(node)
            self.node_cwds.append(cwd)

        # NOTE ("//"+) is a neat trick to handle ip:port cases
        #internal_api.SetupOrchestratorConnection(orchestrator_host_ip, orchestrator_port)

        self.fs = FileSystem()

        self.db = DatabaseClass("main")
        self.schema = BlazingSchemaClass(self.db)
        self.generator = RelationalAlgebraGeneratorClass(self.schema)
        self.tables = {}
        self.logs_initialized = False

        # waitForPingSuccess(self.client)
        print("BlazingContext ready")

    def ready(self, wait=False):
        if wait:
            waitForPingSuccess(self.client)
            return True
        else:
            return self.client.ping()

    def __del__(self):
        self.finalizeCaller()

    # BEGIN FileSystem interface

    def localfs(self, prefix, **kwargs):
        return self.fs.localfs(self.dask_client, prefix, **kwargs)

    # Use result, error_msg = hdfs(args) where result can be True|False
    def hdfs(self, prefix, **kwargs):
        return self.fs.hdfs(self.dask_client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        return self.fs.s3(self.dask_client, prefix, **kwargs)

    def gs(self, prefix, **kwargs):
        return self.fs.gs(self.dask_client, prefix, **kwargs)

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

    def explain(self, sql):
        return str(self.generator.getRelationalAlgebraString(sql))

    def add_remove_table(self, tableName, addTable, table=None):
        self.lock.acquire()
        try:
            if(addTable):
                self.db.removeTable(tableName)
                self.tables[tableName] = table
                arr = ArrayClass()
                order = 0
                # print(">>> Table Schema in Calcite")
                for column in table.input.columns:
                    if(isinstance(table.input, dask_cudf.core.DataFrame)):
                        dataframe_column = table.input.head(0)._data[column]
                    else:
                        dataframe_column = table.input._data[column]
                    data_sz = len(dataframe_column)
                    dtype = get_np_dtype_to_gdf_dtype_str(
                        dataframe_column.dtype)
                    dataType = ColumnTypeClass.fromString(dtype)
                    # print("\t: ", column, dtype, order)
                    column = ColumnClass(column, dataType, order)
                    arr.add(column)
                    order = order + 1
                tableJava = TableClass(tableName, self.db, arr)
                self.db.addTable(tableJava)
                self.schema = BlazingSchemaClass(self.db)
                self.generator = RelationalAlgebraGeneratorClass(self.schema)
                # print("<<< Table Schema in Calcite")
            else:
                self.db.removeTable(tableName)
                self.schema = BlazingSchemaClass(self.db)
                self.generator = RelationalAlgebraGeneratorClass(self.schema)
                del self.tables[tableName]
        finally:
            self.lock.release()

    def create_table(self, table_name, input, **kwargs):
        table = None
        extra_columns = []
        uri_values = []
        file_format_hint = kwargs.get('file_format', 'undefined')  # See datasource.file_format
        extra_kwargs = {}
        in_file = []
        is_hive_input = False
        partitions = {}
        if(isinstance(input, hive.Cursor)):
            hive_table_name = kwargs.get('hive_table_name', table_name)
            folder_list, uri_values, file_format_hint, extra_kwargs, extra_columns, in_file, partitions = get_hive_table(
                input, hive_table_name)
            kwargs.update(extra_kwargs)
            input = folder_list
            is_hive_input = True
        if isinstance(input, str):
            input = [input, ]

        if isinstance(input, pandas.DataFrame):
            input = cudf.DataFrame.from_pandas(input)

        if isinstance(input, pyarrow.Table):
            if (self.dask_client is not None):
                input = cudf.DataFrame.from_arrow(input)
            else:
                table = BlazingTable(	
                input,	
                DataType.ARROW)

        if isinstance(input, cudf.DataFrame):
            if (self.dask_client is not None):
                table = BlazingTable(
                    input,
                    DataType.DASK_CUDF,
                    convert_gdf_to_dask=True,
                    convert_gdf_to_dask_partitions=len(
                        self.nodes),
                    client=self.dask_client)
            else:
                table = BlazingTable(input, DataType.CUDF)
        elif isinstance(input, list):
            parsedSchema = self._parseSchema(
                input, file_format_hint, kwargs, extra_columns)
            
            file_type = parsedSchema['file_type']
            table = BlazingTable(
                parsedSchema['columns'],
                file_type,
                files=parsedSchema['files'],
                datasource=parsedSchema['datasource'],
                calcite_to_file_indices=parsedSchema['calcite_to_file_indices'],
                num_row_groups=parsedSchema['num_row_groups'],
                args=parsedSchema['args'],
                uri_values=uri_values,
                in_file=in_file)

            table.slices = table.getSlices(len(self.nodes))
            if is_hive_input and len(extra_columns) > 0:
                parsedMetadata = self._parseHiveMetadata(input, file_format_hint, table.slices, parsedSchema, kwargs, extra_columns, partitions)
                table.metadata = parsedMetadata
            
            if parsedSchema['file_type'] == DataType.PARQUET :
                parsedMetadata = self._parseMetadata(input, file_format_hint, table.slices, parsedSchema, kwargs, extra_columns)
                if is_hive_input:
                    # TODO: The way hive and blazingsql list files is different!!!
                    # alinear cols concretas y virtuales y las filas( files, row_groups )....
                    # table.metadata = self._mergeMetadata(table.slices, parsedMetadata, table.metadata, extra_columns)
                    print('.')
                else:
                    table.metadata = parsedMetadata

        elif isinstance(input, dask_cudf.core.DataFrame):
            table = BlazingTable(
                input,
                DataType.DASK_CUDF,
                client=self.dask_client)
        if table is not None:
            self.add_remove_table(table_name, True, table)
        return table

    def drop_table(self, table_name):
        self.add_remove_table(table_name, False)

    def _parseSchema(self, input, file_format_hint, kwargs, extra_columns):
        if self.dask_client:
            worker = tuple(self.dask_client.scheduler_info()['workers'])[0]
            connection = self.dask_client.submit(
                cio.parseSchemaCaller,
                input,
                file_format_hint,
                kwargs,
                extra_columns,
                workers=[worker])
            return connection.result()
        else:
            return cio.parseSchemaCaller(
                input, file_format_hint, kwargs, extra_columns)


    def _mergeMetadata(self, currentTableNodes, fileMetadata, hiveMetadata, extra_columns): 
        if self.dask_client:
            dask_futures = []
            workers = tuple(self.dask_client.scheduler_info()['workers'])
            worker_id = 0
            for worker in workers: 
                curr_table = currentTableNodes[worker_id]
                file_part_metadata = fileMetadata.get_partition(worker_id).compute()
                hive_part_metadata = hiveMetadata.get_partition(worker_id).compute()

                connection = self.dask_client.submit(mergeMetadataFor, curr_table, file_part_metadata, hive_part_metadata, extra_columns, workers=[worker])
                dask_futures.append(connection)
                worker_id += 1
            return dask.dataframe.from_delayed(dask_futures) 
        else:
            worker_id = 0
            curr_table = currentTableNodes[worker_id]
            return mergeMetadataFor(curr_table, fileMetadata, hiveMetadata, extra_columns)

    def _parseMetadata(self, input, file_format_hint, currentTableNodes, schema, kwargs, extra_columns):
        if self.dask_client:
            dask_futures = []
            workers = tuple(self.dask_client.scheduler_info()['workers'])
            worker_id = 0
            for worker in workers: 
                file_subset = [ file.decode() for file in currentTableNodes[worker_id].files]
                connection = self.dask_client.submit(
                    cio.parseMetadataCaller,
                    file_subset,
                    currentTableNodes[worker_id].offset,
                    schema,
                    file_format_hint,
                    kwargs,
                    extra_columns,
                    workers=[worker])
                dask_futures.append(connection)
                worker_id += 1
            return dask.dataframe.from_delayed(dask_futures)

        else:
            return cio.parseMetadataCaller(
                input, currentTableNodes[0].offset, schema, file_format_hint, kwargs, extra_columns)

    def _parseHiveMetadata(self, input, file_format_hint, currentTableNodes, schema, kwargs, extra_columns, partitions): 
        if self.dask_client:
            dask_futures = []
            workers = tuple(self.dask_client.scheduler_info()['workers'])
            worker_id = 0
            for worker in workers: 
                curr_table = currentTableNodes[worker_id]
                file_subset = [ file.decode() for file in currentTableNodes[worker_id].files]
                connection = self.dask_client.submit(parseHiveMetadataFor, curr_table, file_subset, partitions, workers=[worker])
                dask_futures.append(connection)
                worker_id += 1
            return dask.dataframe.from_delayed(dask_futures) 
        else:
            worker_id = 0
            curr_table = currentTableNodes[worker_id]
            file_subset = [ file.decode() for file in currentTableNodes[worker_id].files]
            return parseHiveMetadataFor(curr_table, file_subset, partitions)
            

    def _optimize_with_skip_data(self, masterIndex, table_name, table_files, nodeTableList, scan_table_query, fileTypes):
            if self.dask_client is None:
                current_table = nodeTableList[0][table_name]
                table_tuple = (table_name, current_table) 
                print("skip-data-frame:", current_table.metadata[['max_2_t_year', 'max_3_t_company_id', 'file_handle_index']])
                file_indices_and_rowgroup_indices = cio.runSkipDataCaller(masterIndex, self.nodes, table_tuple, fileTypes, 0, scan_table_query, 0)
                has_some_error = '__empty__' in file_indices_and_rowgroup_indices
                print("skip-data-frame:", file_indices_and_rowgroup_indices, has_some_error)
                if not has_some_error:
                    file_and_rowgroup_indices = file_indices_and_rowgroup_indices.to_pandas()
                    files = file_and_rowgroup_indices['file_handle_index'].values.tolist()
                    grouped = file_and_rowgroup_indices.groupby('file_handle_index')
                    actual_files = []
                    uri_values = []
                    current_table.row_groups_ids = []
                    for group_id in grouped.groups:
                        row_indices = grouped.groups[group_id].values.tolist()
                        actual_files.append(table_files[group_id])
                        if group_id < len(current_table.uri_values):
                            uri_values.append(current_table.uri_values[group_id])
                        row_groups_col = file_and_rowgroup_indices['row_group_index'].values.tolist()
                        row_group_ids = [row_groups_col[i] for i in row_indices]
                        current_table.row_groups_ids.append(row_group_ids)
                    current_table.files = actual_files
                    current_table.uri_values = uri_values
                print("*****files****: ", current_table.files)
                print("*****uri_values****: ", current_table.uri_values)
            else:
                dask_futures = []
                i = 0
                for node in self.nodes:
                    worker = node['worker']
                    current_table = nodeTableList[i][table_name]
                    table_tuple = (table_name, current_table)
                    dask_futures.append(
                        self.dask_client.submit(
                            cio.runSkipDataCaller,
                            masterIndex, self.nodes, table_tuple, fileTypes, 0, scan_table_query, 0,
                            workers=[worker]))
                    i = i + 1
                result = dask.dataframe.from_delayed(dask_futures)
                for index in range(len(self.nodes)):
                    file_indices_and_rowgroup_indices = result.get_partition(index).compute()
                    has_some_error = '__empty__' in file_indices_and_rowgroup_indices
                    if has_some_error :
                        continue
                    file_and_rowgroup_indices = file_indices_and_rowgroup_indices.to_pandas()
                    files = file_and_rowgroup_indices['file_handle_index'].values.tolist()
                    grouped = file_and_rowgroup_indices.groupby('file_handle_index')
                    actual_files = []
                    uri_values = []
                    current_table.row_groups_ids = []
                    for group_id in grouped.groups:
                        row_indices = grouped.groups[group_id].values.tolist()
                        actual_files.append(table_files[group_id])
                        if group_id < len(current_table.uri_values):
                            uri_values.append(current_table.uri_values[group_id])
                        row_groups_col = file_and_rowgroup_indices['row_group_index'].values.tolist()
                        row_group_ids = [row_groups_col[i] for i in row_indices]
                        current_table.row_groups_ids.append(row_group_ids)
                    current_table.files = actual_files
                    current_table.uri_values = uri_values
                    print("*****files****: ", index, current_table.files)
                    print("*****uri_values****: ", index, current_table.uri_values)

 
    def sql(self, sql, table_list=[], algebra=None):
        # TODO: remove hardcoding
        masterIndex = 0
        nodeTableList = [{} for _ in range(len(self.nodes))]
        fileTypes = []

        if (algebra is None):
            algebra = self.explain(sql)

        if self.dask_client is None:
            relational_algebra_steps = cio.getTableScanInfoCaller(algebra)
        else:
            worker = tuple(self.dask_client.scheduler_info()['workers'])[0]
            connection = self.dask_client.submit(
                cio.getTableScanInfoCaller,
                algebra,
                workers=[worker])
            relational_algebra_steps = connection.result()
        
        table_columns = mergeTableScans(relational_algebra_steps) 
        new_tables, algebra = modifyAlegebraAndTablesForArrowBasedOnColumnUsage(algebra, relational_algebra_steps,self.tables, table_columns)

        for table in new_tables:
            fileTypes.append(new_tables[table].fileType)
            ftype = new_tables[table].fileType
            if(ftype == DataType.PARQUET or ftype == DataType.ORC or ftype == DataType.JSON or ftype == DataType.CSV):
                currentTableNodes = new_tables[table].getSlices(len(self.nodes))
            elif(new_tables[table].fileType == DataType.DASK_CUDF):
                currentTableNodes = []
                for node in self.nodes:
                    currentTableNodes.append(new_tables[table])
            elif(new_tables[table].fileType == DataType.CUDF or new_tables[table].fileType == DataType.ARROW):
                currentTableNodes = []
                for node in self.nodes:
                    currentTableNodes.append(new_tables[table])
            j = 0
            for nodeList in nodeTableList:
                nodeList[table] = currentTableNodes[j]
                j = j + 1
            if new_tables[table].has_metadata():
                scan_table_query = relational_algebra_steps[table]['table_scans'][0]
                self._optimize_with_skip_data(masterIndex, table, new_tables[table].files, nodeTableList, scan_table_query, fileTypes)

        ctxToken = random.randint(0, 64000)
        accessToken = 0
        if (len(table_list) > 0):
            print("NOTE: You no longer need to send a table list to the .sql() funtion")

        if self.dask_client is None:
            result = cio.runQueryCaller(
                        masterIndex,
                        self.nodes,
                        nodeTableList[0],
                        fileTypes,
                        ctxToken,
                        algebra,
                        accessToken)
        else:
            dask_futures = []
            i = 0
            for node in self.nodes:
                worker = node['worker']
                dask_futures.append(
                    self.dask_client.submit(
                        collectPartitionsRunQuery,
                        masterIndex,
                        self.nodes,
                        nodeTableList[i],
                        fileTypes,
                        ctxToken,
                        algebra,
                        accessToken,
                        workers=[worker]))
                i = i + 1
            result = dask.dataframe.from_delayed(dask_futures)
        return result

    # END SQL interface

    # BEGIN LOG interface
    def log(self, query, logs_table_name='bsql_logs'):
        if not self.logs_initialized:
            self.logs_table_name = logs_table_name
            log_files = [self.node_cwds[i] + '/RAL.' + \
                str(i) + '.log' for i in range(0, len(self.node_cwds))]
            #print(log_files)
            dtypes = [
                'date64',
                'int32',
                'str',
                'int32',
                'int16',
                'int16',
                'str',
                'float32',
                'str',
                'int32',
                'str',
                'int32']
            names = [
                'log_time',
                'node_id',
                'type',
                'query_id',
                'step',
                'substep',
                'info',
                'duration',
                'extra1',
                'data1',
                'extra2',
                'data2']
            t = self.create_table(
                self.logs_table_name,
                log_files,
                delimiter='|',
                dtype=dtypes,
                names=names,
                file_format='csv')
            #print("table created")
            #print(t)
            self.logs_initialized = True

        return self.sql(query)
