# NOTE WARNING NEVER CHANGE THIS FIRST LINE!!!! NEVER EVER
import cudf

from cudf._libxx.column import np_to_cudf_types
from cudf._libxx.column import cudf_to_np_types
from cudf.core.column.column import build_column

from collections import OrderedDict
from enum import Enum

from urllib.parse import urlparse

from threading import Lock
from weakref import ref
from pyblazing.apiv2.filesystem import FileSystem
from pyblazing.apiv2 import DataType


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


def initializeBlazing(ralId=0, networkInterface='lo', singleNode=False):
    #print(networkInterface)
    workerIp = ni.ifaddresses(networkInterface)[ni.AF_INET][0]['addr']
    ralCommunicationPort = random.randint(10000, 32000) + ralId
    while checkSocket(ralCommunicationPort) == False:
        ralCommunicationPort = random.randint(10000, 32000) + ralId

    cudf.set_allocator(allocator="managed",
                        pool=True,
                        initial_pool_size=None,# Default is 1/2 total GPU memory
                        enable_logging=False)

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
    table_names = list(set(tableScanInfo['table_names']))
    table_columns = {}
    for table_name in table_names:
        table_columns[table_name] = []

    for index, table_name in enumerate(tableScanInfo['table_names']):
        if len(tableScanInfo['table_columns'][index]) > 0: # if the column list is empty, it means we want all columns
            table_columns[table_name] = list(set(table_columns[table_name] + tableScanInfo['table_columns'][index]))
            table_columns[table_name].sort()
        else:
            table_columns[table_name] = []

    return table_columns

def modifyAlgebraForDataframesWithOnlyWantedColumns(algebra, tableScanInfo,originalTables):
    for table_name in tableScanInfo:
        #TODO: handle situation with multiple tables being joined twice
        if originalTables[table_name].fileType == DataType.ARROW:
            orig_scan = tableScanInfo[table_name]['table_scans'][0]
            orig_col_indexes = tableScanInfo[table_name]['table_columns'][0]
            merged_col_indexes = list(range(len(orig_col_indexes)))

            new_col_indexes = []
            if len(merged_col_indexes) > 0:
                if orig_col_indexes == merged_col_indexes:
                    new_col_indexes = list(range(0, len(orig_col_indexes)))
                else:
                    for new_index, merged_col_index in enumerate(merged_col_indexes):
                        if merged_col_index in orig_col_indexes:
                            new_col_indexes.append(new_index)

            orig_project = 'projects=[' + str(orig_col_indexes) + ']'
            new_project = 'projects=[' + str(new_col_indexes) + ']'
            new_scan = orig_scan.replace(orig_project, new_project)
            algebra = algebra.replace(orig_scan, new_scan)
    return algebra

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

        self.column_names = []
        self.column_types = []

        if fileType == DataType.CUDF:
            self.column_names = [x for x in input._data.keys()]
            self.column_types = [np_to_cudf_types[x.dtype] for x in input._data.values()]

    def has_metadata(self) :
        if isinstance(self.metadata, dask_cudf.core.DataFrame):
            return not self.metadata.compute().empty
        if self.metadata is not None :
            return not self.metadata.empty
        return False

    def filterAndRemapColumns(self,tableColumns):
        #only used for arrow
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
            # #print(batchSize)
            # #print(startIndex)
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
                                                  metadata=slice_metadata)
                bt.offset = (startIndex, batchSize)
                bt.column_names = self.column_names
                bt.column_types = self.column_types
                nodeFilesList.append(bt)
            else:
                bt = BlazingTable(
                        self.input,
                        self.fileType,
                        files=tempFiles,
                        calcite_to_file_indices=self.calcite_to_file_indices,
                        uri_values=uri_values,
                        args=self.args,
                        metadata=slice_metadata)
                bt.offset = (startIndex, batchSize)
                bt.column_names = self.column_names
                bt.column_types = self.column_types
                nodeFilesList.append(bt)
            startIndex = startIndex + batchSize
            remaining = remaining - batchSize
        return nodeFilesList

    def get_partitions(self, worker):
        return self.dask_mapping[worker]


class BlazingContext(object):

    def __init__(self, dask_client=None, network_interface=None):
        """
        :param dask_client: a dask.distributed.Client instance
            (e.g. BlazingContext(dask_client=dask.distributed.Client('127.0.0.1:8786'))

        :param network_interface: network interface name
            (e.g. BlazingContext(dask_client=..., network_interface='eth0'))
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
                ralId=0, networkInterface='lo', singleNode=True)
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

    def __repr__(self):
        return "BlazingContext('%s')" % (self.dask_client)

    def __str__(self):
        return self.dask_client

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

                if(isinstance(table.input, dask_cudf.core.DataFrame)):
                    schema_df_types = []
                    for col in table.input.head(0)._data[column]:
                        schema_df_types.append(np_to_cudf_types(col.dtype))
                else:
                    schema_df_types = table.column_types

                arr = ArrayClass()
                order = 0
                for column in table.column_names:
                    type_id = schema_df_types[order]
                    dataType = ColumnTypeClass.fromTypeId(type_id)
                    column = ColumnClass(column, dataType, order)
                    arr.add(column)
                    order = order + 1
                tableJava = TableClass(tableName, self.db, arr)
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
        extra_columns = []
        uri_values = []
        file_format_hint = kwargs.get(
            'file_format', 'undefined')  # See datasource.file_format
        extra_kwargs = {}
        in_file = []
        if(isinstance(input, hive.Cursor)):
            hive_table_name = kwargs.get('hive_table_name', table_name)
            folder_list, uri_values, file_format_hint, extra_kwargs, extra_columns, in_file = get_hive_table(
                input, hive_table_name)
            kwargs.update(extra_kwargs)
            input = folder_list
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
                parsedSchema['files'],
                file_type,
                files=parsedSchema['files'],
                datasource=parsedSchema['datasource'],
                calcite_to_file_indices=parsedSchema['calcite_to_file_indices'],
                num_row_groups=parsedSchema['num_row_groups'],
                args=parsedSchema['args'],
                uri_values=uri_values,
                in_file=in_file)

            table.column_names = parsedSchema['names']
            table.column_types = parsedSchema['types']

            table.slices = table.getSlices(len(self.nodes))
            if parsedSchema['file_type'] == DataType.PARQUET :
                parsedMetadata = self._parseMetadata(input, file_format_hint, table.slices, parsedSchema, kwargs, extra_columns)
                if isinstance(parsedMetadata, cudf.DataFrame):
                    table.metadata = parsedMetadata
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

    def _optimize_with_skip_data(self, masterIndex, table_name, table_files, nodeTableList, scan_table_query, fileTypes):
            if self.dask_client is None:
                current_table = nodeTableList[0][table_name]
                table_tuple = (table_name, current_table)
                file_indices_and_rowgroup_indices = cio.runSkipDataCaller(masterIndex, self.nodes, table_tuple, fileTypes, 0, scan_table_query, 0)
                if not file_indices_and_rowgroup_indices.empty:
                    file_and_rowgroup_indices = file_indices_and_rowgroup_indices.to_pandas()
                    files = file_and_rowgroup_indices['file_handle_index'].values.tolist()
                    grouped = file_and_rowgroup_indices.groupby('file_handle_index')
                    actual_files = []
                    current_table.row_groups_ids = []
                    for group_id in grouped.groups:
                        row_indices = grouped.groups[group_id].values.tolist()
                        actual_files.append(table_files[group_id])
                        row_groups_col = file_and_rowgroup_indices['row_group_index'].values.tolist()
                        row_group_ids = [row_groups_col[i] for i in row_indices]
                        current_table.row_groups_ids.append(row_group_ids)
                    current_table.files = actual_files
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
                print("SKIP_DATA+++")
                for index in range(len(self.nodes)):
                    file_indices_and_rowgroup_indices = result.get_partition(index).compute()
                    if file_indices_and_rowgroup_indices.empty :
                        continue
                    file_and_rowgroup_indices = file_indices_and_rowgroup_indices.to_pandas()
                    files = file_and_rowgroup_indices['file_handle_index'].values.tolist()
                    grouped = file_and_rowgroup_indices.groupby('file_handle_index')
                    actual_files = []
                    current_table.row_groups_ids = []
                    for group_id in grouped.groups:
                        row_indices = grouped.groups[group_id].values.tolist()
                        actual_files.append(table_files[group_id])
                        row_groups_col = file_and_rowgroup_indices['row_group_index'].values.tolist()
                        row_group_ids = [row_groups_col[i] for i in row_indices]
                        current_table.row_groups_ids.append(row_group_ids)
                    current_table.files = actual_files


    def sql(self, sql, table_list=[], algebra=None):
        # TODO: remove hardcoding
        masterIndex = 0
        nodeTableList = [{} for _ in range(len(self.nodes))]
        fileTypes = []

        if (algebra is None):
            algebra = self.explain(sql)

        if self.dask_client is None:
            new_tables, relational_algebra_steps = cio.getTableScanInfoCaller(algebra,self.tables)
        else:
            worker = tuple(self.dask_client.scheduler_info()['workers'])[0]
            connection = self.dask_client.submit(
                cio.getTableScanInfoCaller,
                algebra,
                self.tables,
                workers=[worker])
            new_tables, relational_algebra_steps = connection.result()

        algebra = modifyAlgebraForDataframesWithOnlyWantedColumns(algebra, relational_algebra_steps,self.tables)

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
            scan_table_query = relational_algebra_steps[table]['table_scans'][0]
            if new_tables[table].has_metadata():
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
