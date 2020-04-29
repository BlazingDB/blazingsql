# NOTE WARNING NEVER CHANGE THIS FIRST LINE!!!! NEVER EVER
import cudf

from cudf._lib.types import np_to_cudf_types, cudf_to_np_types
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

from enum import IntEnum

jpype.addClassPath(
    os.path.join(
        os.getenv("CONDA_PREFIX"),
        'lib/blazingsql-algebra.jar'))
jpype.addClassPath(
    os.path.join(
        os.getenv("CONDA_PREFIX"),
        'lib/blazingsql-algebra-core.jar'))

jvm_path=os.environ["CONDA_PREFIX"]+"/jre/lib/amd64/server/libjvm.so"
jpype.startJVM('-ea', convertStrings=False, jvmpath=jvm_path)

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


class blazing_allocation_mode(IntEnum):
    CudaDefaultAllocation = (0,)
    PoolAllocation = (1,)
    CudaManagedMemory = (2,)


def initializeBlazing(ralId=0, networkInterface='lo', singleNode=False,
                      allocator="managed", pool=False,
                      initial_pool_size=None, enable_logging=False, devices=0):
    
    workerIp = ni.ifaddresses(networkInterface)[ni.AF_INET][0]['addr']
    ralCommunicationPort = random.randint(10000, 32000) + ralId
    while checkSocket(ralCommunicationPort) == False:
        ralCommunicationPort = random.randint(10000, 32000) + ralId

    if (allocator != 'existing'): 

        managed_memory = True if allocator == "managed" else False
        allocation_mode = 0

        if pool:
            allocation_mode |= blazing_allocation_mode.PoolAllocation
        if managed_memory:
            allocation_mode |= blazing_allocation_mode.CudaManagedMemory

        if not pool:
            initial_pool_size = 0
        elif pool and initial_pool_size is None:
            initial_pool_size = 0
        elif pool and initial_pool_size == 0:
            initial_pool_size = 1
            
        if devices is None:
            devices = [0]
        elif isinstance(devices, int):
            devices = [devices]

        cio.blazingSetAllocatorCaller(
            allocation_mode,
            initial_pool_size,
            devices,
            enable_logging
        )
    
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
    worker_part = client.who_has(df)
    worker_partitions = {}
    for key in worker_part:
        if len(worker_part[key]) > 0:
            worker = worker_part[key][0]
            partition = int(key[key.find(",") + 2:(len(key) - 1)])
            if connectionToId[worker] not in worker_partitions:
                worker_partitions[connectionToId[worker]] = []
            worker_partitions[connectionToId[worker]].append(partition)
        else:
            print("ERROR: In getNodePartitions, woker has no corresponding partition")
    return worker_partitions


def collectPartitionsRunQuery(
        masterIndex,
        nodes,
        tables,
        fileTypes,
        ctxToken,
        algebra,
        accessToken,
        use_execution_graph
        ):
    import dask.distributed
    worker_id = dask.distributed.get_worker().name
    for table_name in tables:
        if(isinstance(tables[table_name].input, dask_cudf.core.DataFrame)):
            partitions = tables[table_name].get_partitions(worker_id)
            if partitions is None:
                print("ERROR: In collectPartitionsRunQuery no partitions found for worker " + str(worker_id))
            if (len(partitions) == 0):
                tables[table_name].input = [tables[table_name].input.get_partition(
                    0).head(0)]
            elif (len(partitions) == 1):
                tables[table_name].input = [tables[table_name].input.get_partition(
                    partitions[0]).compute()]
            else:
                print("""WARNING: Running a query on a table that is from a Dask DataFrame currently requires concatenating its partitions at runtime.
This limitation is expected to exist until blazingsql version 0.14.
In the mean time, for better performance we recommend using the unify_partitions utility function prior to creating a Dask DataFrame based table:
    dask_df = bc.unify_partitions(dask_df)
    bc.create_table('my_table', dask_df)""")
                table_partitions = []
                for partition in partitions:
                    table_partitions.append(
                        tables[table_name].input.get_partition(partition).compute())
                if use_execution_graph:
                    tables[table_name].input = table_partitions #no concat
                else:
                    tables[table_name].input = [cudf.concat(table_partitions)]
    return cio.runQueryCaller(
        masterIndex,
        nodes,
        tables,
        fileTypes,
        ctxToken,
        algebra,
        accessToken,
        use_execution_graph)

def collectPartitionsPerformPartition(
        masterIndex,
        nodes,
        ctxToken,
        input,
        dask_mapping,
        by,
        i):  # this is a dummy variable to make every submit unique which is necessary
    import dask.distributed
    worker_id = dask.distributed.get_worker().name
    if(isinstance(input, dask_cudf.core.DataFrame)):
        partitions = dask_mapping[worker_id]
        if (len(partitions) == 0):
            input = input.get_partition(0).head(0)
        elif (len(partitions) == 1):
            input = input.get_partition(partitions[0]).compute()
        else:
            table_partitions = []
            for partition in partitions:
                table_partitions.append(
                    input.get_partition(partition).compute())
            input = cudf.concat(table_partitions)
    return cio.performPartitionCaller(
                    masterIndex,
                    nodes,
                    ctxToken,
                    input,
                    by)

def workerUnifyPartitions(
        input,
        dask_mapping,
        i):  # this is a dummy variable to make every submit unique which is necessary
    import dask.distributed
    worker_id = dask.distributed.get_worker().name
    partitions = dask_mapping[worker_id]
    if (len(partitions) == 0):
        return input.get_partition(0).head(0)
    elif (len(partitions) == 1):
        return input.get_partition(partitions[0]).compute()
    else:
        table_partitions = []
        for partition in partitions:
            table_partitions.append(
                input.get_partition(partition).compute())
        return cudf.concat(table_partitions)


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


def get_uri_values(files, partitions, base_folder):
    base_folder = base_folder + '/'
    uri_values = []
    for file in files:
        file_dir = os.path.dirname(file.decode())
        partition_name = file_dir.replace(base_folder,'')
        if partition_name in partitions:
            uri_values.append(partitions[partition_name])
        else:
            print("ERROR: Could not get partition values for file: " + file.decode())
    return uri_values


def parseHiveMetadata(curr_table, uri_values):
    metadata = {}
    names = []
    final_names = [] # not all columns will have hive metadata, so this vector will capture all the names that will actually be used in the end
    n_cols = len(curr_table.column_names)

    if all(type in cudf_to_np_types for type in curr_table.column_types):
        dtypes = [cudf_to_np_types[t] for t in curr_table.column_types] 
    else:
        for i in range(len(curr_table.column_types)):
            if not (curr_table.column_types[i] in cudf_to_np_types):
                print("ERROR: Column " + curr_table.column_names[i] + " has type that cannot be mapped: " + curr_table.column_types[i])
    columns = [name.decode() for name in curr_table.column_names]
    for index in range(n_cols):
        col_name = columns[index]
        names.append('min_' + str(index) + '_' + col_name)
        names.append('max_' + str(index) + '_' + col_name)

    names.append('file_handle_index')
    names.append('row_group_index')
    minmax_metadata_table = [[] for _ in range(2 * n_cols + 2)]
    table_partition = {}
    for file_index, uri_value in enumerate(uri_values):
        curr_partition = uri_values
        for index, [col_name, col_value_id] in enumerate(uri_value):
            if col_name in columns:
                col_index = columns.index(col_name)
            else:
                print("ERROR: could not find partition column name " + str(col_name) + " in table names")
            if(dtypes[col_index] == np.dtype("object")):
                np_col_value = col_value_id
            elif(dtypes[col_index] == np.dtype("datetime64[s]") or dtypes[col_index] == np.dtype("datetime64[ms]") or dtypes[col_index] == np.dtype("datetime64[us]") or dtypes[col_index] == np.dtype("datetime64[ns]")):
                np_col_value = np.datetime64(col_value_id)
            else:
                np_col_value = np.fromstring(col_value_id, dtypes[col_index], sep=' ')[0]

            table_partition.setdefault(col_name, []).append(np_col_value)
        minmax_metadata_table[len(minmax_metadata_table) - 2].append(file_index)
        # this assumes that you only have one row group per partitioned file but is addressed in the mergeMetadata function,
        # where you will have information about how many rowgroups per file and you can expand the hive metadata accordingly
        minmax_metadata_table[len(minmax_metadata_table) - 1].append(0) # this is the rowgroup index
    for index in range(n_cols):
        col_name = columns[index]
        if col_name in table_partition:
            col_value_ids = table_partition[col_name]
            minmax_metadata_table[2*index] = col_value_ids
            minmax_metadata_table[2*index+1] = col_value_ids

    series = []
    for index in range(n_cols):
        col_name = columns[index]
        if col_name in table_partition:
            if(dtypes[index] == np.dtype("datetime64[s]") or dtypes[index] == np.dtype("datetime64[ms]") or dtypes[index] == np.dtype("datetime64[us]") or dtypes[index] == np.dtype("datetime64[ns]")):
                # when creating a pandas series, for a datetime type, it has to be in ns because that is the only internal datetime representation
                col1 = pd.Series(minmax_metadata_table[2*index], dtype=np.dtype("datetime64[ns]"), name=names[2*index])
                col2 = pd.Series(minmax_metadata_table[2*index+1], dtype=np.dtype("datetime64[ns]"), name=names[2*index+1])
            else:
                col1 = pd.Series(minmax_metadata_table[2*index], dtype=dtypes[index], name=names[2*index])
                col2 = pd.Series(minmax_metadata_table[2*index+1], dtype=dtypes[index], name=names[2*index+1])
            series.append(col1)
            series.append(col2)
            final_names.append(names[2*index])
            final_names.append(names[2*index+1])
    index = n_cols
    col1 = pd.Series(minmax_metadata_table[2*index], dtype=np.int32, name=names[2*index])
    col2 = pd.Series(minmax_metadata_table[2*index+1], dtype=np.int32, name=names[2*index+1])
    final_names.append(names[2*index])
    final_names.append(names[2*index+1])
    series.append(col1)
    series.append(col2)

    frame = OrderedDict(((key,value) for (key,value) in zip(final_names, series)))
    metadata = cudf.DataFrame(frame)
    for index, col_type in enumerate(dtypes):
        min_col_name = names[2*index]
        max_col_name = names[2*index+1]
        if(dtypes[index] == np.dtype("datetime64[s]") or dtypes[index] == np.dtype("datetime64[ms]") or dtypes[index] == np.dtype("datetime64[us]") or dtypes[index] == np.dtype("datetime64[ns]")):
            if (min_col_name in metadata) and (max_col_name in metadata):
                if metadata[min_col_name].dtype != dtypes[index] or metadata[max_col_name].dtype != dtypes[index]:
                    # here we are casting the timestamp types from ns to their correct desired types
                    metadata[min_col_name] = metadata[min_col_name].astype(dtypes[index])
                    metadata[max_col_name] = metadata[max_col_name].astype(dtypes[index])
    return metadata


def mergeMetadata(curr_table, fileMetadata, hiveMetadata):

    if fileMetadata.shape[0] != hiveMetadata.shape[0]:
        print('ERROR: number of rows from fileMetadata: ' + str(fileMetadata.shape[0]) + ' does not match hiveMetadata: ' + str(hiveMetadata.shape[0]))
        return hiveMetadata

    if not fileMetadata['file_handle_index'].equals(hiveMetadata['file_handle_index']):
        print('ERROR: file_handle_index of fileMetadata does not match the same order as in hiveMetadata')
        return hiveMetadata

    result = fileMetadata
    columns = [c.decode() for c in curr_table.column_names]
    n_cols = len(curr_table.column_names)

    names = []
    final_names = [] # not all columns will have hive metadata, so this vector will capture all the names that will actually be used in the end
    for index in range(n_cols):
        col_name = columns[index]
        names.append('min_' + str(index) + '_' + col_name)
        names.append('max_' + str(index) + '_' + col_name)
    names.append('file_handle_index')
    names.append('row_group_index')

    for col_name in hiveMetadata._data.keys():
        result[col_name] = hiveMetadata[col_name]

    result_col_names = [col_name for col_name in result._data.keys()]

    # reorder dataframes using original min_max col_name order
    series = []
    for col_name in names:
        if col_name in result_col_names:
            col = result[col_name]
            series.append(col)
            final_names.append(col_name)

    frame = OrderedDict(((key,value) for (key,value) in zip(final_names, series)))
    result = cudf.DataFrame(frame)
    return result


import json
import collections
def is_double_children(expr):
    return "LogicalJoin" in expr  or  "LogicalUnion" in expr

def visit (lines):
    stack = collections.deque()
    root_level = 0
    dicc = {
        "expr": lines[root_level][1],
        "children": []
    }
    processed = set()
    for index in range(len(lines)):
        child_level, expr = lines[index]
        if child_level == root_level + 1:
            new_dicc = {
                "expr": expr,
                "children": []
            }
            if len(dicc["children"]) == 0:
                dicc["children"] = [new_dicc]
            else:
                dicc["children"].append(new_dicc)
            stack.append( (index, child_level, expr, new_dicc) )
            processed.add(index)

    for index in processed:
        lines[index][0] = -1

    while len(stack) > 0:
        curr_index, curr_level, curr_expr, curr_dicc = stack.pop()
        processed = set()

        if curr_index < len(lines)-1: # is brother
            child_level, expr = lines[curr_index+1]
            if child_level == curr_level:
                continue
            elif child_level == curr_level + 1:
                index = curr_index + 1
                if is_double_children(curr_expr):
                    while index < len(lines) and len(curr_dicc["children"]) < 2:
                        child_level, expr = lines[index]
                        if child_level == curr_level + 1:
                            new_dicc = {
                                "expr": expr,
                                "children": []
                            }
                            if len(curr_dicc["children"]) == 0:
                                curr_dicc["children"] = [new_dicc]
                            else:
                                curr_dicc["children"].append(new_dicc)
                            processed.add(index)
                            stack.append( (index, child_level, expr, new_dicc) )
                        index += 1
                else:
                    while index < len(lines) and len(curr_dicc["children"]) < 1:
                        child_level, expr = lines[index]
                        if child_level == curr_level + 1:
                            new_dicc = {
                                "expr": expr,
                                "children": []
                            }
                            if len(curr_dicc["children"]) == 0:
                                curr_dicc["children"] = [new_dicc]
                            else:
                                curr_dicc["children"].append(new_dicc)
                            processed.add(index)
                            stack.append( (index, child_level, expr, new_dicc) )
                        index += 1

        for index in processed:
            lines[index][0] = -1
    return json.dumps(dicc)


def get_plan(algebra):
    algebra = algebra.replace("  ", "\t")
    lines = algebra.split("\n")
    new_lines = []
    for i in range(len(lines) - 1):
        line = lines[i]
        level = line.count("\t")
        new_lines.append( [level, line.replace("\t", "")] )
    return visit(new_lines)


def resolve_relative_path(files):
    files_out = []
    for file in files:
        if isinstance(file, str):
            # if its an abolute path or fs path
            if file.startswith('/') |  file.startswith('hdfs://') | file.startswith('s3://') | file.startswith('gs://'):
                files_out.append(file)
            else: # if its not, lets see if its a relative path we can access
                abs_file = os.path.abspath(os.path.join(os.getcwd(), file))
                if os.path.exists(abs_file):
                    files_out.append(abs_file)
                else: # if its not, lets just leave it and see if somehow the engine can access it
                    files_out.append(file)
        else: # we are assuming all are string. If not, lets just return
            return files
    return files_out

# this is to handle the cases where there is a file that does not actually have data
# files that do not have data wont show up in the metadata and we will want to remove them from the table schema
def adjust_due_to_missing_rowgroups(metadata, files):
    metadata_ids = metadata[['file_handle_index', 'row_group_index']].to_pandas()
    grouped = metadata_ids.groupby('file_handle_index')
    new_files = []
    missing_file_inds = []
    prev_group_id = -1
    for group_id in grouped.groups:
        if group_id != -1:
            new_files.append(files[group_id])
        else:
            missing_file_inds.append(prev_group_id + 1)
        prev_group_id = group_id

    missing_file_inds = list(reversed(missing_file_inds))
    for ind in missing_file_inds:
        mask = metadata['file_handle_index'] > ind
        metadata['file_handle_index'][mask] = metadata['file_handle_index'][mask] - 1
    return metadata, new_files


class BlazingTable(object):
    def __init__(
            self,
            input,
            fileType,
            files=None,
            datasource=[],
            calcite_to_file_indices=None,
            args={},
            convert_gdf_to_dask=False,
            convert_gdf_to_dask_partitions=1,
            client=None,
            uri_values=[],
            in_file=[],
            force_conversion=False,
            metadata=None,
            row_groups_ids = []): # row_groups_ids, vector<vector<int>> one vector of row_groups per file
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

        self.args = args
        if fileType == DataType.CUDF or DataType.DASK_CUDF:
            if(convert_gdf_to_dask and isinstance(self.input, cudf.DataFrame)):
                self.input = dask_cudf.from_cudf(
                    self.input, npartitions=convert_gdf_to_dask_partitions)
            if(isinstance(self.input, dask_cudf.core.DataFrame)):
                self.input = self.input.persist()
                self.dask_mapping = getNodePartitions(self.input, client)
        self.uri_values = uri_values
        self.in_file = in_file

        # slices, this is computed in create table, and then reused in sql method
        self.slices = None
        # metadata, this is computed in create table, after call get_metadata
        self.metadata = metadata
        # row_groups_ids, vector<vector<int>> one vector of row_groups per file
        self.row_groups_ids = row_groups_ids
        # a pair of values with the startIndex and batchSize info for each slice
        self.offset = (0,0)

        self.column_names = []
        self.column_types = []

        if fileType == DataType.CUDF:
            self.column_names = [x for x in self.input._data.keys()]
            self.column_types = [np_to_cudf_types[x.dtype] for x in self.input._data.values()]
        elif fileType == DataType.DASK_CUDF:
            self.column_names = [x for x in input.columns]
            self.column_types = [np_to_cudf_types[x] for x in input.dtypes]

        # file_column_names are usually the same as column_names, except for when
        # in a hive table the column names defined by the hive schema
        # are different that the names in actual files
        self.file_column_names = self.column_names

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
            tempFiles = self.files[startIndex: startIndex + batchSize]
            uri_values = self.uri_values[startIndex: startIndex + batchSize]

            slice_row_groups_ids = []
            if self.row_groups_ids is not None:
                slice_row_groups_ids=self.row_groups_ids[startIndex: startIndex + batchSize]

            bt = BlazingTable(self.input,
                                self.fileType,
                                files=tempFiles,
                                calcite_to_file_indices=self.calcite_to_file_indices,
                                uri_values=uri_values,
                                args=self.args,
                                row_groups_ids=slice_row_groups_ids,
                                in_file=self.in_file)
            bt.offset = (startIndex, batchSize)
            bt.column_names = self.column_names
            bt.file_column_names = self.file_column_names
            bt.column_types = self.column_types
            nodeFilesList.append(bt)

            startIndex = startIndex + batchSize
            remaining = remaining - batchSize

        return nodeFilesList

    def get_partitions(self, worker):
        if worker in self.dask_mapping:
            return self.dask_mapping[worker]
        else:
            return None


class BlazingContext(object):
    """
    BlazingContext is the Python API of BlazingSQL. Along with initialization arguments allowing for
    easy multi-GPU distribution, the BlazingContext class has a number of methods which assist not only
    in creating and querying tables, but also in connecting remote data sources and understanding your ETL.

    Docs: https://docs.blazingdb.com/docs/blazingcontext
    """

    def __init__(self, dask_client=None, network_interface=None, allocator="managed",
                 pool=False, initial_pool_size=None, enable_logging=False):
        """
        Create a BlazingSQL API instance.

        Parameters
        -------------------

        dask_client (optional) : dask.distributed.Client instance. only necessary for distributed query execution.
        network_interface (optional) : for communicating with the dask-scheduler. see note below.
        allocator (optional) :  "managed" or "default", where "managed" uses Unified Virtual Memory (UVM)
                                and may use system memory if GPU memory runs out, "default" assumes rmm
                                allocator is already set and does not initialize it.
        pool (optional) : if True, BlazingContext will self-allocate a GPU memory pool. can greatly improve performance.
        initial_pool_size (optional) : initial size of memory pool in bytes (if pool=True).
                                       if None, and pool=True, defaults to 1/2 GPU memory.
        enable_logging (optional) : if True, memory allocator logging will be enabled. can negatively impact perforamance.

        Examples
        --------

        Initialize BlazingContext (single-GPU):

        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        BlazingContext ready


        For distributed (multi-GPU) query execution:

        >>> from blazingsql import BlazingContext
        >>> from dask_cuda import LocalCUDACluster
        >>> from dask.distributed import Client

        >>> cluster = LocalCUDACluster()
        >>> client = Client(cluster)
        >>> bc = BlazingContext(dask_client=client, network_interface='lo')
        BlazingContext ready


        Note: When using BlazingSQL with multiple nodes, you will need to set the correct network_interface your
        servers are using to communicate with the IP address of the dask-scheduler. You can see the different network
        interfaces and what IP addresses they serve with the bash command ifconfig. The default is set to 'eth0'.
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

    def __repr__(self):
        return "BlazingContext('%s')" % (self.dask_client)

    def __str__(self):
        return self.dask_client

    # BEGIN FileSystem interface

    def localfs(self, prefix, **kwargs):
        return self.fs.localfs(self.dask_client, prefix, **kwargs)

    # Use result, error_msg = hdfs(args) where result can be True|False
    def hdfs(self, prefix, **kwargs):
        """
        Register a Hadoop Distributed File System (HDFS) Cluster.

        Parameters
        ----------

        name : string that represents the name with which you will refer to your HDFS cluster.
        host : string IP Address of your HDFS NameNode.
        port : integer of the Port number of your HDFS NameNode.
        user : string of the HDFS User on your NameNode.
        kerb_ticket (optional) : string file path to your ticket for kerberos authentication.

        You may also need to set the following environment variables to properly interface with HDFS.
        HADOOP_HOME: the root of your installed Hadoop distribution.
        JAVA_HOME: the location of your Java SDK installation (should point to CONDA_PREFIX).
        ARROW_LIBHDFS_DIR: explicit location of libhdfs.so if not installed at $HADOOP_HOME/lib/native.
        CLASSPATH: must contain the Hadoop jars.

        Examples
        --------

        Register and create table from HDFS:

        >>> bc.hdfs('dir_name', host='name_node_ip', port=port_number, user='hdfs_user')
        >>> bc.create_table('table_name', 'hdfs://dir_name/file.csv')
        <pyblazing.apiv2.context.BlazingTable at 0x7f11897c0310>


        Docs: https://docs.blazingdb.com/docs/hdfs
        """
        return self.fs.hdfs(self.dask_client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        """
        Register an AWS S3 bucket.

        Parameters
        ----------

        name : string that represents the name with which you will refer to your S3 bucket.
        bucket_name : string name of your S3 bucket.
        access_key_id : string of your AWS IAM access key. not required for public buckets.
        secret_key : string of your AWS IAM secret key. not required for public buckets.
        encryption_type (optional) : None (default), 'AES_256', or 'AWS_KMS'.
        session_token (optional) : string of your AWS IAM session token.
        root (optional) : string path of your bucket that will be used as a shortcut path.
        kms_key_amazon_resource (optional) : string value, required for KMS encryption only.

        Examples
        --------

        Register and create table from a public S3 bucket:

        >>> bc.s3('blazingsql-colab', bucket_name='blazingsql-colab')
        >>> bc.create_table('taxi', 's3://blazingsql-colab/yellow_taxi/1_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f6d4e640c90>


        Register and create table from a private S3 bucket:
        >>> bc.s3('other-data', bucket_name='kws-parquet-data', access_key_id='AKIASPFMPQMQD2OG54IQ',
        >>>       secret_key='bmt+TLTosdkIelsdw9VQjMe0nBnvAA5nPt0kaSx/Y', encryption_type=S3EncryptionType.AWS_KMS,
        >>>       kms_key_amazon_resource_name='arn:aws:kms:region:acct-id:key/key-id')
        >>> bc.create_table('taxi', 's3://other-data/yellow_taxi/1_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f12327c0310>


        Docs: https://docs.blazingdb.com/docs/s3
        """
        return self.fs.s3(self.dask_client, prefix, **kwargs)

    def gs(self, prefix, **kwargs):
        """
        Register a Google Storage bucket.

        Parameters
        ----------

        name : string that represents the name with which you will refer to your GS bucket.
        project_id : string name of your Google Cloud Platform project.
        bucket_name : string of the name of your GS bucket.
        use_default_adc_json_file (optional) : boolean, whether or not to use the default GCP ADC JSON.
        adc_json_file (optional) : string with the location of your custom ADC JSON.

        Examples
        --------

        Register and create table from a GS bucket:

        >>> bc.gs('gs_1gb', project_id='blazingsql-enduser', bucket_name='bsql')
        >>> bc.create_table('nation', 'gs://gs_1gb/tpch_sf1/nation/0_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f11897c0310>


        Docs: https://docs.blazingdb.com/docs/google-storage
        """
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
        """
        Returns break down of a given query's Logical Relational Algebra plan.

        Parameters
        ----------

        sql : string SQL query.

        Examples
        --------

        Explain this UNION query:

        >>> query = '''
        >>>         SELECT a.*
        >>>         FROM taxi_1 as a
        >>>         UNION ALL
        >>>         SELECT b.*
        >>>         FROM taxi_2 as b
        >>>             WHERE b.fare_amount < 100 OR b.passenger_count <> 4
        >>>             '''
        >>> plan = bc.explain(query)
        >>> print(plan)
        LogicalUnion(all=[true])
          LogicalTableScan(table=[[main, taxi_1]])
          BindableTableScan(table=[[main, taxi_2]], filters=[[OR(<($12, 100), <>($3, 4))]])


        Docs: https://docs.blazingdb.com/docs/explain
        """
        try:
            algebra = str(self.generator.getRelationalAlgebraString(sql))
        except jpype.JException as exception:
            algebra = ""
            print("SQL Parsing Error")
            print(exception.message())
        if algebra.startswith("fail:"):
            print("Error found")
            print(algebra)
            algebra=""
        return algebra

    def add_remove_table(self, tableName, addTable, table=None):
        self.lock.acquire()
        try:
            if(addTable):
                self.db.removeTable(tableName)
                self.tables[tableName] = table

                arr = ArrayClass()
                for order, column in enumerate(table.column_names):
                    type_id = table.column_types[order]
                    dataType = ColumnTypeClass.fromTypeId(type_id)
                    column = ColumnClass(column, dataType, order)
                    arr.add(column)
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
        """
        Create a BlazingSQL table.

        Parameters
        ----------

        table_name : string of table name.
        input : data source for table.
                cudf.Dataframe, dask_cudf.DataFrame, pandas.DataFrame, filepath for csv, orc, parquet, etc...

        Examples
        --------

        Create table from cudf.DataFrame:

        >>> import cudf
        >>> df = cudf.DataFrame()
        >>> df['a'] = [6, 9, 1, 6, 2]
        >>> df['b'] = [7, 2, 7, 1, 2]

        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        BlazingContext ready
        >>> bc.create_table('sample_df', df)
        <pyblazing.apiv2.context.BlazingTable at 0x7f22f58371d0>

        Create table from local file in 'data' directory:

        >>> bc.create_table('taxi', 'data/nyc_taxi.csv', header=0)
        <pyblazing.apiv2.context.BlazingTable at 0x7f73893c0310>


        Register and create table from a public AWS S3 bucket:

        >>> bc.s3('blazingsql-colab', bucket_name='blazingsql-colab')
        >>> bc.create_table('taxi', 's3://blazingsql-colab/yellow_taxi/1_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f09264c0310>


        Docs: https://docs.blazingdb.com/docs/create_table
        """
        table = None
        extra_kwargs = {}
        in_file = []
        is_hive_input = False
        partitions = {}
        extra_columns = []

        file_format_hint = kwargs.get('file_format', 'undefined')  # See datasource.file_format
        user_partitions = kwargs.get('partitions', None) # these are user defined partitions should be a dictionary object of the form partitions={'col_nameA':[val, val], 'col_nameB':['str_val', 'str_val']}
        if user_partitions is not None and type(user_partitions) != type({}):
            print("ERROR: User defined partitions should be a dictionary object of the form partitions={'col_nameA':[val, val], 'col_nameB':['str_val', 'str_val']}")
            return
        user_partitions_schema = kwargs.get('partitions_schema', None) # for user defined partitions, partitions_schema should be a list of tuples of the column name and column type of the form partitions_schema=[('col_nameA','int32','col_nameB','str')]
        if user_partitions_schema is not None:
            if user_partitions is None:
                print("ERROR: 'partitions_schema' was defined, but 'partitions' was not. The parameter 'partitions_schema' is only to be used when defining 'partitions'")
                return
            elif type(user_partitions_schema) != type([]) and all(len(part_schema) == 2 for part_schema in user_partitions_schema):
                print("ERROR: 'partitions_schema' should be a list of tuples of the column name and column type of the form partitions_schema=[('col_nameA','int32','col_nameB','str')]")
                return
            elif len(user_partitions_schema) != len(user_partitions):
                print("ERROR: The number of columns in 'partitions' should be the same as 'partitions_schema'")
                return
                    
        if(isinstance(input, hive.Cursor)):
            hive_table_name = kwargs.get('hive_table_name', table_name)
            hive_database_name = kwargs.get('hive_database_name', 'default')
            folder_list, hive_file_format_hint, extra_kwargs, extra_columns, hive_schema = get_hive_table(
                input, hive_table_name, hive_database_name, user_partitions)
            
            if file_format_hint == 'undefined':
                file_format_hint = hive_file_format_hint
            elif file_format_hint != hive_file_format_hint:
                print("WARNING: file_format specified (" + str(file_format_hint) + ") does not match the file_format infered by the Hive cursor (" + str(hive_file_format_hint) + "). Using user specified file_format")

            kwargs.update(extra_kwargs)
            input = folder_list
            is_hive_input = True
        elif user_partitions is not None:
            if user_partitions_schema is None:
                print("ERROR: When using 'partitions' without a Hive cursor, you also need to set 'partitions_schema' which should be a list of tuples of the column name and column type of the form partitions_schema=[('col_nameA','int32','col_nameB','str')]")
                return
            
            hive_schema = {}
            if isinstance(input, str):
                hive_schema['location'] = input
            elif isinstance(input, list) and len(input) == 1:
                hive_schema['location'] = input[0]
            else:
                print("ERROR: When using 'partitions' without a Hive cursor, the input needs to be a path to the base folder of the partitioned data")
                return

            hive_schema['partitions'] = getPartitionsFromUserPartitions(user_partitions)
            input = getFolderListFromPartitions(hive_schema['partitions'], hive_schema['location'] )

        if user_partitions_schema is not None:
            extra_columns = []
            for part_schema in user_partitions_schema:
                extra_columns.append((part_schema[0], convertTypeNameStrToCudfType(part_schema[1])))
        
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

            input = resolve_relative_path(input)

            ignore_missing_paths = user_partitions_schema is not None # if we are using user defined partitions without hive, we want to ignore paths we dont find. 
            parsedSchema = self._parseSchema(
                input, file_format_hint, kwargs, extra_columns, ignore_missing_paths)

            if is_hive_input or user_partitions is not None:
                uri_values = get_uri_values(parsedSchema['files'], hive_schema['partitions'], hive_schema['location'])
                num_cols = len(parsedSchema['names'])
                num_partition_cols = len(extra_columns)
                in_file = [True]*(num_cols - num_partition_cols) + [False]*num_partition_cols
            else:
                uri_values = []

            file_type = parsedSchema['file_type']
            table = BlazingTable(
                parsedSchema['files'],
                file_type,
                files=parsedSchema['files'],
                datasource=parsedSchema['datasource'],
                calcite_to_file_indices=parsedSchema['calcite_to_file_indices'],
                args=parsedSchema['args'],
                uri_values=uri_values,
                in_file=in_file)

            if is_hive_input:
                table.column_names = hive_schema['column_names'] # table.column_names are the official schema column_names
                table.file_column_names = parsedSchema['names'] # table.file_column_names are the column_names used by the file (may be different)
                merged_types = []
                if len(hive_schema['column_types']) == len(parsedSchema['types']):
                    for i in range(len(parsedSchema['types'])):
                        if parsedSchema['types'][i] == 0:  # if the type parsed from the file is 0 we want to use the one from Hive
                            merged_types.append(hive_schema['column_types'][i])
                        else:
                            merged_types.append(parsedSchema['types'][i])
                else:
                    print("ERROR: number of hive_schema columns does not match number of parsedSchema columns")

                table.column_types = merged_types
            else:
                table.column_names = parsedSchema['names'] # table.column_names are the official schema column_names
                table.file_column_names = parsedSchema['names'] # table.file_column_names are the column_names used by the file (may be different
                table.column_types = parsedSchema['types']

            table.slices = table.getSlices(len(self.nodes))

            if len(uri_values) > 0:
                parsedMetadata = parseHiveMetadata(table, uri_values) 
                table.metadata = parsedMetadata                

            if parsedSchema['file_type'] == DataType.PARQUET :
                parsedMetadata = self._parseMetadata(file_format_hint, table.slices, parsedSchema, kwargs)
                
                if isinstance(parsedMetadata, dask_cudf.core.DataFrame):
                    parsedMetadata = parsedMetadata.compute()
                    parsedMetadata = parsedMetadata.reset_index()

                if len(uri_values) > 0:
                    table.metadata = mergeMetadata(table, parsedMetadata, table.metadata)
                else:
                    table.metadata = parsedMetadata

                # lets make sure that the number of files from the metadata actually matches the number of files.
                # this is to handle the cases where there is a file that does not actually have data
                # files that do not have data wont show up in the metadata and we will want to remove them from the table schema
                file_groups = table.metadata.groupby('file_handle_index')._grouped()
                if len(file_groups) != len(table.files):
                    table.metadata, table.files = adjust_due_to_missing_rowgroups(table.metadata, table.files)

                # now lets get the row_groups_ids from the metadata
                metadata_ids = table.metadata[['file_handle_index', 'row_group_index']].to_pandas()
                grouped = metadata_ids.groupby('file_handle_index')
                row_groups_ids = []
                for group_id in grouped.groups:
                    row_indices = grouped.groups[group_id].values.tolist()
                    row_groups_col = metadata_ids['row_group_index'].values.tolist()
                    row_group_ids = [row_groups_col[i] for i in row_indices]
                    row_groups_ids.append(row_group_ids)
                table.row_groups_ids = row_groups_ids


        elif isinstance(input, dask_cudf.core.DataFrame):
            table = BlazingTable(
                input,
                DataType.DASK_CUDF,
                client=self.dask_client)

        if table is not None:
            self.add_remove_table(table_name, True, table)


    def drop_table(self, table_name):
        """
        Drop table from BlazingContext memory.

        Parameters
        ----------

        table_name : string of table name to drop.

        Examples
        --------

        Drop 'taxi' table:

        >>> bc.drop_table('taxi')


        Docs: https://docs.blazingdb.com/docs/using-blazingsql#section-drop-tables
        """
        self.add_remove_table(table_name, False)

    def _parseSchema(self, input, file_format_hint, kwargs, extra_columns, ignore_missing_paths):
        if self.dask_client:
            worker = tuple(self.dask_client.scheduler_info()['workers'])[0]
            connection = self.dask_client.submit(
                cio.parseSchemaCaller,
                input,
                file_format_hint,
                kwargs,
                extra_columns,
                ignore_missing_paths,
                workers=[worker])
            return connection.result()
        else:
            return cio.parseSchemaCaller(
                input, file_format_hint, kwargs, extra_columns, ignore_missing_paths)

    def _parseMetadata(self, file_format_hint, currentTableNodes, schema, kwargs):
        if self.dask_client:
            dask_futures = []
            workers = tuple(self.dask_client.scheduler_info()['workers'])
            for worker_id, worker in enumerate(workers):
                file_subset = [ file.decode() for file in currentTableNodes[worker_id].files]
                if len(file_subset) > 0:
                    connection = self.dask_client.submit(
                        cio.parseMetadataCaller,
                        file_subset,
                        currentTableNodes[worker_id].offset,
                        schema,
                        file_format_hint,
                        kwargs,
                        workers=[worker])
                    dask_futures.append(connection)
            return dask.dataframe.from_delayed(dask_futures)

        else:
            files = [ file.decode() for file in currentTableNodes[0].files]
            return cio.parseMetadataCaller(
                files, currentTableNodes[0].offset, schema, file_format_hint, kwargs)

    def _sliceRowGroups(self, numSlices, files, uri_values, row_groups_ids):
        nodeFilesList = []

        total_num_rowgroups = sum([len(x) for x in row_groups_ids])
        file_index_per_rowgroups = [file_index  for file_index, row_groups_for_file in enumerate(row_groups_ids) for row_group in row_groups_for_file]
        flattened_rowgroup_ids = [row_group  for row_groups_for_file in row_groups_ids for row_group in row_groups_for_file]

        all_sliced_files = []
        all_sliced_uri_values = []
        all_sliced_row_groups_ids = []
        remaining = total_num_rowgroups
        startIndex = 0
        for i in range(0, numSlices):
            batchSize = int(remaining / (numSlices - i))
            file_indexes_for_slice = file_index_per_rowgroups[startIndex: startIndex + batchSize]
            unique_file_indexes_for_slice = list(set(file_indexes_for_slice)) # lets get the unique indexes
            sliced_files = [files[i] for i in unique_file_indexes_for_slice]
            if uri_values is not None and len(uri_values) > 0:
                sliced_uri_values = [uri_values[i] for i in unique_file_indexes_for_slice]
            else:
                sliced_uri_values = []

            sliced_rowgroup_ids = []
            last_file_index = None
            for ind, file_index in enumerate(file_indexes_for_slice):
                if last_file_index is None or file_index != last_file_index:
                    sliced_rowgroup_ids.append([])
                sliced_rowgroup_ids[-1].append(flattened_rowgroup_ids[ind + startIndex])
                last_file_index = file_index

            startIndex = startIndex + batchSize
            remaining = remaining - batchSize

            all_sliced_files.append(sliced_files)
            all_sliced_uri_values.append(sliced_uri_values)
            all_sliced_row_groups_ids.append(sliced_rowgroup_ids)

        return all_sliced_files, all_sliced_uri_values, all_sliced_row_groups_ids


    def _optimize_with_skip_data_getSlices(self, current_table, scan_table_query):
        nodeFilesList = []
        file_indices_and_rowgroup_indices = cio.runSkipDataCaller(current_table, scan_table_query)
        skipdata_analysis_fail = file_indices_and_rowgroup_indices['skipdata_analysis_fail']
        file_indices_and_rowgroup_indices = file_indices_and_rowgroup_indices['metadata']

        if not skipdata_analysis_fail:
            actual_files = []
            uri_values = []
            row_groups_ids = []

            if not file_indices_and_rowgroup_indices.empty: #skipdata did not filter everything
                file_and_rowgroup_indices = file_indices_and_rowgroup_indices.to_pandas()
                files = file_and_rowgroup_indices['file_handle_index'].values.tolist()
                grouped = file_and_rowgroup_indices.groupby('file_handle_index')
                for group_id in grouped.groups:
                    row_indices = grouped.groups[group_id].values.tolist()
                    actual_files.append(current_table.files[group_id])
                    if group_id < len(current_table.uri_values):
                        uri_values.append(current_table.uri_values[group_id])
                    row_groups_col = file_and_rowgroup_indices['row_group_index'].values.tolist()
                    row_group_ids = [row_groups_col[i] for i in row_indices]
                    row_groups_ids.append(row_group_ids)

            if self.dask_client is None:
                bt = BlazingTable(current_table.input,
                                current_table.fileType,
                                files=actual_files,
                                calcite_to_file_indices=current_table.calcite_to_file_indices,
                                uri_values=uri_values,
                                args=current_table.args,
                                row_groups_ids=row_groups_ids,
                                in_file=current_table.in_file)
                bt.column_names = current_table.column_names
                bt.file_column_names = current_table.file_column_names
                bt.column_types = current_table.column_types
                nodeFilesList.append(bt)

            else:
                all_sliced_files, all_sliced_uri_values, all_sliced_row_groups_ids = self._sliceRowGroups(len(self.nodes), actual_files, uri_values, row_groups_ids)

                for i, node in enumerate(self.nodes):
                    bt = BlazingTable(current_table.input,
                                current_table.fileType,
                                files=all_sliced_files[i],
                                calcite_to_file_indices=current_table.calcite_to_file_indices,
                                uri_values=all_sliced_uri_values[i],
                                args=current_table.args,
                                row_groups_ids=all_sliced_row_groups_ids[i],
                                in_file=current_table.in_file)
                    bt.column_names = current_table.column_names
                    bt.file_column_names = current_table.file_column_names
                    bt.column_types = current_table.column_types
                    nodeFilesList.append(bt)
            return nodeFilesList
        else:
            return current_table.getSlices(len(self.nodes))

    def partition(self, input, by=[]):
        masterIndex = 0
        ctxToken = random.randint(0, 64000)

        if self.dask_client is None:
            print("Not supported...")
        else:
            if(not isinstance(input, dask_cudf.core.DataFrame)):
                print("Not supported...")
            else:
                dask_mapping = getNodePartitions(input, self.dask_client)
                dask_futures = []
                for i, node in enumerate(self.nodes):
                    worker = node['worker']
                    dask_futures.append(
                        self.dask_client.submit(
                            collectPartitionsPerformPartition,
                            masterIndex,
                            self.nodes,
                            ctxToken,
                            input,
                            dask_mapping,
                            by,
                            i,  # this is a dummy variable to make every submit unique which is necessary
                            workers=[worker]))
                result = dask.dataframe.from_delayed(dask_futures)
            return result

    def unify_partitions(self, input):
        """
        Concatenate all partitions that belong to a Dask Worker as one, so that
        you only have one partition per worker. This improves performance when
        running multiple queries on a table created from a dask_cudf DataFrame.

        Parameters
        ----------

        input : a dask_cudf DataFrame.

        Examples
        --------

        Distribute BlazingSQL then create and query a table from a dask_cudf DataFrame:

        >>> from blazingsql import BlazingContext
        >>> from dask.distributed import Client
        >>> from dask_cuda import LocalCUDACluster

        >>> cluster = LocalCUDACluster()
        >>> client = Client(cluster)
        >>> bc = BlazingContext(dask_client=client)

        >>> dask_df = dask_cudf.read_parquet('/Data/my_file.parquet')
        >>> dask_df = bc.unify_partitions(dask_df)
        >>> bc.create_table("unified_partitions_table", dask_df)
        >>> result = bc.sql("SELECT * FROM unified_partitions_table")


        Docs: https://docs.blazingdb.com/docs/unify_partitions
        """
        if isinstance(input, dask_cudf.core.DataFrame) and self.dask_client is not None:
            dask_mapping = getNodePartitions(input, self.dask_client)
            max_num_partitions_per_node = max([len(x) for x in dask_mapping.values()])
            if max_num_partitions_per_node > 1:
                dask_futures = []
                for i, node in enumerate(self.nodes):
                    worker = node['worker']
                    dask_futures.append(
                        self.dask_client.submit(
                            workerUnifyPartitions,
                            input,
                            dask_mapping,
                            i,  # this is a dummy variable to make every submit unique which is necessary
                            workers=[worker]))
                result = dask.dataframe.from_delayed(dask_futures)
                return result
            else:
                return input
        else:
            return input


    def sql(self, sql, table_list=[], algebra=None, use_execution_graph=True):
        """
        Query a BlazingSQL table.

        Returns results as cudf.DataFrame on single-GPU or dask_cudf.DataFrame when distributed (multi-GPU).

        Parameters
        ----------

        sql : string of SQL query.
        algebra (optional) : string of SQL algebra plan. if you used, sql string is not used.

        Examples
        --------

        Register a public S3 bucket, then create and query a table from it:

        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        >>> bc.s3('blazingsql-colab', bucket_name='blazingsql-colab')
        >>> bc.create_table('taxi', 's3://blazingsql-colab/yellow_taxi/1_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f186006a310>

        >>> result = bc.sql('SELECT vendor_id, tpep_pickup_datetime, passenger_count, Total_amount FROM taxi')
        >>> print(result)
                  vendor_id tpep_pickup_datetime  passenger_count  Total_amount
        0                 1  2017-01-09 11:13:28                1     15.300000
        1                 1  2017-01-09 11:32:27                1      7.250000
        2                 1  2017-01-09 11:38:20                1      7.300000
        3                 1  2017-01-09 11:52:13                1      8.500000
        4                 2  2017-01-01 00:00:00                1     52.799999
        ...             ...                  ...              ...           ...

        >>> query = '''
        >>>         SELECT
        >>>             tpep_pickup_datetime, trip_distance, Tip_amount,
        >>>             MTA_tax + Improvement_surcharge + Tolls_amount AS extra
        >>>         FROM taxi
        >>>         WHERE passenger_count = 1 AND Fare_amount > 100
        >>>         '''
        >>> df = bc.sql(query)
        >>> print(df)
             tpep_pickup_datetime  trip_distance  Tip_amount      extra
        0     2017-01-01 06:56:01       0.000000    0.000000   1.000000
        1     2017-01-01 07:11:52       0.000000    0.000000  24.619999
        2     2017-01-01 07:27:10      37.740002   37.580002  31.179998
        3     2017-01-01 07:35:13      42.730000    5.540000  26.869999
        4     2017-01-01 07:42:09      17.540001    0.000000  24.900000
        ...                   ...            ...         ...        ...


        Docs: https://docs.blazingdb.com/docs/single-gpu
        """
        # TODO: remove hardcoding
        masterIndex = 0
        nodeTableList = [{} for _ in range(len(self.nodes))]
        fileTypes = []

        if (algebra is None):
            algebra = self.explain(sql)

        if algebra == '':
            print("Parsing Error")
            return

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
                if new_tables[table].has_metadata():
                    scan_table_query = relational_algebra_steps[table]['table_scans'][0]
                    currentTableNodes = self._optimize_with_skip_data_getSlices(new_tables[table], scan_table_query)
                else:
                    currentTableNodes = new_tables[table].getSlices(len(self.nodes))
            elif(new_tables[table].fileType == DataType.DASK_CUDF):
                if new_tables[table].input.npartitions < len(self.nodes): # dask DataFrames are expected to have one partition per node. If we have less, we have to repartition
                    print("WARNING: Dask DataFrame table has less partitions than there are nodes. Repartitioning ... ")
                    temp_df = new_tables[table].input.compute()
                    new_tables[table].input = dask_cudf.from_cudf(temp_df,npartitions=len(self.nodes))
                    new_tables[table].input = new_tables[table].input.persist()
                    new_tables[table].dask_mapping = getNodePartitions(new_tables[table].input, self.dask_client)
                currentTableNodes = []
                for node in self.nodes:
                    currentTableNodes.append(new_tables[table])
            elif(new_tables[table].fileType == DataType.CUDF or new_tables[table].fileType == DataType.ARROW):
                currentTableNodes = []
                for node in self.nodes:
                    if not isinstance(new_tables[table].input, list):
                        new_tables[table].input = [new_tables[table].input]
                    currentTableNodes.append(new_tables[table])

            for j, nodeList in enumerate(nodeTableList):
                nodeList[table] = currentTableNodes[j]

        ctxToken = random.randint(0, 64000)
        accessToken = 0
        if (len(table_list) > 0):
            print("NOTE: You no longer need to send a table list to the .sql() funtion")

        if use_execution_graph:
            algebra = get_plan(algebra)

        if self.dask_client is None:
            result = cio.runQueryCaller(
                        masterIndex,
                        self.nodes,
                        nodeTableList[0],
                        fileTypes,
                        ctxToken,
                        algebra,
                        accessToken,
                        use_execution_graph)
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
                        use_execution_graph,
                        workers=[worker]))
                i = i + 1
            result = dask.dataframe.from_delayed(dask_futures)
        return result

    # END SQL interface

    # BEGIN LOG interface
    def log(self, query, logs_table_name='bsql_logs'):
        """
        Query BlazingSQL's internal log (bsql_logs) that records events from all queries run.

        Parameters
        ----------

        query : string value SQL query on bsql_logs table.
        logs_table_name (optional) : string of logs table name, 'bsql_logs' by default.

        Examples
        --------

        Initialize BlazingContext and query bsql_logs for how long each query took:

        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        BlazingContext ready
        >>> log_result = bc.log("SELECT log_time, query_id, duration FROM bsql_logs WHERE info = 'Query Execution Done' ORDER BY log_time DESC")
        >>> print(log_result)
                      log_time  query_id      duration
        0  2020-03-30 23:32:25     28799   1961.016235
        1  2020-03-30 23:31:41     56005   1942.558960
        2  2020-03-30 23:27:26       243   3820.107666
        3  2020-03-30 23:27:16     12974   4591.859375
        4  2020-03-30 23:10:44     45323   5897.124023
        ...                ...       ...           ...


        Docs: https://docs.blazingdb.com/docs/blazingsql-logs
        """
        if not self.logs_initialized:
            self.logs_table_name = logs_table_name
            log_files = [self.node_cwds[i] + '/RAL.' + \
                str(i) + '.log' for i in range(0, len(self.node_cwds))]
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
            self.logs_initialized = True

        return self.sql(query, use_execution_graph=False)
