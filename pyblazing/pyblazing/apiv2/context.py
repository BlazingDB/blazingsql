# NOTE WARNING NEVER CHANGE THIS FIRST LINE!!!! NEVER EVER
import cudf


from cudf.core.column.column import build_column

from dask.distributed import get_worker


from collections import OrderedDict

from urllib.parse import urlparse

from threading import Lock
from weakref import ref
from pyblazing.apiv2.filesystem import FileSystem
from pyblazing.apiv2 import DataType
import asyncio

from distributed.comm import listen
from pyblazing.apiv2.comms import PollingPlugin, listen

import json
import collections
from pyhive import hive
from .hive import (
    convertTypeNameStrToCudfType,
    getFolderListFromPartitions,
    getPartitionsFromUserPartitions,
    get_hive_table,
)
import time
import socket
import errno
import os
import pandas
import numpy as np
import pyarrow
from pathlib import PurePath
import cio
import dask_cudf
import dask
import jpype
import dask.distributed
import netifaces as ni

import random

import logging

from enum import IntEnum

jpype.addClassPath(
    os.path.join(os.getenv("CONDA_PREFIX"), "lib/blazingsql-algebra.jar")
)
jpype.addClassPath(
    os.path.join(os.getenv("CONDA_PREFIX"), "lib/blazingsql-algebra-core.jar")
)

# NOTE felipe try first with CONDA_PREFIX/jre/lib/amd64/server/libjvm.so
# (for older Java versions e.g. 8.x)
jvm_path = os.environ["CONDA_PREFIX"] + "/jre/lib/amd64/server/libjvm.so"

if not os.path.isfile(jvm_path):
    # NOTE felipe try a second time using CONDA_PREFIX/lib/server/
    # (for newer java versions e.g. 11.x)
    jvm_path = os.environ["CONDA_PREFIX"] + "/lib/server/libjvm.so"

#jpype.startJVM("-ea", convertStrings=False, jvmpath=jvm_path)
jpype.startJVM()

ArrayClass = jpype.JClass("java.util.ArrayList")
ColumnTypeClass = jpype.JClass(
    "com.blazingdb.calcite.catalog.domain.CatalogColumnDataType"
)
dataType = ColumnTypeClass.fromString("GDF_INT8")
ColumnClass = jpype.JClass("com.blazingdb.calcite.catalog.domain.CatalogColumnImpl")
TableClass = jpype.JClass("com.blazingdb.calcite.catalog.domain.CatalogTableImpl")
DatabaseClass = jpype.JClass("com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl")
BlazingSchemaClass = jpype.JClass("com.blazingdb.calcite.schema.BlazingSchema")
RelationalAlgebraGeneratorClass = jpype.JClass(
    "com.blazingdb.calcite.application.RelationalAlgebraGenerator"
)




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
    s.close()
    return socket_free


class blazing_allocation_mode(IntEnum):
    CudaDefaultAllocation = (0,)
    PoolAllocation = (1,)
    CudaManagedMemory = (2,)


def initializeBlazing(
    ralId=0,
    worker_id='',
    networkInterface="lo",
    singleNode=False,
    allocator="default",
    pool=True,
    initial_pool_size=1000000000,
    config_options={},
    logging_dir_path="blazing_log",
):
    last_str = '|%(levelname)s|||"%(message)s"||||||'
    FORMAT = "%(asctime)s|" + str(ralId) + last_str
    filename = os.path.join(logging_dir_path, "pyblazing." + str(ralId) + ".log")
    logging.basicConfig(filename=filename, format=FORMAT, level=logging.INFO)
    workerIp = ni.ifaddresses(networkInterface)[ni.AF_INET][0]["addr"]
    ralCommunicationPort = random.randint(10000, 32000) + ralId

    logging.info("Worker IP: %s   Port: %d", workerIp, ralCommunicationPort)

    while checkSocket(ralCommunicationPort) is False:
        ralCommunicationPort = random.randint(10000, 32000) + ralId

    if not pool:
        initial_pool_size = 0
    elif pool and initial_pool_size is None:
        initial_pool_size = 0
    elif pool and initial_pool_size == 0:
        initial_pool_size = 1

    possible_allocators = [
        "default",
        "managed",
        "existing",
        "cuda_memory_resource",
        "managed_memory_resource",
        "cnmem_memory_resource",
        "cnmem_managed_memory_resource",
    ]
    if allocator not in possible_allocators:
        print(
            'ERROR: parameter "allocator" was not set to a proper value. '
            + "It was set to: "
            + str(allocator)
            + '. It needs to be either "managed", "default" or "existing"'
        )
        allocator = "managed"

    if not pool and allocator == "default":
        allocator = "cuda_memory_resource"
    elif not pool and allocator == "managed":
        allocator = "managed_memory_resource"
    elif pool and allocator == "default":
        allocator = "cnmem_memory_resource"
    elif pool and allocator == "managed":
        allocator = "cnmem_managed_memory_resource"

    cio.blazingSetAllocatorCaller(allocator.encode(), initial_pool_size, config_options)

    output_cache, input_cache = cio.initializeCaller(
        ralId,
        worker_id.encode(),
        0,
        networkInterface.encode(),
        workerIp.encode(),
        ralCommunicationPort,
        singleNode,
        config_options,
    )

    if singleNode is False:
        worker = dask.distributed.get_worker()
        worker.output_cache = output_cache
        worker.input_cache = input_cache

    if (os.path.isabs(logging_dir_path)):
        log_path = logging_dir_path
    else:
        log_path = os.path.join(os.getcwd(), logging_dir_path)

    return ralCommunicationPort, workerIp, log_path


def getNodePartitionKeys(df, client):
    workers = client.scheduler_info()["workers"]

    worker_partitions = {}
    for worker in workers:
        worker_partitions[worker] = []

    dask.distributed.wait(df)
    worker_part = client.who_has(df)

    for key in worker_part:
        if len(worker_part[key]) > 0:
            worker = worker_part[key][0]
            worker_partitions[worker].append(key)

    return worker_partitions


def get_element(query_partid):
    worker = dask.distributed.get_worker()
    df = worker.query_parts[query_partid]
    del worker.query_parts[query_partid]
    return df

def generateGraphs(
    masterIndex,
    nodes,
    tables,
    table_scans,
    fileTypes,
    ctxToken,
    algebra,
    accessToken,
    config_options,
    single_gpu=False,
):

    import dask.distributed

    worker = dask.distributed.get_worker()
    for table_index in range(len(tables)):
        if isinstance(tables[table_index].input, dask_cudf.core.DataFrame):
            if single_gpu:
                tables[table_index].input = [tables[table_index].input.compute()]
            else:
                print(
                    "ERROR: collectPartitionsRunQuery should not be called "
                    + "with an input of dask_cudf.core.DataFrame"
                )
                logging.error(
                    "collectPartitionsRunQuery should not be called "
                    + "with an input of dask_cudf.core.DataFrame"
                )

        if not single_gpu and hasattr(
            tables[table_index], "partition_keys"
        ):  # this is a dask cudf table
            if len(tables[table_index].partition_keys) > 0:
                tables[table_index].input = []
                for key in tables[table_index].partition_keys:
                    tables[table_index].input.append(worker.data[key])

    try:
        graph = cio.runGenerateGraphCaller(
                        masterIndex,
                        nodes,
                        tables,
                        table_scans,
                        fileTypes,
                        ctxToken,
                        algebra,
                        accessToken,
                        config_options)
        graph.set_input_and_output_caches(worker.input_cache, worker.output_cache)
    except Exception as e:
        raise e

    with worker._lock:
        if not hasattr(worker, "query_graphs"):
            worker.query_graphs = {}

    worker.query_graphs[ctxToken] = graph

def executeGraph(ctxToken):
    import dask.distributed
    worker = dask.distributed.get_worker()

    graph = worker.query_graphs[ctxToken]
    with worker._lock:
        dfs = cio.runExecuteGraphCaller(graph, is_single_node=False)
        meta = dask.dataframe.utils.make_meta(dfs[0])
        query_partids = []

        if not hasattr(worker, "query_parts"):
            worker.query_parts = {}

    for df in dfs:
        query_partid = random.randint(
            0, np.iinfo(np.int32).max
        )  # query_partid should be a unique identifier
        worker.query_parts[query_partid] = df
        query_partids.append(query_partid)
    del worker.query_graphs[ctxToken]
    return query_partids, meta, worker.name


def collectPartitionsPerformPartition(
    masterIndex, nodes, ctxToken, input, partition_keys_mapping, df_schema, by, i
):
    import dask.distributed

    worker = dask.distributed.get_worker()
    worker_id = nodes[i]["worker"]

    if worker_id in partition_keys_mapping:
        partition_keys = partition_keys_mapping[worker_id]
        if len(partition_keys) > 1:
            node_inputs = []
            for key in partition_keys:
                node_inputs.append(worker.data[key])
            # TODO, eventually we want the engine side of the
            # partition function to handle the table in parts
            node_input = cudf.concat(node_inputs)
        elif len(partition_keys) == 1:
            node_input = worker.data[partition_keys[0]]
        else:
            node_input = df_schema
    else:
        node_input = df_schema

    return cio.performPartitionCaller(masterIndex, nodes, ctxToken, node_input, by)


# returns a map of table names to the indices of the columns needed.
# If there are more than one table scan for one table, it merged the
# needed columns if the column list is empty, it means we want all columns
def mergeTableScans(tableScanInfo):
    table_names = list(set(tableScanInfo["table_names"]))
    table_columns = {}
    for table_name in table_names:
        table_columns[table_name] = []

    for index, table_name in enumerate(tableScanInfo["table_names"]):
        # if the column list is empty, it means we want all columns
        if len(tableScanInfo["table_columns"][index]) > 0:
            table_columns[table_name] = list(
                set(table_columns[table_name] + tableScanInfo["table_columns"][index])
            )
            table_columns[table_name].sort()
        else:
            table_columns[table_name] = []

    return table_columns


def modifyAlgebraForDataframesWithOnlyWantedColumns(
    algebra, tableScanInfo, originalTables
):
    for table_name in tableScanInfo:
        # TODO: handle situation with multiple tables being joined twice
        if originalTables[table_name].fileType == DataType.ARROW:
            orig_scan = tableScanInfo[table_name]["table_scans"][0]
            orig_col_indexes = tableScanInfo[table_name]["table_columns"][0]
            merged_col_indexes = list(range(len(orig_col_indexes)))

            new_col_indexes = []
            if len(merged_col_indexes) > 0:
                if orig_col_indexes == merged_col_indexes:
                    new_col_indexes = list(range(0, len(orig_col_indexes)))
                else:
                    enumerated_indexes = enumerate(merged_col_indexes)
                    for new_index, merged_col_index in enumerated_indexes:
                        if merged_col_index in orig_col_indexes:
                            new_col_indexes.append(new_index)

            orig_project = "projects=[" + str(orig_col_indexes) + "]"
            new_project = "projects=[" + str(new_col_indexes) + "]"
            new_scan = orig_scan.replace(orig_project, new_project)
            algebra = algebra.replace(orig_scan, new_scan)
    return algebra


def get_uri_values(files, partitions, base_folder):
    if base_folder[-1] != "/":
        base_folder = base_folder + "/"

    uri_values = []
    for file in files:
        file_dir = os.path.dirname(file.decode())
        partition_name = file_dir.replace(base_folder, "")
        if partition_name in partitions:
            uri_values.append(partitions[partition_name])
        else:
            print("ERROR: Could not get partition values for file: " + file.decode())
    return uri_values


def parseHiveMetadata(curr_table, uri_values):
    metadata = {}
    names = []
    # not all columns will have hive metadata, so this vector will capture
    # all the names that will actually be used in the end
    final_names = []
    n_cols = len(curr_table.column_names)

    dtypes = [cio.cudf_type_int_to_np_types(t) for t in curr_table.column_types]

    columns = [name.decode() for name in curr_table.column_names]
    for index in range(n_cols):
        col_name = columns[index]
        names.append("min_" + str(index) + "_" + col_name)
        names.append("max_" + str(index) + "_" + col_name)

    names.append("file_handle_index")
    names.append("row_group_index")
    minmax_metadata_table = [[] for _ in range(2 * n_cols + 2)]
    table_partition = {}
    for file_index, uri_value in enumerate(uri_values):
        for index, [col_name, col_value_id] in enumerate(uri_value):
            if col_name in columns:
                col_index = columns.index(col_name)
            else:
                print(
                    "ERROR: could not find partition column name "
                    + str(col_name)
                    + " in table names"
                )
            if dtypes[col_index] == np.dtype("object"):
                np_col_value = col_value_id
            elif (
                dtypes[col_index] == np.dtype("datetime64[s]")
                or dtypes[col_index] == np.dtype("datetime64[ms]")
                or dtypes[col_index] == np.dtype("datetime64[us]")
                or dtypes[col_index] == np.dtype("datetime64[ns]")
            ):
                np_col_value = np.datetime64(col_value_id)
            else:
                np_col_value = np.fromstring(col_value_id, dtypes[col_index], sep=" ")[
                    0
                ]

            table_partition.setdefault(col_name, []).append(np_col_value)
        minmax_metadata_table[len(minmax_metadata_table) - 2].append(file_index)
        # this assumes that you only have one row group per partitioned file
        # but is addressed in the mergeMetadata function, where you will have
        # information about how many rowgroups per file and you can expand
        # the hive metadata accordingly
        minmax_metadata_table[len(minmax_metadata_table) - 1].append(
            0
        )  # this is the rowgroup index
    for index in range(n_cols):
        col_name = columns[index]
        if col_name in table_partition:
            col_value_ids = table_partition[col_name]
            minmax_metadata_table[2 * index] = col_value_ids
            minmax_metadata_table[2 * index + 1] = col_value_ids

    series = []
    for index in range(n_cols):
        col_name = columns[index]
        if col_name in table_partition:
            if (
                dtypes[index] == np.dtype("datetime64[s]")
                or dtypes[index] == np.dtype("datetime64[ms]")
                or dtypes[index] == np.dtype("datetime64[us]")
                or dtypes[index] == np.dtype("datetime64[ns]")
            ):
                # when creating a pandas series, for a datetime type,
                # it has to be in ns because that is the only internal
                # datetime representation
                col1 = pandas.Series(
                    minmax_metadata_table[2 * index],
                    dtype=np.dtype("datetime64[ns]"),
                    name=names[2 * index],
                )
                col2 = pandas.Series(
                    minmax_metadata_table[2 * index + 1],
                    dtype=np.dtype("datetime64[ns]"),
                    name=names[2 * index + 1],
                )
            else:
                col1 = pandas.Series(
                    minmax_metadata_table[2 * index],
                    dtype=dtypes[index],
                    name=names[2 * index],
                )
                col2 = pandas.Series(
                    minmax_metadata_table[2 * index + 1],
                    dtype=dtypes[index],
                    name=names[2 * index + 1],
                )
            series.append(col1)
            series.append(col2)
            final_names.append(names[2 * index])
            final_names.append(names[2 * index + 1])
    index = n_cols
    col1 = pandas.Series(
        minmax_metadata_table[2 * index], dtype=np.int32, name=names[2 * index]
    )
    col2 = pandas.Series(
        minmax_metadata_table[2 * index + 1], dtype=np.int32, name=names[2 * index + 1]
    )
    final_names.append(names[2 * index])
    final_names.append(names[2 * index + 1])
    series.append(col1)
    series.append(col2)

    frame = OrderedDict((key, value) for (key, value) in zip(final_names, series))
    metadata = cudf.DataFrame(frame)
    for index, col_type in enumerate(dtypes):
        min_col_name = names[2 * index]
        max_col_name = names[2 * index + 1]
        if (
            dtypes[index] == np.dtype("datetime64[s]")
            or dtypes[index] == np.dtype("datetime64[ms]")
            or dtypes[index] == np.dtype("datetime64[us]")
            or dtypes[index] == np.dtype("datetime64[ns]")
        ):
            if (min_col_name in metadata) and (max_col_name in metadata):
                if (
                    metadata[min_col_name].dtype != dtypes[index]
                    or metadata[max_col_name].dtype != dtypes[index]
                ):
                    # here we are casting the timestamp types from ns
                    # to their correct desired types
                    metadata[min_col_name] = metadata[min_col_name].astype(
                        dtypes[index]
                    )
                    metadata[max_col_name] = metadata[max_col_name].astype(
                        dtypes[index]
                    )
    return metadata


def mergeMetadata(curr_table, fileMetadata, hiveMetadata):

    if fileMetadata.shape[0] != hiveMetadata.shape[0]:
        print(
            "ERROR: number of rows from fileMetadata: "
            + str(fileMetadata.shape[0])
            + " does not match hiveMetadata: "
            + str(hiveMetadata.shape[0])
        )
        return hiveMetadata

    file_hand_hive = hiveMetadata["file_handle_index"]
    if not fileMetadata["file_handle_index"].equals(file_hand_hive):
        print(
            """ERROR: file_handle_index of fileMetadata does not match
             the same order as in hiveMetadata"""
        )
        return hiveMetadata

    result = fileMetadata
    columns = [c.decode() for c in curr_table.column_names]
    n_cols = len(curr_table.column_names)

    names = []
    # not all columns will have hive metadata, so this vector will capture
    # all the names that will actually be used in the end
    final_names = []
    for index in range(n_cols):
        col_name = columns[index]
        names.append("min_" + str(index) + "_" + col_name)
        names.append("max_" + str(index) + "_" + col_name)
    names.append("file_handle_index")
    names.append("row_group_index")

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

    frame = OrderedDict((key, value) for (key, value) in zip(final_names, series))
    result = cudf.DataFrame(frame)
    return result


def is_double_children(expr):
    return "LogicalJoin" in expr or "LogicalUnion" in expr


def visit(lines):
    stack = collections.deque()
    root_level = 0
    dicc = {"expr": lines[root_level][1], "children": []}
    processed = set()
    for index in range(len(lines)):
        child_level, expr = lines[index]
        if child_level == root_level + 1:
            new_dicc = {"expr": expr, "children": []}
            if len(dicc["children"]) == 0:
                dicc["children"] = [new_dicc]
            else:
                dicc["children"].append(new_dicc)
            stack.append((index, child_level, expr, new_dicc))
            processed.add(index)

    for index in processed:
        lines[index][0] = -1

    while len(stack) > 0:
        curr_index, curr_level, curr_expr, curr_dicc = stack.pop()
        processed = set()

        if curr_index < len(lines) - 1:  # is brother
            child_level, expr = lines[curr_index + 1]
            if child_level == curr_level:
                continue
            elif child_level == curr_level + 1:
                index = curr_index + 1
                if is_double_children(curr_expr):
                    while index < len(lines) and len(curr_dicc["children"]) < 2:
                        child_level, expr = lines[index]
                        if child_level == curr_level + 1:
                            new_dicc = {"expr": expr, "children": []}
                            if len(curr_dicc["children"]) == 0:
                                curr_dicc["children"] = [new_dicc]
                            else:
                                curr_dicc["children"].append(new_dicc)
                            processed.add(index)
                            stack.append((index, child_level, expr, new_dicc))
                        index += 1
                else:
                    while index < len(lines) and len(curr_dicc["children"]) < 1:
                        child_level, expr = lines[index]
                        if child_level == curr_level + 1:
                            new_dicc = {"expr": expr, "children": []}
                            if len(curr_dicc["children"]) == 0:
                                curr_dicc["children"] = [new_dicc]
                            else:
                                curr_dicc["children"].append(new_dicc)
                            processed.add(index)
                            stack.append((index, child_level, expr, new_dicc))
                        index += 1

        for index in processed:
            lines[index][0] = -1
    return json.dumps(dicc)


def get_plan(algebra):
    algebra = algebra.replace("  ", "\t")
    lines = algebra.split("\n")
    # algebra plan was provided and only contains one-line as logical plan
    if len(lines) == 1:
        algebra += "\n"
        lines = algebra.split("\n")
    new_lines = []
    for i in range(len(lines) - 1):
        line = lines[i]
        level = line.count("\t")
        new_lines.append([level, line.replace("\t", "")])
    return visit(new_lines)


def resolve_relative_path(files):
    files_out = []
    for file in files:
        if isinstance(file, str):
            # if its an abolute path or fs path
            if (
                file.startswith("/")
                | file.startswith("hdfs://")
                | file.startswith("s3://")
                | file.startswith("gs://")
            ):
                files_out.append(file)
            else:  # if its not, lets see if its a relative path we can access
                abs_file = os.path.abspath(os.path.join(os.getcwd(), file))
                if os.path.exists(abs_file):
                    files_out.append(abs_file)
                # if its not, lets just leave it and see if somehow
                # the engine can access it
                else:
                    files_out.append(file)
        else:  # we are assuming all are string. If not, lets just return
            return files
    return files_out


# this is to handle the cases where there is a file that does not actually
# have data files that do not have data wont show up in the metadata and
# we will want to remove them from the table schema
def adjust_due_missing_rowgroups(metadata, files):
    metadata_ids = metadata[["file_handle_index", "row_group_index"]].to_pandas()
    grouped = metadata_ids.groupby("file_handle_index")
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
        mask = metadata["file_handle_index"] > ind
        metadata["file_handle_index"][mask] = metadata["file_handle_index"][mask] - 1
    return metadata, new_files


def distributed_initialize_server_directory(client, dir_path):

    # We are going to differentiate the two cases. When path is absolute,
    # we do the logging folder creation only once per host (server).
    # When path is relative, we have to group the workers according
    # to whether they have the same current working directory,
    # so, a unique folder will be created for each sub common cwd set.

    all_items = client.scheduler_info()["workers"].items()

    is_absolute_path = os.path.isabs(dir_path)

    if is_absolute_path:
        # Let's group the workers by host_name
        host_worker_dict = {}
        for worker, worker_info in all_items:
            host_name = worker.split(":")[0]
            if host_name not in host_worker_dict.keys():
                host_worker_dict[host_name] = [worker]
            else:
                host_worker_dict[host_name].append(worker)

        dask_futures = []
        for host_name, worker_list in host_worker_dict.items():
            dask_futures.append(
                client.submit(
                    initialize_server_directory,
                    dir_path,
                    workers=[worker_list[0]],
                    pure=False,
                )
            )

        for connection in dask_futures:
            made_dir = connection.result()
            if not made_dir:
                logging.info("Directory already exists")
    else:
        # Let's get the current working directory of all workers
        dask_futures = []
        for worker, worker_info in all_items:
            dask_futures.append(
                client.submit(get_current_directory_path, workers=[worker], pure=False)
            )

        current_working_dirs = client.gather(dask_futures)

        # Let's group the workers by host_name and by common cwd
        host_worker_dict = {}
        for worker_key, cwd in zip(all_items, current_working_dirs):
            worker = worker_key[0]
            host_name = worker.split(":")[0]
            if host_name not in host_worker_dict.keys():
                host_worker_dict[host_name] = {cwd: [worker]}
            else:
                if cwd not in host_worker_dict[host_name].keys():
                    host_worker_dict[host_name][cwd] = [worker]
                else:
                    host_worker_dict[host_name][cwd].append(worker)

        dask_futures = []
        for host_name, common_current_work in host_worker_dict.items():
            for cwd, worker_list in common_current_work.items():
                dask_futures.append(
                    client.submit(
                        initialize_server_directory,
                        dir_path,
                        workers=[worker_list[0]],
                        pure=False,
                    )
                )

        for connection in dask_futures:
            made_dir = connection.result()
            if not made_dir:
                logging.info("Directory already exists")


def initialize_server_directory(dir_path):
    if not os.path.exists(dir_path):
        try:
            os.mkdir(dir_path)
        except OSError as error:
            logging.error("Could not create directory: " + error)
            raise
        return True
    else:
        return True



def get_current_directory_path():
    return os.getcwd()


# Delete all generated (older than 1 hour) orc files
def remove_orc_files_from_disk(data_dir):
    if os.path.isfile(data_dir):  # only if data_dir exists
        all_files = os.listdir(data_dir)
        current_time = time.time()
        for file in all_files:
            if ".blazing-temp" in file:
                full_path_file = data_dir + "/" + file
                creation_time = os.path.getctime(full_path_file)
                if (current_time - creation_time) // (1 * 60 * 60) >= 1:
                    os.remove(full_path_file)


class BlazingTable(object):
    def __init__(
        self,
        name,
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
        row_groups_ids=[],
    ):
        # row_groups_ids, vector<vector<int>> one vector
        # of row_groups per file
        self.name = name
        self.fileType = fileType
        if fileType == DataType.ARROW:
            if force_conversion:
                # converts to cudf for querying
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
        if self.fileType == DataType.CUDF or self.fileType == DataType.DASK_CUDF:
            if convert_gdf_to_dask and isinstance(self.input, cudf.DataFrame):
                self.input = dask_cudf.from_cudf(
                    self.input, npartitions=convert_gdf_to_dask_partitions
                )
            if isinstance(self.input, dask_cudf.core.DataFrame):
                self.input = self.input.persist()
        self.uri_values = uri_values
        self.in_file = in_file

        # slices, this is computed in create table,
        # and then reused in sql method
        self.slices = None
        # metadata, this is computed in create table, after call get_metadata
        self.metadata = metadata
        # row_groups_ids, vector<vector<int>> one vector of
        # row_groups per file
        self.row_groups_ids = row_groups_ids
        # a pair of values with the startIndex and batchSize
        # info for each slice
        self.offset = (0, 0)

        self.column_names = []
        self.column_types = []

        if self.fileType == DataType.CUDF:
            self.column_names = [x for x in self.input._data.keys()]
            data_values = self.input._data.values()
            self.column_types = [cio.np_to_cudf_types_int(x.dtype) for x in data_values]
        elif self.fileType == DataType.DASK_CUDF:
            self.column_names = [x for x in input.columns]
            self.column_types = [cio.np_to_cudf_types_int(x) for x in input.dtypes]

        # file_column_names are usually the same as column_names, except
        # for when in a hive table the column names defined by the hive schema
        # are different that the names in actual files
        self.file_column_names = self.column_names

    def has_metadata(self):
        if isinstance(self.metadata, dask_cudf.core.DataFrame):
            return not self.metadata.compute().empty
        if self.metadata is not None:
            return not self.metadata.empty
        return False

    def filterAndRemapColumns(self, tableColumns):
        # only used for arrow
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
        new_table = pyarrow.Table.from_arrays(columns, names=names)
        new_table = BlazingTable(
            self.name, new_table, DataType.ARROW, force_conversion=True
        )

        return new_table

    def convertForQuery(self):
        return BlazingTable(
            self.name, self.arrow_table, DataType.ARROW, force_conversion=True
        )

    # until this is implemented we cant do self join with arrow tables
    #    def unionColumns(self,otherTable):

    def getDaskDataFrameKeySlices(self, nodes, client):
        import copy

        nodeFilesList = []
        partition_keys_mapping = getNodePartitionKeys(self.input, client)
        for node in nodes:
            # here we are making a shallow copy of the table and getting rid
            # of the reference for the dask.cudf.DataFrame since we dont want
            # to send that to dask wokers. You dont want to send a distributed
            # object to individual workers
            table = copy.copy(self)
            if node["worker"] in partition_keys_mapping:
                table.partition_keys = partition_keys_mapping[node["worker"]]
                table.input = []
            else:
                table.input = [table.input._meta]
                table.partition_keys = []
            nodeFilesList.append(table)

        return nodeFilesList

    def getSlices(self, numSlices):
        nodeFilesList = []
        if self.files is None:
            for i in range(0, numSlices):
                nodeFilesList.append(BlazingTable(self.name, self.input, self.fileType))
            return nodeFilesList
        remaining = len(self.files)
        startIndex = 0
        for i in range(0, numSlices):
            batchSize = int(remaining / (numSlices - i))
            tempFiles = self.files[startIndex : startIndex + batchSize]
            uri_values = self.uri_values[startIndex : startIndex + batchSize]

            slice_row_groups_ids = []
            if self.row_groups_ids is not None:
                slice_row_groups_ids = self.row_groups_ids[
                    startIndex : startIndex + batchSize
                ]

            bt = BlazingTable(
                self.name,
                self.input,
                self.fileType,
                files=tempFiles,
                calcite_to_file_indices=self.calcite_to_file_indices,
                uri_values=uri_values,
                args=self.args,
                row_groups_ids=slice_row_groups_ids,
                in_file=self.in_file,
            )
            bt.offset = (startIndex, batchSize)
            bt.column_names = self.column_names
            bt.file_column_names = self.file_column_names
            bt.column_types = self.column_types
            nodeFilesList.append(bt)

            startIndex = startIndex + batchSize
            remaining = remaining - batchSize

        return nodeFilesList


class BlazingContext(object):
    """
    BlazingContext is the Python API of BlazingSQL. Along with initialization
    arguments allowing for easy multi-GPU distribution, the BlazingContext
    class has a number of methods which assist not only in creating and
    querying tables, but also in connecting remote data sources
    and understanding your ETL.

    Docs: https://docs.blazingdb.com/docs/blazingcontext
    """

    def __init__(
        self,
        dask_client=None,
        network_interface=None,
        allocator="default",
        pool=False,
        initial_pool_size=None,
        config_options={},
    ):
        """
        Create a BlazingSQL API instance.

        Parameters
        -------------------

        dask_client (optional) : dask.distributed.Client instance.
                    only necessary for distributed query execution.
        network_interface (optional) : for communicating with the
                    dask-scheduler. see note below.
        allocator (optional) :  "managed" or "default" or "existing", where
                    "managed" uses Unified Virtual Memory (UVM) and
                    may use system memory if GPU memory runs out, "default"
                    uses the default Cuda allocation and "existing" assumes
                    rmm allocator is already set and does not initialize it.
                    "managed" is the BlazingSQL default, since it provides
                    the most robustness against OOM errors.
        pool (optional) : if True, BlazingContext will self-allocate a GPU
                    memory pool. can greatly improve performance.
        initial_pool_size (optional) : initial size of memory pool in bytes
                    (if pool=True).
                    if None, and pool=True, defaults to 1/2 GPU memory.
        config_options (optional) : this is a dictionary for setting certain
                    parameters in the engine. These parameters will be used
                    for all queries except if overriden by setting these
                    parameters when running the query itself.
                    The possible parameters are:

            JOIN_PARTITION_SIZE_THRESHOLD : Num bytes to try to have the
                    partitions for each side of a join before doing the join.
                    Too small can lead to overpartitioning, too big can lead
                    to OOM errors.
                    default: 400000000
            MAX_JOIN_SCATTER_MEM_OVERHEAD : The bigger this value, the more
                    likely one of the tables of join will be scattered to all
                    the nodes, instead of doing a standard hash based
                    partitioning shuffle. Value is in bytes.
                    default: 500000000
            MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE : The maximum number of
                    partitions that will be made for an order by.
                    Increse this number if running into OOM issues when
                    doing order bys with large amounts of data.
                    default: 8
            NUM_BYTES_PER_ORDER_BY_PARTITION : The max number size in bytes
                    for each order by partition. Note that,
                    MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE will be enforced over
                    this parameter.
                    default: 400000000
            TABLE_SCAN_KERNEL_NUM_THREADS: The number of threads used in the
                    TableScan & BindableTableScan kernels for reading batches
                    default: 4
            MAX_DATA_LOAD_CONCAT_CACHE_BYTE_SIZE : The max size in bytes to
                    concatenate the batches read from the scan kernels
                    default: 400000000
            FLOW_CONTROL_BYTES_THRESHOLD: If an output cache surpasses this
                    value in bytes, the kernel will try to stop
                    execution until the output cache contains less.
                    default: max size_t (makes it not applicable)
            ORDER_BY_SAMPLES_RATIO : The ratio to multiply the estimated total
                    number of rows in the SortAndSampleKernel to calculate
                    the number of samples
                    default: 0.1
            BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD : The percent
                    (as a decimal) of total GPU memory that the memory
                    resource will consider to be full
                    NOTE: This parameter only works when used in the
                    BlazingContext
                    default: 0.95
            BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD : The percent
                    (as a decimal) of total host memory that the memory
                    resource will consider to be full. In the presence of
                    several GPUs per server, this resource will be shared
                    among all of them in equal parts.
                    NOTE: This parameter only works when used in the
                    BlazingContext
                    default: 0.75
            BLAZING_LOGGING_DIRECTORY : A folder path to place all logging
                    files. The path can be relative or absolute.
                    NOTE: This parameter only works when used in the
                    BlazingContext
                    default: 'blazing_log'
            BLAZING_CACHE_DIRECTORY : A folder path to place all orc files
                    when start caching on Disk. The path can be relative
                    or absolute.
                    NOTE: This parameter only works when used in the
                    BlazingContext
                    default: '/tmp/'
            MEMORY_MONITOR_PERIOD : How often the memory monitor checks memory
                    consumption. The value is in milliseconds.
                    default: 50  (milliseconds)

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


        Note: When using BlazingSQL with multiple nodes, you will need to set
        the correct network_interface your servers are using to communicate
        with the IP address of the dask-scheduler. You can see the different
        network interfaces and what IP addresses they serve with the bash
        command ifconfig. The default is set to 'eth0'.
        """

        self.single_gpu_idx = 0
        self.lock = Lock()
        self.finalizeCaller = ref(cio.finalizeCaller)
        self.dask_client = dask_client
        self.nodes = []
        self.node_log_paths = []
        self.finalizeCaller = lambda: NotImplemented
        self.config_options = {}
        for option in config_options:
            self.config_options[option.encode()] = str(
                config_options[option]
            ).encode()  # make sure all options are encoded strings

        logging_dir_path = "blazing_log"
        # want to use config_options and not self.config_options
        # since its not encoded
        if "BLAZING_LOGGING_DIRECTORY" in config_options:
            logging_dir_path = config_options["BLAZING_LOGGING_DIRECTORY"]

        cache_dir_path = "/tmp"  # default directory to store orc files
        if "BLAZING_CACHE_DIRECTORY" in config_options:
            cache_dir_path = config_options["BLAZING_CACHE_DIRECTORY"] + "tmp"

        self.config_options[
            "BLAZING_CACHE_DIRECTORY".encode()
        ] = cache_dir_path.encode()

        # remove if exists older orc tmp files
        remove_orc_files_from_disk(cache_dir_path)

        host_memory_quota = 0.75
        if not "BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD".encode() in self.config_options:
            self.config_options["BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD".encode()] = str(
                host_memory_quota
            ).encode()

        if dask_client is not None:
            distributed_initialize_server_directory(self.dask_client, logging_dir_path)
            distributed_initialize_server_directory(self.dask_client, cache_dir_path)

            if network_interface is None:
                network_interface = "eth0"

            worker_list = []
            dask_futures = []
            i = 0

            if "BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD" in config_options:
                host_memory_quota = float(
                    self.config_options["BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD".encode()]
                )

            # If all workers are on the same machine, the memory threshold is
            # split between the workers, here we are assuming that there are
            # the same number of GPUs/workers per server.
            workers_info = self.dask_client.scheduler_info()["workers"]
            host_list = [value["host"] for key, value in workers_info.items()]
            self.config_options["BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD".encode()] = str(
                host_memory_quota * len(set(host_list)) / len(workers_info)
            ).encode()

            for worker in list(self.dask_client.scheduler_info()["workers"]):
                dask_futures.append(
                    self.dask_client.submit(
                        initializeBlazing,
                        ralId=i,
                        worker_id=worker,
                        networkInterface=network_interface,
                        singleNode=False,
                        allocator=allocator,
                        pool=pool,
                        initial_pool_size=initial_pool_size,
                        config_options=self.config_options,
                        logging_dir_path=logging_dir_path,
                        workers=[worker],
                    )
                )
                worker_list.append(worker)
                i = i + 1
            i = 0
            for connection in dask_futures:
                ralPort, ralIp, log_path = connection.result()
                node = {}
                node["worker"] = worker_list[i]
                node["ip"] = ralIp
                node["communication_port"] = ralPort
                self.nodes.append(node)
                self.node_log_paths.append(log_path)
                i = i + 1



            # Register and start polling plugin on each Dask worker
            self.polling_plugin = PollingPlugin()
            self.dask_client.register_worker_plugin(self.polling_plugin)

            # Start listener on each worker to send received messages to router
            
            listen(client=self.dask_client)


            # need to initialize this logging independently, in case its set as a relative path
            # and the location from where the python script is running is different
            # than the local dask workers


            initialize_server_directory(logging_dir_path)
            # this one is for the non dask side
            FORMAT = '%(asctime)s||%(levelname)s|||"%(message)s"||||||'
            filename = os.path.join(logging_dir_path, "pyblazing.log")
            logging.basicConfig(filename=filename, format=FORMAT, level=logging.INFO)
        else:
            initialize_server_directory(logging_dir_path)
            initialize_server_directory(cache_dir_path)

            ralPort, ralIp, log_path = initializeBlazing(
                ralId=0,
                worker_id='',
                networkInterface="lo",
                singleNode=True,
                allocator=allocator,
                pool=pool,
                initial_pool_size=initial_pool_size,
                config_options=self.config_options,
                logging_dir_path=logging_dir_path,
            )
            node = {}
            node["ip"] = ralIp
            node["communication_port"] = ralPort
            self.nodes.append(node)
            self.node_log_paths.append(log_path)

        self.fs = FileSystem()

        self.db = DatabaseClass("main")
        self.schema = BlazingSchemaClass(self.db)
        self.generator = RelationalAlgebraGeneratorClass(self.schema)
        self.tables = {}
        self.logs_initialized = False

        # waitForPingSuccess(self.client)
        print("BlazingContext ready")

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

        name : string that represents the name with which you will refer to
            your HDFS cluster.
        host : string IP Address of your HDFS NameNode.
        port : integer of the Port number of your HDFS NameNode.
        user : string of the HDFS User on your NameNode.
        kerb_ticket (optional) : string file path to your ticket for
            kerberos authentication.

        You may also need to set the following environment variables to
        properly interface with HDFS.
        HADOOP_HOME: the root of your installed Hadoop distribution.
        JAVA_HOME: the location of your Java SDK installation
            (should point to CONDA_PREFIX).
        ARROW_LIBHDFS_DIR: explicit location of libhdfs.so if not installed
            at $HADOOP_HOME/lib/native.
        CLASSPATH: must contain the Hadoop jars.

        Examples
        --------

        Register and create table from HDFS:

        >>> bc.hdfs('dir_name', host='name_node_ip', port=port_number,
            user='hdfs_user')
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

        name : string that represents the name with which you will refer to
            your S3 bucket.
        bucket_name : string name of your S3 bucket.
        access_key_id : string of your AWS IAM access key. not required for
            public buckets.
        secret_key : string of your AWS IAM secret key. not required for
            public buckets.
        encryption_type (optional) : None (default), 'AES_256', or 'AWS_KMS'.
        session_token (optional) : string of your AWS IAM session token.
        root (optional) : string path of your bucket that will be used as a
            shortcut path.
        kms_key_amazon_resource (optional) : string value, required for KMS
            encryption only.

        Examples
        --------

        Register and create table from a public S3 bucket:

        >>> bc.s3('blazingsql-colab', bucket_name='blazingsql-colab')
        >>> bc.create_table('taxi',
        >>>    's3://blazingsql-colab/yellow_taxi/1_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f6d4e640c90>


        Register and create table from a private S3 bucket:
        >>> bc.s3('other-data', bucket_name='kws-parquet-data',
        >>>    access_key_id='AKIASPFMPQMQD2OG54IQ',
        >>>    secret_key='bmt+TLTosdkIelsdw9VQjMe0nBnvAA5nPt0kaSx/Y',
        >>>    encryption_type=S3EncryptionType.AWS_KMS,
        >>>       kms_key_amazon_resource_name=
        >>>         'arn:aws:kms:region:acct-id:key/key-id')
        >>> bc.create_table('taxi',
            's3://other-data/yellow_taxi/1_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f12327c0310>


        Docs: https://docs.blazingdb.com/docs/s3
        """
        return self.fs.s3(self.dask_client, prefix, **kwargs)

    def gs(self, prefix, **kwargs):
        """
        Register a Google Storage bucket.

        Parameters
        ----------

        name : string that represents the name with which you will refer to
            your GS bucket.
        project_id : string name of your Google Cloud Platform project.
        bucket_name : string of the name of your GS bucket.
        use_default_adc_json_file (optional) : boolean, whether or not to use
            the default GCP ADC JSON.
        adc_json_file (optional) : string with the location of your custom
            ADC JSON.

        Examples
        --------

        Register and create table from a GS bucket:

        >>> bc.gs('gs_1gb', project_id='blazingsql-enduser',
            bucket_name='bsql')
        >>> bc.create_table('nation',
            'gs://gs_1gb/tpch_sf1/nation/0_0_0.parquet')
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
          BindableTableScan(table=[[main, taxi_2]],
                    filters=[[OR(<($12, 100), <>($3, 4))]])


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
            algebra = ""

        return algebra

    def add_remove_table(self, tableName, addTable, table=None):
        self.lock.acquire()
        try:
            if addTable:
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
                cudf.Dataframe, dask_cudf.DataFrame, pandas.DataFrame,
                filepath for csv, orc, parquet, etc...

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
        >>> bc.create_table('taxi',
        >>>     's3://blazingsql-colab/yellow_taxi/1_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f09264c0310>


        Docs: https://docs.blazingdb.com/docs/create_table
        """

        logging.info("create_table start for " + table_name)

        table = None
        extra_kwargs = {}
        in_file = []
        is_hive_input = False
        extra_columns = []

        # See datasource.file_format
        file_format_hint = kwargs.get("file_format", "undefined")
        # these are user defined partitions should be a dictionary object
        # of the form partitions=
        # {'col_nameA':[val, val], 'col_nameB':['str_val', 'str_val']}
        user_partitions = kwargs.get("partitions", None)
        if user_partitions is not None and not isinstance(user_partitions, dict):
            print(
                """ERROR: User defined partitions should be a dictionary
                 object of the form partitions={'col_nameA':[val, val],
                 'col_nameB':['str_val', 'str_val']}"""
            )
            logging.error(
                """ERROR: User defined partitions should be a dictionary
                 object of the form partitions={'col_nameA':[val, val],
                  'col_nameB':['str_val', 'str_val']}"""
            )
            return

        # for user defined partitions, partitions_schema should be a list of
        # tuples of the column name and column type of the form
        # partitions_schema=[('col_nameA','int32','col_nameB','str')]
        user_partitions_schema = kwargs.get("partitions_schema", None)
        if user_partitions_schema is not None:
            if user_partitions is None:
                print(
                    "ERROR: 'partitions_schema' was defined, but 'partitions'"
                    + " was not. The parameter 'partitions_schema' is only"
                    + " to be used when defining 'partitions'"
                )
                logging.error(
                    "ERROR: 'partitions_schema' was defined, but 'partitions'"
                    + " was not. The parameter 'partitions_schema' is only"
                    + " to be used when defining 'partitions'"
                )
                return
            elif not isinstance(user_partitions_schema, list) and all(
                len(schema) == 2 for schema in user_partitions_schema
            ):
                print(
                    "ERROR: 'partitions_schema' should be a list of tuples of"
                    + " the column name and column type of the form "
                    + "partitions_schema="
                    + "[('col_nameA','int32','col_nameB','str')]"
                )
                logging.error(
                    "ERROR: 'partitions_schema' should be a list of tuples of"
                    + " the column name and column type of the form "
                    + "partitions_schema="
                    + "[('col_nameA','int32','col_nameB','str')]"
                )
                return
            elif len(user_partitions_schema) != len(user_partitions):
                print(
                    "ERROR: The number of columns in 'partitions' should be"
                    + " the same as 'partitions_schema'"
                )
                logging.error(
                    "ERROR: The number of columns in 'partitions' should be"
                    + " the same as 'partitions_schema'"
                )
                return

        if isinstance(input, hive.Cursor):
            hive_table_name = kwargs.get("hive_table_name", table_name)
            hive_database_name = kwargs.get("hive_database_name", "default")
            (
                folder_list,
                hive_file_format_hint,
                extra_kwargs,
                extra_columns,
                hive_schema,
            ) = get_hive_table(
                input, hive_table_name, hive_database_name, user_partitions
            )

            if file_format_hint == "undefined":
                file_format_hint = hive_file_format_hint
            elif file_format_hint != hive_file_format_hint:
                print(
                    "WARNING: file_format specified ("
                    + str(file_format_hint)
                    + ") does not match the file_format infered by"
                    + " the Hive cursor ("
                    + str(hive_file_format_hint)
                    + "). Using user specified file_format"
                )
                logging.warning(
                    "WARNING: file_format specified ("
                    + str(file_format_hint)
                    + ") does not match the file_format infered by"
                    + " the Hive cursor ("
                    + str(hive_file_format_hint)
                    + "). Using user specified file_format"
                )

            kwargs.update(extra_kwargs)
            input = folder_list
            is_hive_input = True
        elif user_partitions is not None:
            if user_partitions_schema is None:
                print(
                    """ERROR: When using 'partitions' without a Hive cursor,
                     you also need to set 'partitions_schema' which should be
                     a list of tuples of the column name and column type of
                     the form partitions_schema=
                     [('col_nameA','int32','col_nameB','str')]"""
                )
                logging.error(
                    """ERROR: When using 'partitions' without a Hive cursor,
                     you also need to set 'partitions_schema' which should be
                     a list of tuples of the column name and column type of
                     the form partitions_schema=
                     [('col_nameA','int32','col_nameB','str')]"""
                )
                return

            hive_schema = {}
            if isinstance(input, str):
                hive_schema["location"] = input
            elif isinstance(input, list) and len(input) == 1:
                hive_schema["location"] = input[0]
            else:
                print(
                    """ERROR: When using 'partitions' without a Hive cursor,
                     the input needs to be a path to the base folder
                     of the partitioned data"""
                )
                logging.error(
                    """ERROR: When using 'partitions' without a Hive cursor,
                     the input needs to be a path to the base folder
                     of the partitioned data"""
                )
                return
            partitions_users = getPartitionsFromUserPartitions(user_partitions)
            hive_schema["partitions"] = partitions_users
            input = getFolderListFromPartitions(
                hive_schema["partitions"], hive_schema["location"]
            )

        if user_partitions_schema is not None:
            extra_columns = []
            for part_schema in user_partitions_schema:
                extra_columns.append(
                    (part_schema[0], convertTypeNameStrToCudfType(part_schema[1]))
                )

        if isinstance(input, str):
            input = [
                input,
            ]

        if isinstance(input, pandas.DataFrame):
            input = cudf.DataFrame.from_pandas(input)

        if isinstance(input, pyarrow.Table):
            if self.dask_client is not None:
                input = cudf.DataFrame.from_arrow(input)
            else:
                table = BlazingTable(table_name, input, DataType.ARROW)

        if isinstance(input, cudf.DataFrame):
            if self.dask_client is not None:
                table = BlazingTable(
                    table_name,
                    input,
                    DataType.DASK_CUDF,
                    convert_gdf_to_dask=True,
                    convert_gdf_to_dask_partitions=len(self.nodes),
                    client=self.dask_client,
                )
            else:
                table = BlazingTable(table_name, input, DataType.CUDF)
        elif isinstance(input, list):
            input = resolve_relative_path(input)

            # if we are using user defined partitions without hive,
            # we want to ignore paths we dont find.
            ignore_missing_paths = user_partitions_schema is not None
            parsedSchema = self._parseSchema(
                input, file_format_hint, kwargs, extra_columns, ignore_missing_paths
            )

            if is_hive_input or user_partitions is not None:
                uri_values = get_uri_values(
                    parsedSchema["files"],
                    hive_schema["partitions"],
                    hive_schema["location"],
                )
                num_cols = len(parsedSchema["names"])
                num_partition_cols = len(extra_columns)
                in_file = [True] * (num_cols - num_partition_cols) + [
                    False
                ] * num_partition_cols
            else:
                uri_values = []

            file_type = parsedSchema["file_type"]
            table = BlazingTable(
                table_name,
                parsedSchema["files"],
                file_type,
                files=parsedSchema["files"],
                datasource=parsedSchema["datasource"],
                calcite_to_file_indices=parsedSchema["calcite_to_file_indices"],
                args=parsedSchema["args"],
                uri_values=uri_values,
                in_file=in_file,
            )

            if is_hive_input:
                # table.column_names are the official schema column_names
                table.column_names = hive_schema["column_names"]
                # table.file_column_names are the column_names used by
                # the file (may be different)
                table.file_column_names = parsedSchema["names"]
                merged_types = []
                len_hive_column_types = len(hive_schema["column_types"])
                if len_hive_column_types == len(parsedSchema["types"]):
                    for i in range(len(parsedSchema["types"])):
                        # if the type parsed from the file is 0 we want
                        # to use the one from Hive
                        if parsedSchema["types"][i] == 0:
                            merged_types.append(hive_schema["column_types"][i])
                        else:
                            merged_types.append(parsedSchema["types"][i])
                else:
                    print(
                        """ERROR: number of hive_schema columns does not
                        match number of parsedSchema columns"""
                    )
                    logging.error(
                        """ERROR: number of hive_schema columns does not
                        match number of parsedSchema columns"""
                    )

                table.column_types = merged_types
            else:
                # table.column_names are the official schema column_names
                table.column_names = parsedSchema["names"]
                # table.file_column_names are the column_names used by
                # the file (may be different
                table.file_column_names = parsedSchema["names"]
                table.column_types = parsedSchema["types"]

            table.slices = table.getSlices(len(self.nodes))

            if len(uri_values) > 0:
                parsedMetadata = parseHiveMetadata(table, uri_values)
                table.metadata = parsedMetadata

            if parsedSchema["file_type"] == DataType.PARQUET:
                parsedMetadata = self._parseMetadata(
                    file_format_hint, table.slices, parsedSchema, kwargs
                )

                if isinstance(parsedMetadata, dask_cudf.core.DataFrame):
                    parsedMetadata = parsedMetadata.compute()
                    parsedMetadata = parsedMetadata.reset_index()

                if len(uri_values) > 0:
                    table.metadata = mergeMetadata(
                        table, parsedMetadata, table.metadata
                    )
                else:
                    table.metadata = parsedMetadata

                # lets make sure that the number of files from the metadata
                # actually matches the number of files.
                # this is to handle the cases where there is a file
                # that does not actually have data
                # files that do not have data wont show up in the metadata
                # and we will want to remove them from the table schema
                file_groups = table.metadata.groupby("file_handle_index")._grouped()
                if len(file_groups) != len(table.files):
                    table.metadata, table.files = adjust_due_missing_rowgroups(
                        table.metadata, table.files
                    )

                # now lets get the row_groups_ids from the metadata
                metadata_ids = table.metadata[
                    ["file_handle_index", "row_group_index"]
                ].to_pandas()
                grouped = metadata_ids.groupby("file_handle_index")
                row_groups_ids = []
                for group_id in grouped.groups:
                    row_indices = grouped.groups[group_id].values.tolist()
                    row_meta_ids = metadata_ids["row_group_index"]
                    row_groups_col = row_meta_ids.values.tolist()
                    row_group_ids = [row_groups_col[i] for i in row_indices]
                    row_groups_ids.append(row_group_ids)
                table.row_groups_ids = row_groups_ids

        elif isinstance(input, dask_cudf.core.DataFrame):
            table = BlazingTable(
                table_name, input, DataType.DASK_CUDF, client=self.dask_client
            )

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


        Docs:
        https://docs.blazingdb.com/docs/using-blazingsql#section-drop-tables
        """
        self.add_remove_table(table_name, False)

    def _parseSchema(
        self, input, file_format_hint, kwargs, extra_columns, ignore_missing_paths
    ):
        if self.dask_client:
            worker = tuple(self.dask_client.scheduler_info()["workers"])[0]
            connection = self.dask_client.submit(
                cio.parseSchemaCaller,
                input,
                file_format_hint,
                kwargs,
                extra_columns,
                ignore_missing_paths,
                workers=[worker],
            )
            return connection.result()
        else:
            return cio.parseSchemaCaller(
                input, file_format_hint, kwargs, extra_columns, ignore_missing_paths
            )

    def _parseMetadata(self, file_format_hint, currentTableNodes, schema, kwargs):
        if self.dask_client:
            dask_futures = []
            workers = tuple(self.dask_client.scheduler_info()["workers"])
            for worker_id, worker in enumerate(workers):
                all_files = currentTableNodes[worker_id].files
                file_subset = [file.decode() for file in all_files]
                if len(file_subset) > 0:
                    connection = self.dask_client.submit(
                        cio.parseMetadataCaller,
                        file_subset,
                        currentTableNodes[worker_id].offset,
                        schema,
                        file_format_hint,
                        kwargs,
                        workers=[worker],
                    )
                    dask_futures.append(connection)
            return dask.dataframe.from_delayed(dask_futures)

        else:
            files = [file.decode() for file in currentTableNodes[0].files]
            return cio.parseMetadataCaller(
                files, currentTableNodes[0].offset, schema, file_format_hint, kwargs
            )

    def _sliceRowGroups(self, numSlices, files, uri_values, row_groups_ids):
        total_num_rowgroups = sum([len(x) for x in row_groups_ids])
        file_index_per_rowgroups = [
            file_index
            for file_index, row_groups_for_file in enumerate(row_groups_ids)
            for row_group in row_groups_for_file
        ]
        flattened_rowgroup_ids = [
            row_group
            for row_groups_for_file in row_groups_ids
            for row_group in row_groups_for_file
        ]

        all_sliced_files = []
        all_sliced_uri_values = []
        all_sliced_row_groups_ids = []
        remaining = total_num_rowgroups
        startIndex = 0
        for i in range(0, numSlices):
            batchSize = int(remaining / (numSlices - i))
            file_indexes_for_slice = file_index_per_rowgroups[
                startIndex : startIndex + batchSize
            ]
            unique_file_indexes_for_slice = list(
                dict.fromkeys(file_indexes_for_slice)
            )  # lets get the unique indexes, but preserving order
            sliced_files = [files[i] for i in unique_file_indexes_for_slice]
            if uri_values is not None and len(uri_values) > 0:
                sliced_uri_values = [
                    uri_values[i] for i in unique_file_indexes_for_slice
                ]
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

        return (all_sliced_files, all_sliced_uri_values, all_sliced_row_groups_ids)

    def _optimize_skip_data_getSlices(
        self, current_table, scan_table_query, single_gpu
    ):
        nodeFilesList = []

        try:
            file_indices_and_rowgroup_indices = cio.runSkipDataCaller(
                current_table, scan_table_query
            )
        except cio.RunSkipDataError as e:
            print(">>>>>>>> ", e)
            file_indices_and_rowgroup_indices = {}
            file_indices_and_rowgroup_indices["skipdata_analysis_fail"] = True
            file_indices_and_rowgroup_indices["metadata"] = cudf.DataFrame()
        except Exception as e:
            raise e

        skipdata_analysis_fail = file_indices_and_rowgroup_indices[
            "skipdata_analysis_fail"
        ]
        file_indices_and_rowgroup_indices = file_indices_and_rowgroup_indices[
            "metadata"
        ]

        if not skipdata_analysis_fail:
            actual_files = []
            uri_values = []
            row_groups_ids = []

            if (
                not file_indices_and_rowgroup_indices.empty
            ):  # skipdata did not filter everything
                file_and_rowgroup_indices = (
                    file_indices_and_rowgroup_indices.to_pandas()
                )
                grouped = file_and_rowgroup_indices.groupby("file_handle_index")
                for group_id in grouped.groups:
                    row_indices = grouped.groups[group_id].values.tolist()
                    actual_files.append(current_table.files[group_id])
                    if group_id < len(current_table.uri_values):
                        uri_values.append(current_table.uri_values[group_id])
                    row_groups_col = file_and_rowgroup_indices[
                        "row_group_index"
                    ].values.tolist()
                    row_group_ids = [row_groups_col[i] for i in row_indices]
                    row_groups_ids.append(row_group_ids)

            if self.dask_client is None:
                curr_calcite = current_table.calcite_to_file_indices
                bt = BlazingTable(
                    current_table.name,
                    current_table.input,
                    current_table.fileType,
                    files=actual_files,
                    calcite_to_file_indices=curr_calcite,
                    uri_values=uri_values,
                    args=current_table.args,
                    row_groups_ids=row_groups_ids,
                    in_file=current_table.in_file,
                )
                bt.column_names = current_table.column_names
                bt.file_column_names = current_table.file_column_names
                bt.column_types = current_table.column_types
                nodeFilesList.append(bt)

            else:
                if single_gpu:
                    (
                        all_sliced_files,
                        all_sliced_uri_values,
                        all_sliced_row_groups_ids,
                    ) = self._sliceRowGroups(
                        1, actual_files, uri_values, row_groups_ids
                    )
                    i = 0
                    curr_calcite = current_table.calcite_to_file_indices
                    bt = BlazingTable(
                        current_table.name,
                        current_table.input,
                        current_table.fileType,
                        files=all_sliced_files[i],
                        calcite_to_file_indices=curr_calcite,
                        uri_values=all_sliced_uri_values[i],
                        args=current_table.args,
                        row_groups_ids=all_sliced_row_groups_ids[i],
                        in_file=current_table.in_file,
                    )
                    bt.column_names = current_table.column_names
                    bt.file_column_names = current_table.file_column_names
                    bt.column_types = current_table.column_types
                    nodeFilesList.append(bt)
                else:
                    (
                        all_sliced_files,
                        all_sliced_uri_values,
                        all_sliced_row_groups_ids,
                    ) = self._sliceRowGroups(
                        len(self.nodes), actual_files, uri_values, row_groups_ids
                    )

                    for i, node in enumerate(self.nodes):
                        curr_calcite = current_table.calcite_to_file_indices
                        bt = BlazingTable(
                            current_table.name,
                            current_table.input,
                            current_table.fileType,
                            files=all_sliced_files[i],
                            calcite_to_file_indices=curr_calcite,
                            uri_values=all_sliced_uri_values[i],
                            args=current_table.args,
                            row_groups_ids=all_sliced_row_groups_ids[i],
                            in_file=current_table.in_file,
                        )
                        bt.column_names = current_table.column_names
                        bt.file_column_names = current_table.file_column_names
                        bt.column_types = current_table.column_types
                        nodeFilesList.append(bt)

            return nodeFilesList
        else:
            if single_gpu:
                return current_table.getSlices(1)
            else:
                return current_table.getSlices(len(self.nodes))

    """
    Partition a dask_cudf DataFrame based on one or more columns.

    Parameters
    ----------

    input : the dask_cudf.DataFrame you want to partition
    by : a list of strings of the column names by which you want to partition.

    Examples
    --------

    >>> bc = BlazingContext(dask_client=client)
    >>> bc.create_table('product_reviews', "product_reviews/*.parquet")
    >>> query_1= "SELECT pr_item_sk, pr_review_content, pr_review_sk
        FROM product_reviews where pr_review_content IS NOT NULL"
    >>> product_reviews_df = bc.sql(query_1)
    >>> product_reviews_df = bc.partition(product_reviews_df,
                                by=["pr_item_sk",
                                    "pr_review_content",
                                    "pr_review_sk"])
    >>> sentences = product_reviews_df.map_partitions(
                        create_sentences_from_reviews)

    """

    def partition(self, input, by=[]):
        masterIndex = 0
        ctxToken = random.randint(0, np.iinfo(np.int32).max)

        if self.dask_client is None:
            print("Not supported...")
        else:
            if not isinstance(input, dask_cudf.core.DataFrame):
                print("Not supported...")
            else:
                partition_keys_mapping = getNodePartitionKeys(input, self.dask_client)
                df_schema = input._meta

                dask_futures = []
                for i, node in enumerate(self.nodes):
                    worker = node["worker"]
                    dask_futures.append(
                        self.dask_client.submit(
                            collectPartitionsPerformPartition,
                            masterIndex,
                            self.nodes,
                            ctxToken,
                            input,
                            partition_keys_mapping,
                            df_schema,
                            by,
                            i,  # node number
                            workers=[worker],
                        )
                    )
                result = dask.dataframe.from_delayed(dask_futures)
            return result

    def sql(
        self,
        query,
        algebra=None,
        return_futures=False,
        single_gpu=False,
        config_options={},
    ):
        """
        Query a BlazingSQL table.

        Returns results as cudf.DataFrame on single-GPU or dask_cudf.DataFrame
        when distributed (multi-GPU).

        Parameters
        ----------
        query :                     string of SQL query.
        algebra (optional) :        string of SQL algebra plan. Use this to
                    run on a relational algebra, instead of the query string.
        return_futures (optional) : defaulted to false. Set to true if you
                    want the `sql` function to return futures instead of data.
        single_gpu (optional) :     defaulted to false. Set to true if you
                    want to run the query on a single gpu, even is the
                    BlazingContext is setup with a dask cluster.
                    This is useful for manually running different queries
                     on different gpus simultaneously.
        config_options (optional) : defaulted to empty. You can use this to
                    set a specific set of config_options for this query
                    instead of the ones set in BlazingContext.
                    See BlazingContext for more info on this parameter

        Examples
        --------

        Register a public S3 bucket, then create and query a table from it:

        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        >>> bc.s3('blazingsql-colab', bucket_name='blazingsql-colab')
        >>> bc.create_table('taxi',
            's3://blazingsql-colab/yellow_taxi/1_0_0.parquet')
        <pyblazing.apiv2.context.BlazingTable at 0x7f186006a310>

        >>> result = bc.sql('SELECT vendor_id, tpep_pickup_datetime,
                passenger_count, Total_amount FROM taxi')
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
        nodeTableList = [[] for _ in range(len(self.nodes))]
        if single_gpu:
            nodeTableList = [
                [],
            ]
        fileTypes = []

        if algebra is None:
            algebra = self.explain(query)

        # when an empty `LogicalValues` appears on the optimized plan
        # there aren't neither BindableTableScan nor TableScan nor Project
        if "LogicalValues(tuples=[[]])" in algebra:
            print(
                """This SQL statement returns empty result.
                Please double check your query."""
            )
            result = cudf.DataFrame()  # it will return an empty DataFrame
            return result

        if algebra == "":
            print("Parsing Error")
            return

        if len(config_options) == 0:
            query_config_options = self.config_options
        else:
            query_config_options = {}
            for option in config_options:
                query_config_options[option.encode()] = str(
                    config_options[option]
                ).encode()  # make sure all options are encoded strings

        if self.dask_client is None or single_gpu is True:
            table_names, table_scans = cio.getTableScanInfoCaller(algebra)
        else:
            worker = tuple(self.dask_client.scheduler_info()["workers"])[0]
            connection = self.dask_client.submit(
                cio.getTableScanInfoCaller,
                algebra,
                workers=[worker],
                pure=False)
            table_names, table_scans = connection.result()

        query_tables = [self.tables[table_name] for table_name in table_names]

        # this was for ARROW tables which are currently deprecated
        # algebra = modifyAlgebraForDataframesWithOnlyWantedColumns(algebra, relational_algebra_steps,self.tables)

        for table_idx, query_table in enumerate(query_tables):
            fileTypes.append(query_table.fileType)
            ftype = query_table.fileType
            if (
                ftype == DataType.PARQUET
                or ftype == DataType.ORC
                or ftype == DataType.JSON
                or ftype == DataType.CSV
            ):
                if query_table.has_metadata():
                    currentTableNodes = self._optimize_skip_data_getSlices(
                        query_table, table_scans[table_idx], single_gpu
                    )
                else:
                    if single_gpu:
                        currentTableNodes = query_table.getSlices(1)
                    else:
                        currentTableNodes = query_table.getSlices(len(self.nodes))
            elif query_table.fileType == DataType.DASK_CUDF:
                if single_gpu:
                    # TODO: repartition onto the node that does the work

                    currentTableNodes = []
                    for node in self.nodes:
                        currentTableNodes.append(query_table)
                else:
                    currentTableNodes = query_table.getDaskDataFrameKeySlices(
                        self.nodes, self.dask_client
                    )

            elif (
                query_table.fileType == DataType.CUDF
                or query_table.fileType == DataType.ARROW
            ):
                currentTableNodes = []
                for node in self.nodes:
                    if not isinstance(query_table.input, list):
                        query_table.input = [query_table.input]
                    currentTableNodes.append(query_table)

            for j, nodeList in enumerate(nodeTableList):
                nodeList.append(currentTableNodes[j])

        ctxToken = random.randint(0, np.iinfo(np.int32).max)
        accessToken = 0

        algebra = get_plan(algebra)

        if self.dask_client is None:
            try:
                graph = cio.runGenerateGraphCaller(
                                masterIndex,
                                self.nodes,
                                nodeTableList[0],
                                table_scans,
                                fileTypes,
                                ctxToken,
                                algebra,
                                accessToken,
                                query_config_options)
                result = cio.runExecuteGraphCaller(graph, is_single_node=True)
            except cio.RunQueryError as e:
                print(">>>>>>>> ", e)
                result = cudf.DataFrame()
            except Exception as e:
                raise e
        else:
            if single_gpu == True:
                #the following is wrapped in an array because .sql expects to return
                #an array of dask_futures or a df, this makes it consistent
                worker = self.nodes[self.single_gpu_idx]["worker"]
                self.single_gpu_idx = self.single_gpu_idx + 1
                if self.single_gpu_idx >= len(self.nodes):
                    self.single_gpu_idx = 0
                graph_futures = [self.dask_client.submit(
                        generateGraphs,
                        masterIndex,
                        [self.nodes[0],],
                        nodeTableList[0],
                        table_scans,
                        fileTypes,
                        ctxToken,
                        algebra,
                        accessToken,
                        query_config_options,
                        single_gpu=True,
                        pure=False,
                        workers=[worker])]
                self.dask_client.gather(graph_futures)

                dask_futures = [self.dask_client.submit(executeGraph, ctxToken,
                pure=False)]
            else:
                graph_futures = []
                i = 0
                for node in self.nodes:
                    worker = node['worker']
                    graph_futures.append(
                        self.dask_client.submit(
                            generateGraphs,
                            masterIndex,
                            self.nodes,
                            nodeTableList[i],
                            table_scans,
                            fileTypes,
                            ctxToken,
                            algebra,
                            accessToken,
                            query_config_options,
                            workers=[worker],
                            pure=False))
                    i = i + 1
                graph_futures = self.dask_client.gather(graph_futures)

                dask_futures = []
                for node in self.nodes:
                    worker = node['worker']
                    dask_futures.append(
                        self.dask_client.submit(
                            executeGraph,
                            ctxToken,
                            workers=[worker],
                            pure=False))

            if return_futures:
                result = dask_futures
            else:
                meta_results = self.dask_client.gather(dask_futures)

                futures = []
                for query_partids, meta, worker_id in meta_results:
                    for query_partid in query_partids:
                        futures.append(self.dask_client.submit(get_element, query_partid, workers=[worker_id],
                        pure=False))

                result = dask.dataframe.from_delayed(futures, meta=meta)
        return result

    # END SQL interface

    # BEGIN LOG interface
    def log(self, query, logs_table_name="bsql_logs"):
        """
        Query BlazingSQL's internal log (bsql_logs) that records events
        from all queries run.

        Parameters
        ----------

        query : string value SQL query on bsql_logs table.
        logs_table_name (optional) : string of logs table name,
                                     'bsql_logs' by default.

        Examples
        --------

        Initialize BlazingContext and query bsql_logs
        for how long each query took:

        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        BlazingContext ready
        >>> log_result = bc.log("SELECT log_time, query_id, duration
            FROM bsql_logs WHERE info = 'Query Execution Done'
            ORDER BY log_time DESC")
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
            log_files = [
                os.path.join(self.node_log_paths[i], "RAL." + str(i) + ".log")
                for i in range(0, len(self.node_log_paths))
            ]
            dtypes = [
                "date64",
                "int32",
                "str",
                "int32",
                "int16",
                "int16",
                "str",
                "float32",
                "str",
                "int32",
                "str",
                "int32",
            ]
            names = [
                "log_time",
                "node_id",
                "type",
                "query_id",
                "step",
                "substep",
                "info",
                "duration",
                "extra1",
                "data1",
                "extra2",
                "data2",
            ]
            self.create_table(
                self.logs_table_name,
                log_files,
                delimiter="|",
                dtype=dtypes,
                names=names,
                file_format="csv",
            )

            log_schemas = {
                "bsql_queries": (
                    ["ral_id", "query_id", "start_time", "plan"],
                    ["int32", "int32", "int64", "str"],
                ),
                "bsql_kernels": (
                    ["ral_id", "query_id", "kernel_id", "is_kernel", "kernel_type"],
                    ["int32", "int32", "int64", "int16", "str"],
                ),
                "bsql_kernels_edges": (
                    ["ral_id", "query_id", "source", "sink", "port_name"],
                    ["int32", "int32", "int64", "int64", "str"],
                ),
                "bsql_kernel_events": (
                    [
                        "ral_id",
                        "query_id",
                        "kernel_id",
                        "input_num_rows",
                        "input_num_bytes",
                        "output_num_rows",
                        "output_num_bytes",
                        "event_type",
                        "timestamp_begin",
                        "timestamp_end",
                    ],
                    [
                        "int32",
                        "int32",
                        "int64",
                        "int64",
                        "int64",
                        "int64",
                        "int64",
                        "str",
                        "int64",
                        "int64",
                    ],
                ),
                "bsql_cache_events": (
                    [
                        "ral_id",
                        "query_id",
                        "source",
                        "sink",
                        "port_name",
                        "num_rows",
                        "num_bytes",
                        "event_type",
                        "timestamp_begin",
                        "timestamp_end",
                    ],
                    [
                        "int32",
                        "int32",
                        "int64",
                        "int64",
                        "int64",
                        "int64",
                        "int64",
                        "str",
                        "int64",
                        "int64",
                    ],
                ),
            }

            for log_table_name in log_schemas:
                log_files = [
                    os.path.join(
                        self.node_log_paths[i], log_table_name + "." + str(i) + ".log"
                    )
                    for i in range(0, len(self.node_log_paths))
                ]

                names, dtypes = log_schemas[log_table_name]
                self.create_table(
                    log_table_name,
                    log_files,
                    delimiter="|",
                    dtype=dtypes,
                    names=names,
                    file_format="csv",
                )

            self.logs_initialized = True

        return self.sql(query)
