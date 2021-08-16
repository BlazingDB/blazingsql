# NOTE WARNING NEVER CHANGE THIS FIRST LINE!!!! NEVER EVER
import cudf


from cudf.core.column.column import build_column
from cudf.utils.dtypes import is_decimal_dtype
from dask.distributed import get_worker
from datetime import datetime

from collections import OrderedDict

from urllib.parse import urlparse

from threading import Lock
from weakref import ref
from distributed.comm import parse_address
from pyblazing.apiv2.filesystem import FileSystem
from pyblazing.apiv2 import DataType
from pyblazing.apiv2.comms import listen
from pyblazing.apiv2.sqlengines_utils import (
    SQLEngineDataTypeMap,
    UnsupportedSQLEngineError,
)
from pyblazing.apiv2.algebra import get_json_plan, format_json_plan

import json
import collections
from pyhive import hive
from .hive import (
    convertTypeNameStrToCudfType,
    cudfTypeToCsvType,
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
from glob import glob
import cio
import dask_cudf
import dask
import jpype
import dask.distributed
import netifaces as ni

import random

import logging

from enum import IntEnum

import platform

import sys
from time import sleep

jpype.addClassPath(
    os.path.join(os.getenv("CONDA_PREFIX"), "lib/blazingsql-algebra.jar")
)
jpype.addClassPath(
    os.path.join(os.getenv("CONDA_PREFIX"), "lib/blazingsql-algebra-core.jar")
)

machine_processor = platform.processor()

if machine_processor in ("x86_64", "x64"):
    machine_processor = "amd64"

ctx_instances_ip_port = {}

the_java_home = "CONDA_PREFIX"

if "JAVA_HOME" in os.environ:
    the_java_home = "JAVA_HOME"

# NOTE felipe try first with CONDA_PREFIX/jre/lib/amd64/server/libjvm.so
# (for older Java versions e.g. 8.x)
java_home_path = os.environ[the_java_home]
jvm_path = java_home_path

if not os.path.isfile(jvm_path):
    jvm_path = java_home_path + "/lib/" + machine_processor + "/server/libjvm.so"

if not os.path.isfile(jvm_path):
    # NOTE felipe try a second time using CONDA_PREFIX/lib/server/
    # (for newer java versions e.g. 11.x)
    jvm_path = os.environ[the_java_home] + "/lib/server/libjvm.so"
    if machine_processor == "amd64":
        if not os.path.isfile(jvm_path):
            jvm_path = (
                java_home_path + "/jre/lib/" + machine_processor + "/server/libjvm.so"
            )
    elif machine_processor in ("ppc64", "ppc64le"):
        jvm_path = (
            os.environ[the_java_home]
            + "/lib/"
            + machine_processor
            + "/default/libjvm.so"
        )

jpype.startJVM("-ea", convertStrings=False, jvmpath=jvm_path)
# jpype.startJVM()

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
SqlValidationExceptionClass = jpype.JClass(
    "com.blazingdb.calcite.application.SqlValidationException"
)
SqlSyntaxExceptionClass = jpype.JClass(
    "com.blazingdb.calcite.application.SqlSyntaxException"
)
RelConversionExceptionClass = jpype.JClass(
    "org.apache.calcite.tools.RelConversionException"
)


def get_blazing_logger(is_dask):
    """
    Returns the corresponding logger according to the input flag.

    Parameters
    ----------

    is_dask : bool, whether the logger is called from a dask environment
    or locally as a client.
    """
    if is_dask:
        return logging.getLogger(get_worker().id)
    else:
        return logging.getLogger("blz_client")


class blazing_allocation_mode(IntEnum):
    CudaDefaultAllocation = (0,)
    PoolAllocation = (1,)
    CudaManagedMemory = (2,)


def initializeBlazing(
    ralId=0,
    worker_id="",
    networkInterface="lo",
    singleNode=False,
    nodes=[],
    allocator="default",
    pool=True,
    initial_pool_size=None,
    maximum_pool_size=None,
    enable_logging=False,
    config_options={},
    logging_dir_path="blazing_log",
    is_dask=False,
):

    last_str = '|%(levelname)s|||"%(message)s"||||||'
    FORMAT = "%(asctime)s|" + str(ralId) + last_str
    filename = os.path.join(logging_dir_path, "pyblazing." + str(ralId) + ".log")
    logger = get_blazing_logger(is_dask)
    handler = logging.FileHandler(filename)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    workerIp = ni.ifaddresses(networkInterface)[ni.AF_INET][0]["addr"]
    ralCommunicationPort = random.randint(10000, 32000) + ralId

    logger.info("Worker IP: %s   Port: %d", workerIp, ralCommunicationPort)

    if not pool:
        initial_pool_size = 0
        maximum_pool_size = 0
    elif pool and initial_pool_size is None:
        initial_pool_size = 0

    if maximum_pool_size is None or maximum_pool_size == 0:
        maximum_pool_size = 0
    elif maximum_pool_size < initial_pool_size:
        maximum_pool_size = initial_pool_size

    possible_allocators = [
        "default",
        "managed",
        "existing",
        "cuda_memory_resource",
        "managed_memory_resource",
        "pool_memory_resource",
        "managed_pool_memory_resource",
        "arena_memory_resource",
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
        allocator = "pool_memory_resource"
    elif pool and allocator == "managed":
        allocator = "managed_pool_memory_resource"

    import ucp.core as ucp_core

    workers_ucp_info = []
    self_port = 0
    if singleNode is False:
        worker = get_worker()
        for dask_addr in worker.ucx_addresses:
            other_worker = worker.ucx_addresses[dask_addr]
            workers_ucp_info.append(
                {
                    "worker_id": dask_addr.encode(),
                    "ip": other_worker["ip"].encode(),
                    "port": other_worker["port"],
                    "ep_handle": 0,
                    "worker_handle": 0,
                    "context_handle": ucp_core._get_ctx().context.handle,
                }
            )
        self_port = worker.ucx_addresses[worker_id]["port"]

    output_cache, input_cache, self_port = cio.initializeCaller(
        ralId,
        worker_id.encode(),
        networkInterface.encode(),
        self_port,
        workers_ucp_info,
        singleNode,
        config_options,
        allocator.encode(),
        initial_pool_size,
        maximum_pool_size,
        enable_logging,
    )
    if singleNode is False:
        worker.output_cache = output_cache
        worker.input_cache = input_cache

    if os.path.isabs(logging_dir_path):
        log_path = logging_dir_path
    else:
        log_path = os.path.join(os.getcwd(), logging_dir_path)
    return self_port, ni.ifaddresses(networkInterface)[ni.AF_INET][0]["addr"], log_path


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
    worker = get_worker()
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
    config_options,
    sql,
    current_timestamp,
):

    worker = get_worker()
    for table_index in range(len(tables)):
        if isinstance(tables[table_index].input, dask_cudf.core.DataFrame):
            print(
                "ERROR: collectPartitionsRunQuery should not be called "
                + "with an input of dask_cudf.core.DataFrame"
            )
            get_blazing_logger(is_dask=True).error(
                "collectPartitionsRunQuery should not be called "
                + "with an input of dask_cudf.core.DataFrame"
            )

        if hasattr(tables[table_index], "partition_keys"):  # this is a dask cudf table
            if len(tables[table_index].partition_keys) > 0:
                tables[table_index].input = []
                for key in tables[table_index].partition_keys:
                    tables[table_index].input.append(worker.data[key])

    try:
        graph = cio.runGenerateGraphCaller(
            masterIndex,
            worker.ucx_addresses.keys(),
            tables,
            table_scans,
            fileTypes,
            ctxToken,
            algebra,
            config_options,
            sql,
            current_timestamp,
        )
        graph.set_input_and_output_caches(worker.input_cache, worker.output_cache)
    except Exception as e:
        logger = get_blazing_logger(True)
        logger.error("runGenerateGraphCaller failed")
        raise e

    with worker._lock:
        if not hasattr(worker, "query_graphs"):
            worker.query_graphs = {}
        worker.query_graphs[ctxToken] = graph


def startExecuteGraph(ctxToken):
    worker = get_worker()

    graph = worker.query_graphs[ctxToken]
    cio.startExecuteGraphCaller(graph, ctxToken)


def getQueryIsComplete(ctxToken):
    worker = get_worker()
    graph = worker.query_graphs[ctxToken]
    ret = graph.query_is_complete()
    return ret


def queryProgressAsPandas(progress):
    progress["kernel_descriptions"] = [
        kernel.decode() for kernel in progress["kernel_descriptions"]
    ]
    pdf = pandas.DataFrame(
        list(
            zip(
                progress["kernel_descriptions"],
                progress["finished"],
                progress["batches_completed"],
            )
        ),
        columns=["kernel_descriptions", "finished", "batches_completed"],
    )
    return pdf


def getQueryProgress(ctxToken):
    worker = get_worker()

    graph = worker.query_graphs[ctxToken]
    progress = graph.get_progress()
    return queryProgressAsPandas(progress)


def getExecuteGraphResult(ctxToken):
    worker = get_worker()

    graph = worker.query_graphs[ctxToken]
    del worker.query_graphs[ctxToken]
    with worker._lock:
        dfs = cio.getExecuteGraphResultCaller(graph, ctxToken, is_single_node=False)
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

    return query_partids, meta, worker.name


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

    dtypes = []
    for t in curr_table.column_types:
        # this hack allows to read an orc file with decimal columns casting the decimal column to float64
        if t == 26:  # check decimal
            print(
                "WARNING: BlazingSQL currently does not support operations on DECIMAL datatype columns"
            )
            dtypes.append(np.dtype("float64"))
        else:
            # TIMESTAMP_DAYS (12) is not supported in cudf/python/cudf/cudf/_lib/types.pyx
            # so just for this case let's get TIMESTAMP_SECONDS
            if t != 12:
                dtypes.append(cio.cudf_type_int_to_np_types(t))
            else:
                dtypes.append(cio.cudf_type_int_to_np_types(13))

    columns = curr_table.column_names
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

    # we actually dont need this column here
    hiveMetadata = hiveMetadata.drop(columns=["row_group_index"])
    fileMetadata = fileMetadata.merge(
        hiveMetadata,
        left_on=["file_handle_index"],
        right_on=["file_handle_index"],
        how="inner",
    )
    fileMetadata = fileMetadata.sort_values(by=["file_handle_index", "row_group_index"])

    result = fileMetadata
    columns = curr_table.column_names
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
        result[col_name] = fileMetadata[col_name]

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

    result = result.reset_index()  # if we dont reset index, other logic gets messed up
    return result


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
                # by default os.path.abspath removes the trailing slash,
                # we must add them again if any
                if file[-1] == "/":  # ends in a slash
                    abs_file = abs_file + "/"
                # we check if the file exists otherwise we try to expand the
                # wildcard pattern with glob
                if os.path.exists(abs_file) or glob(abs_file):
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

    import re

    if is_absolute_path:
        # Let's group the workers by host_name
        host_worker_dict = {}
        for worker, worker_info in all_items:
            host_name = re.findall(r"[0-9]+(?:\.[0-9]+){3}", worker)[0]
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
                    True,
                    workers=[worker_list[0]],
                    pure=False,
                )
            )

        for connection in dask_futures:
            made_dir = connection.result()
            if not made_dir:
                get_blazing_logger(is_dask=False).info("Directory already exists")
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
            host_name = re.findall(r"[0-9]+(?:\.[0-9]+){3}", worker)[0]
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
                        True,
                        workers=[worker_list[0]],
                        pure=False,
                    )
                )

        for connection in dask_futures:
            made_dir = connection.result()
            if not made_dir:
                get_blazing_logger(is_dask=False).info("Directory already exists")


def initialize_server_directory(dir_path, is_dask):
    if not os.path.exists(dir_path):
        try:
            os.mkdir(dir_path)
        except OSError as error:
            get_blazing_logger(is_dask).error(
                f"Could not create directory: {dir_path}" + str(error)
            )
            raise
        return True
    else:
        return True


def get_current_directory_path():
    return os.getcwd()


# Delete all generated (older than 1 hour) orc files
def remove_orc_files_from_disk(data_dir, query_id=None):
    if os.path.isdir(data_dir):  # only if data_dir exists
        all_files = os.listdir(data_dir)
        current_time = time.time()
        for file in all_files:
            if ".blazing-temp" in file:
                full_path_file = data_dir + "/" + file
                if query_id is not None:
                    if f"-{query_id}-" in file or "-none-" in file:
                        os.remove(full_path_file)
                else:
                    creation_time = os.path.getctime(full_path_file)
                    if (current_time - creation_time) // (1 * 60 * 60) >= 1:
                        os.remove(full_path_file)


def distributed_remove_orc_files_from_disk(client, data_dir, query_id=None):
    workers = list(client.scheduler_info()["workers"])
    dask_futures = []
    for i, worker in enumerate(workers):
        worker_path = os.path.join(data_dir, f"{i}")
        dask_futures.append(
            client.submit(
                remove_orc_files_from_disk,
                worker_path,
                query_id=query_id,
                workers=[worker],
            )
        )

    client.gather(dask_futures)


def initialize_orc_files_folder(client, data_dir):
    workers = list(client.scheduler_info()["workers"])
    dask_futures = []
    for i, worker in enumerate(workers):
        worker_path = os.path.join(data_dir, f"{i}")
        dask_futures.append(
            client.submit(
                initialize_server_directory, worker_path, True, workers=[worker]
            )
        )

    client.gather(dask_futures)


# Updates the dtype from `object` to `str` to be more friendly
def convert_friendly_dtype_to_string(list_types):
    for i in range(len(list_types)):
        if list_types[i] == "object":
            list_types[i] = "str"
    return list_types


def kwargs_validation(kwargs, bc_api_str):
    """
    Validation of kwargs params when a bc API is called
    """
    # csv, parquet, orc, json params
    if bc_api_str == "create_table":
        full_kwargs = [
            "local_files",
            "file_format",
            "partitions",
            "partitions_schema",
            "hive_table_name",
            "hive_database_name",
            "names",
            "dtype",
            "delimiter",
            "skiprows",
            "skipfooter",
            "lineterminator",
            "header",
            "nrows",
            "skip_blank_lines",
            "decimal",
            "true_values",
            "false_values",
            "na_values",
            "keep_default_na",
            "na_filter",
            "quotechar",
            "quoting",
            "doublequote",
            "comment",
            "delim_whitespace",
            "skipinitialspace",
            "use_cols_indexes",
            "use_cols_names",
            "byte_range_offset",
            "byte_range_size",
            "compression",
            "lines",
            "stripes",
            "skiprows",
            "num_rows",
            "use_index",
            "max_bytes_chunk_read",  # Used for reading CSV files in chunks
            "local_files",
            "get_metadata",
            # SQL Engines arguments
            "from_sql",
            "hostname",
            "port",
            "username",
            "password",
            "database",
            "table_filter",
            "table_batch_size",
        ]
        params_info = "https://docs.blazingdb.com/docs/create_table"

    elif bc_api_str == "s3":
        full_kwargs = [
            "name",
            "bucket_name",
            "access_key_id",
            "secret_key",
            "encryption_type",
            "session_token",
            "root",
            "kms_key_amazon_resource_name",
            "endpoint_override",
            "region",
        ]
        params_info = "https://docs.blazingdb.com/docs/s3"

    elif bc_api_str == "hdfs":
        full_kwargs = ["name", "host", "port", "user", "kerb_ticket"]
        params_info = "https://docs.blazingdb.com/docs/hdfs"

    elif bc_api_str == "gs":
        full_kwargs = [
            "name",
            "project_id",
            "bucket_name",
            "use_default_adc_json_file",
            "adc_json_file",
        ]
        params_info = "https://docs.blazingdb.com/docs/google-storage"

    for arg_i in kwargs.keys():
        if arg_i not in full_kwargs:
            raise Exception(
                "ERROR: The parameter '"
                + arg_i
                + "' does not exists. Please make sure you are using the correct parameter:"
                + "\nTo get the correct parameters, check:  "
                + params_info
            )


def recognized_extension(extension):
    if len(extension) == 0:
        return False
    extension = extension[1:]  # removing `.`
    if extension in ["orc", "parquet", "json", "csv", "psv"]:
        return True
    return False


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
        local_files=False,
        mapping_files={},
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

        # This flag allows to differentiate the accessibility of the files
        # by the worker nodes. Set to True if the files are distributed,
        # for example in the case of log files.
        self.local_files = local_files

        # When local_files is True, mapping_files allows to know
        # which files reside in which nodes.
        self.mapping_files = mapping_files

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
            for x in self.input._data.values():
                # for now `decimal` type is not considered from `np_to_cudf_types_int` call
                if is_decimal_dtype(x.dtype):
                    raise Exception(
                        "ERROR: BlazingSQL currently does not support tables based on cudf DataFrames with DECIMAL datatype columns"
                    )
                    type_int = 26
                else:
                    type_int = cio.np_to_cudf_types_int(x.dtype)
                self.column_types.append(type_int)
        elif self.fileType == DataType.DASK_CUDF:
            self.column_names = [x for x in input.columns]
            for x in input.dtypes:
                # for now `decimal` type is not considered from `np_to_cudf_types_int` call
                if is_decimal_dtype(x):
                    raise Exception(
                        "ERROR: BlazingSQL currently does not support tables based on dask_cudf DataFrames with DECIMAL datatype columns"
                    )
                    type_int = 26
                else:
                    type_int = cio.np_to_cudf_types_int(x)
                self.column_types.append(type_int)

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
                bt = BlazingTable(self.name, self.input, self.fileType, args=self.args)
                bt.column_names = self.column_names
                bt.column_types = self.column_types
                nodeFilesList.append(bt)
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

    def getSlicesByWorker(self, numSlices):
        nodeFilesList = []
        if self.files is None:
            for i in range(0, numSlices):
                nodeFilesList.append(BlazingTable(self.name, self.input, self.fileType))
            return nodeFilesList

        offset_x = 0
        for target_files in self.mapping_files.values():
            bt = BlazingTable(
                self.name,
                self.input,
                self.fileType,
                files=target_files,
                calcite_to_file_indices=self.calcite_to_file_indices,
                uri_values=self.uri_values,
                args=self.args,
                row_groups_ids=self.row_groups_ids,
                in_file=self.in_file,
            )

            bt.offset = (offset_x, len(target_files))
            offset_x = offset_x + len(target_files)
            bt.column_names = self.column_names
            bt.file_column_names = self.file_column_names
            bt.column_types = self.column_types
            nodeFilesList.append(bt)

        return nodeFilesList


# NOTE The name of the env var is "BSQL_"+option_name
# For example:
# The env var 'BSQL_BLAZING_CACHE_DIRECTORY' will be 'BLAZING_CACHE_DIRECTORY' for the config_options python map
def get_config_option_from_env(option_name: str, default_value):
    sys_opt_name = "BSQL_" + option_name
    if sys_opt_name in os.environ:
        return os.environ[sys_opt_name]
    return default_value


def load_config_options_from_env(user_config_options: dict):
    config_options = {}
    default_values = {
        "JOIN_PARTITION_SIZE_THRESHOLD": 400000000,
        "CONCATENATING_CACHE_NUM_BYTES_TIMEOUT": 100,
        "MAX_JOIN_SCATTER_MEM_OVERHEAD": 500000000,
        "MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE": 8,
        "NUM_BYTES_PER_ORDER_BY_PARTITION": 400000000,
        "MAX_DATA_LOAD_CONCAT_CACHE_BYTE_SIZE": 400000000,
        "FLOW_CONTROL_BYTES_THRESHOLD": 18446744073709551615,  # see https://en.cppreference.com/w/cpp/types/numeric_limits/max
        "MAX_ORDER_BY_SAMPLES_PER_NODE": 10000,
        "BLAZING_PROCESSING_DEVICE_MEM_CONSUMPTION_THRESHOLD": 0.9,
        "BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD": 0.6,
        "BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD": 0.75,
        "BLAZING_LOGGING_DIRECTORY": "blazing_log",
        "BLAZING_CACHE_DIRECTORY": "/tmp/",
        "BLAZING_LOCAL_LOGGING_DIRECTORY": "blazing_log",
        "MEMORY_MONITOR_PERIOD": 50,
        "MAX_KERNEL_RUN_THREADS": 16,
        "EXECUTOR_THREADS": 10,
        "MAX_SEND_MESSAGE_THREADS": 20,
        "LOGGING_LEVEL": "trace",
        "LOGGING_FLUSH_LEVEL": "warn",
        "ENABLE_GENERAL_ENGINE_LOGS": True,
        "ENABLE_COMMS_LOGS": False,
        "ENABLE_TASK_LOGS": False,
        "ENABLE_OTHER_ENGINE_LOGS": False,
        "LOGGING_MAX_SIZE_PER_FILE": 1073741824,  # 1 GB
        "TRANSPORT_BUFFER_BYTE_SIZE": 1048576,  # 1 MB in bytes
        "TRANSPORT_POOL_NUM_BUFFERS": 1000,
        "PROTOCOL": "AUTO",
        "REQUIRE_ACKNOWLEDGE": False,
    }

    # key: option_name, value: default_value
    for option_name, default_value in default_values.items():
        # if the user set this option in the Blazingcontext ctor
        if option_name in user_config_options:
            config_options[option_name] = user_config_options[option_name]
        else:  # else: the user didn't specify this option so we can load it from the its env var
            config_options[option_name] = get_config_option_from_env(
                option_name, default_value
            )

    # make sure all options are encoded strings
    encoded_config_options = {}
    for option in config_options:
        encoded_config_options[option.encode()] = str(config_options[option]).encode()

    return encoded_config_options


class BlazingContext(object):
    """
    BlazingContext is the Python API of BlazingSQL. Along with initialization
    arguments allowing for easy multi-GPU distribution, the BlazingContext
    class has a number of methods which assist not only in creating and
    querying tables, but also in connecting remote data sources
    and understanding your ETL.

    Parameters
    ----------

    :param dask_client: ``Client`` object from ``dask.distributed``.
        The dask client used for
        communicating with other nodes. This is only necessary for running BlazingSQL with
        multiple nodes.
        **Default:** ``"autocheck"``
    :param network_interface: string.
        Network interface used for communicating with the
        dask-scheduler.
        **Default:** ``None``. See note below.
    :param allocator: string, allowed options are ``"default"``, ``"managed"`` or ``'existing'``.
        Where ``"managed"`` uses Unified Virtual Memory (UVM) and may use system memory
        if GPU memory runs out, or ``"existing"`` where it assumes you have already set the
        rmm allocator and therefore does not initialize it (this is for advanced users.)
        **Default:** ``"default"``
    :param pool: boolean.
        If ``True``, allocate a memory pool in the beginning. This can greatly improve performance.
        **Default:** ``False``
    :param initial_pool_size: long integer.
        Initial size of memory pool in bytes (if pool=True). If ``None``, it
        will default to using half of the GPU memory.
        **Default:** ``None``
    :param maximum_pool_size: long integer.
        Maximum size of the pool.
        **Default:** ``None``
    :param enable_logging: boolean.
        If set to ``True`` the memory allocator logging will be enabled.
        This can negatively impact performance and is aimed at advanced users.
        **Default:** ``False``
    :param enable_progress_bar: boolean.
        Set to ``True`` to display a progress bar during query executions.
        **Default:** ``False``
    :param config_options: dictionary.
        A dictionary for setting certain parameters in the engine. **Default:** ``{}``
        List of options:
            JOIN_PARTITION_SIZE_THRESHOLD: long integer
                Num bytes to try to have the
                partitions for each side of a join before doing the join.
                Too small can lead to overpartitioning, too big can lead
                to OOM errors.
                **Default:** ``400000000``
            MAX_JOIN_SCATTER_MEM_OVERHEAD: long integer
                The bigger this value, the more
                likely one of the tables of join will be scattered to all
                the nodes, instead of doing a standard hash based
                partitioning shuffle. Value is in bytes.
                **Default:** ``500000000``
            MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE: integer
                The maximum number of
                partitions that will be made for an order by.
                Increse this number if running into OOM issues when
                doing order bys with large amounts of data.
                **Default:** ``8``
            NUM_BYTES_PER_ORDER_BY_PARTITION: long integer
                The max number size in bytes
                for each order by partition. Note that,
                ``MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE`` will be enforced over
                this parameter.
                **Default:** ``400000000``
            MAX_DATA_LOAD_CONCAT_CACHE_BYTE_SIZE: long integer
                The max size in bytes to
                concatenate the batches read from the scan kernels
                **Default:** ``400000000``
            MAX_ORDER_BY_SAMPLES_PER_NODE: integer
                The max number order by samples
                to capture per node
                **Default:** ``10000``
            BLAZING_PROCESSING_DEVICE_MEM_CONSUMPTION_THRESHOLD: float
                The percent
                (as a decimal) of total GPU memory that the memory
                that the task executor will be allowed to consume.
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``0.9``
            BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD: float
                The percent
                (as a decimal) of total GPU memory that the memory
                resource will consider to be full
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``0.6``
            BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD: float
                The percent
                (as a decimal) of total host memory that the memory
                resource will consider to be full. In the presence of
                several GPUs per server, this resource will be shared
                among all of them in equal parts.
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``0.75``
            BLAZING_LOGGING_DIRECTORY: string
                A folder path to place all logging
                files. The path can be relative or absolute.
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``'blazing_log'``
            BLAZING_CACHE_DIRECTORY: string
                A folder path to place all orc files
                when start caching on Disk. The path can be relative
                or absolute.
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``'/tmp/'``
            BLAZING_LOCAL_LOGGING_DIRECTORY: string
                A folder path to place the
                client logging file on a dask environment. The path can
                be relative or absolute.
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``'blazing_log'``
            MEMORY_MONITOR_PERIOD: integer
                How often the memory monitor checks memory
                consumption. The value is in milliseconds.
                **Default:** ``50``  (milliseconds)
            MAX_KERNEL_RUN_THREADS: integer
                The number of threads available to run
                kernels simultaneously.
                **Default:** ``16``
            EXECUTOR_THREADS: integer
                The number of threads available to run executor
                tasks simultaneously.
                **Default:** ``10``
            MAX_SEND_MESSAGE_THREADS: integer
                The number of threads available to send
                outgoing messages.
                **Default:** ``20``
            LOGGING_LEVEL: string
                Set the level (as string) to register into the logs
                for the current tool of logging. Log levels have order of priority:
                ``{trace, debug, info, warn, err, critical, off}``. Using ``'trace'`` will
                registers all info.
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``'trace'``
            LOGGING_FLUSH_LEVEL: string
                Set the level (as string) of the flush for
                the current tool of logging. Log levels have order of priority:
                ``{trace, debug, info, warn, err, critical, off}``
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``'warn'``
            ENABLE_GENERAL_ENGINE_LOGS: boolean
                Enables ``'batch_logger'`` logger
                **Default:** ``True``
            ENABLE_COMMS_LOGS: boolean
                Enables ``'output_comms'`` and ``'input_comms'`` logger
                **Default:** ``False``
            ENABLE_TASK_LOGS: boolean
                Enables ``'task_logger'`` logger
                **Default:** ``False``
            ENABLE_OTHER_ENGINE_LOGS: boolean
                Enables ``'queries_logger'``, ``'kernels_logger'``,
                ``'kernels_edges_logger'``, ``'cache_events_logger'`` loggers
                **Default:** ``False``
            LOGGING_MAX_SIZE_PER_FILE: string
                Set the max size in bytes for the log files.
                **NOTE:** This parameter only works when used in the
                BlazingContext
                **Default:** ``1GB``
            TRANSPORT_BUFFER_BYTE_SIZE: string
                The size in bytes about the pinned buffer memory
                **Default:** ``1MBs``
            TRANSPORT_POOL_NUM_BUFFERS: integer
                The number of buffers in the punned buffer memory pool.
                **Default:** ``1000`` (buffers)
            PROTOCOL: string
                The protocol to use with the current BlazingContext.
                It should use what the user set. If the user does not explicitly set it,
                by default it will be set by whatever dask client is using (``'tcp'``, ``'ucx'``, ..).
                **NOTE:** This parameter only works when used in the BlazingContext.
                **Default:** ``'tcp'``

    .. note:: When using BlazingSQL with multiple nodes, you will need to set the
        correct ``network_interface`` your servers are using to communicate with the
        IP address of the dask-scheduler. You can see the different network interfaces
        and what IP addresses they serve with the bash command ``ifconfig``. The default
        is set to ``'eth0'``.

    :return: ``BlazingContext`` object
    """

    def __init__(
        self,
        dask_client="autocheck",
        network_interface=None,
        allocator="default",
        pool=False,
        initial_pool_size=None,
        maximum_pool_size=None,
        enable_logging=False,
        enable_progress_bar=False,
        config_options={},
    ):
        self.lock = Lock()
        self.finalizeCaller = cio.finalizeCaller
        self.nodes = []
        self.node_log_paths = set()
        self.config_options = load_config_options_from_env(config_options)
        self.calcite_primed = False

        logging_dir_path = "blazing_log"
        if "BLAZING_LOGGING_DIRECTORY".encode() in self.config_options:
            logging_dir_path = self.config_options[
                "BLAZING_LOGGING_DIRECTORY".encode()
            ].decode()

        self.cache_dir_path = "/tmp"  # default directory to store orc files
        if "BLAZING_CACHE_DIRECTORY".encode() in self.config_options:
            self.cache_dir_path = self.config_options[
                "BLAZING_CACHE_DIRECTORY".encode()
            ].decode()

        local_logging_dir_path = "blazing_log"
        if "BLAZING_LOCAL_LOGGING_DIRECTORY".encode() in self.config_options:
            local_logging_dir_path = self.config_options[
                "BLAZING_LOCAL_LOGGING_DIRECTORY".encode()
            ].decode()

        if dask_client == "autocheck":
            try:
                dask_client = dask.distributed.default_client()
            except ValueError:
                dask_client = None
                pass

        self.dask_client = dask_client

        host_memory_quota = 0.75
        if not "BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD".encode() in self.config_options:
            self.config_options["BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD".encode()] = str(
                host_memory_quota
            ).encode()

        if dask_client is not None:
            # if the user does not explicitly set it, it will be set by whatever dask client is using
            if self.config_options["PROTOCOL".encode()] == "AUTO".encode():
                self.config_options["PROTOCOL".encode()] = parse_address(
                    dask_client.scheduler.addr
                )[0].encode()

            distributed_initialize_server_directory(self.dask_client, logging_dir_path)

            distributed_remove_orc_files_from_disk(
                self.dask_client, self.cache_dir_path
            )
            #  first lets initialize the root cache_dir_path before initializing the ones for all the individual workers
            distributed_initialize_server_directory(
                self.dask_client, self.cache_dir_path
            )
            initialize_orc_files_folder(self.dask_client, self.cache_dir_path)

            if network_interface is None:
                import psutil

                local_addr = dask_client.scheduler_comm.comm._local_addr
                local = local_addr.split("://")[-1].split(":")[0]
                for name, addrs in psutil.net_if_addrs().items():
                    for addr in addrs:
                        if addr.address == local:
                            network_interface = name
                            break
                    if network_interface:
                        break
                if network_interface is None:
                    network_interface = "eth0"

            dask_futures = []
            i = 0

            # If all workers are on the same machine, the memory threshold is
            # split between the workers, here we are assuming that there are
            # the same number of GPUs/workers per server.
            workers_info = self.dask_client.scheduler_info()["workers"]
            if len(workers_info) == 0:
                raise Exception("No workers registered on the scheduler")

            host_list = [value["host"] for key, value in workers_info.items()]
            self.config_options["BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD".encode()] = str(
                host_memory_quota * len(set(host_list)) / len(workers_info)
            ).encode()
            # Start listener on each worker to send received messages to router
            worker_maps = listen(self.dask_client, network_interface=network_interface)
            workers = list(self.dask_client.scheduler_info()["workers"])

            self.nodes = []
            for node_name in worker_maps:
                node = {}
                node["worker"] = node_name
                node["ip"] = worker_maps[node_name]["ip"]
                node["communication_port"] = worker_maps[node_name]["port"]
                self.nodes.append(node)

            i = 0
            dask_futures = []
            for worker in workers:
                dask_futures.append(
                    self.dask_client.submit(
                        initializeBlazing,
                        ralId=i,
                        worker_id=worker,
                        networkInterface=network_interface,
                        singleNode=False,
                        nodes=self.nodes,
                        allocator=allocator,
                        pool=pool,
                        initial_pool_size=initial_pool_size,
                        maximum_pool_size=maximum_pool_size,
                        enable_logging=enable_logging,
                        config_options=self.config_options,
                        logging_dir_path=logging_dir_path,
                        workers=[worker],
                        is_dask=True,
                    )
                )
                i = i + 1

            for connection in dask_futures:
                ralPort, ralIp, log_path = connection.result()
                self.node_log_paths.add(log_path)

            # need to initialize this logging independently, in case its set
            # as a relative path and the location from where the python script
            # is running is different than the local dask workers
            initialize_server_directory(local_logging_dir_path, False)
            # this one is for the non dask side
            FORMAT = '%(asctime)s||%(levelname)s|||"%(message)s"||||||'
            filename = os.path.join(local_logging_dir_path, "pyblazing.log")

            logger = get_blazing_logger(is_dask=False)
            local_handler = logging.FileHandler(filename)
            local_handler.setFormatter(logging.Formatter(FORMAT))
            logger.addHandler(local_handler)
            logger.setLevel(logging.INFO)
            logger.propagate = False
        else:
            initialize_server_directory(logging_dir_path, False)

            # remove if exists older orc tmp files
            remove_orc_files_from_disk(self.cache_dir_path)
            initialize_server_directory(self.cache_dir_path, False)

            node = {}
            node["worker"] = ""
            self.nodes.append(node)

            ralPort, ralIp, log_path = initializeBlazing(
                ralId=0,
                worker_id="self",
                networkInterface="lo",
                singleNode=True,
                nodes=self.nodes,
                allocator=allocator,
                pool=pool,
                initial_pool_size=initial_pool_size,
                maximum_pool_size=maximum_pool_size,
                enable_logging=enable_logging,
                config_options=self.config_options,
                logging_dir_path=logging_dir_path,
                is_dask=False,
            )
            self.node_log_paths.add(log_path)

        self.fs = FileSystem()

        self.db = DatabaseClass("main")
        self.schema = BlazingSchemaClass(self.db)
        self.generator = RelationalAlgebraGeneratorClass(self.schema)
        self.tables = {}
        self.logs_initialized = False
        self.enable_progress_bar = enable_progress_bar
        self.graphs = OrderedDict()  # token -> graph

        # waitForPingSuccess(self.client)
        print("BlazingContext ready")

    def __del__(self):
        tokens = list(self.graphs.keys())
        if self.dask_client is None:  # the context is in this current process
            self.finalizeCaller(tokens)
        else:  # the context is in the dask worker process
            # TODO percy william we need to free the memory from all workers invoking the finalizer
            # We cannot just send the finalizerCaller on each worker here because python garbage collector
            # already deleted some objects related with dask, so the tornado loop from dask doesn't exists here
            # Related with -> https://github.com/BlazingDB/blazingsql/issues/1363
            pass

    def __repr__(self):
        return "BlazingContext('%s')" % (self.dask_client)

    def __str__(self):
        return self.dask_client

    # BEGIN FileSystem interface

    def localfs(self, prefix, **kwargs):
        """
        returns a boolean meaning True when Registered Successfully
        """

        return self.fs.localfs(self.dask_client, prefix, **kwargs)

    # Use result, error_msg = hdfs(args) where result can be True|False
    def hdfs(self, prefix, **kwargs):
        """
        Register a Hadoop Distributed File System (HDFS) Cluster.
        returns a boolean meaning True when Registered Successfully

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
        kwargs_validation(kwargs, "hdfs")
        return self.fs.hdfs(self.dask_client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        """
        Register an AWS S3 bucket.
        returns a boolean meaning True when Registered Successfully

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
        kwargs_validation(kwargs, "s3")
        return self.fs.s3(self.dask_client, prefix, **kwargs)

    def gs(self, prefix, **kwargs):
        """
        Register a Google Storage bucket.
        returns a boolean meaning True when Registered Successfully

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
        kwargs_validation(kwargs, "gs")
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

    def explain(self, sql, detail=False):
        """
        Returns break down of a given query's Logical Relational Algebra plan.

        Parameters
        ----------

        sql : string SQL query.
        detail : bool to print physical plan

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


        Explain physical plan this UNION query:

        >>> query = '''
        >>>         SELECT a.*
        >>>         FROM taxi_1 as a
        >>>         UNION ALL
        >>>         SELECT b.*
        >>>         FROM taxi_2 as b
        >>>             WHERE b.fare_amount < 100 OR b.passenger_count <> 4
        >>>             '''
        >>> plan = bc.explain(query, detail=True)
        >>> print(plan) TODO DFR
        LogicalUnion(all=[true])
          LogicalTableScan(table=[[main, taxi_1]])
          BindableTableScan(table=[[main, taxi_2]],
                    filters=[[OR(<($12, 100), <>($3, 4))]])

        Docs: https://docs.blazingdb.com/docs/explain
        """
        self.lock.acquire()
        try:
            algebra = self.generator.getRelationalAlgebraString(sql)

            if detail is True:
                masterIndex = 0
                ctxToken = random.randint(0, np.iinfo(np.int32).max)
                algebra = get_json_plan(str(algebra))

                if self.dask_client is None:
                    physical_plan = cio.runGeneratePhysicalGraphCaller(
                        masterIndex, ["self"], ctxToken, str(algebra)
                    )
                else:
                    dummy_nodes = [str(i) for i in range(len(self.nodes))]
                    physical_plan = cio.runGeneratePhysicalGraphCaller(
                        masterIndex, dummy_nodes, ctxToken, str(algebra)
                    )

        except SqlValidationExceptionClass as exception:
            raise Exception(exception.message())
        except SqlSyntaxExceptionClass as exception:
            raise Exception(exception.message())
        except RelConversionExceptionClass as exception:
            raise Exception(exception.message())
        finally:
            self.lock.release()

        if detail is True:
            return format_json_plan(str(physical_plan))

        return str(algebra)

    def add_remove_table(self, tableName, addTable, table=None):
        need_to_prime = False
        self.lock.acquire()
        try:
            if addTable:
                need_to_prime = not self.calcite_primed
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

        # this is because if you do multithreaded explains without it ever being called before, it will crash. Dont know why.
        if need_to_prime:
            self.explain("select * from " + tableName)
            self.lock.acquire()
            self.calcite_primed = True
            self.lock.release()

    def get_free_memory(self):
        """
        This function returns a dictionary which contains as
        key the gpuID and as value the free memory (bytes)

        Example
        --------
        # single-GPU
        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        >>> free_mem = bc.get_free_memory()
        >>> print(free_mem)
                {0: 4234220154}

        # multi-GPU (4 GPUs):
        >>> from blazingsql import BlazingContext
        >>> from dask_cuda import LocalCUDACluster
        >>> from dask.distributed import Client
        >>> cluster = LocalCUDACluster()
        >>> client = Client(cluster)
        >>> bc = BlazingContext(dask_client=client, network_interface='lo')
        >>> free_mem = bc.get_free_memory()
        >>> print(free_mem)
                {0: 4234220154, 1: 4104210987,
                 2: 4197720291, 3: 3934320116}
        """
        if self.dask_client:
            dask_futures = []
            workers_id = []
            workers = tuple(self.dask_client.scheduler_info()["workers"])
            for worker_id, worker in enumerate(workers):
                free_memory = self.dask_client.submit(
                    cio.getFreeMemoryCaller, workers=[worker], pure=False
                )
                dask_futures.append(free_memory)
                workers_id.append(worker_id)
            aslist = self.dask_client.gather(dask_futures)
            free_memory_dictionary = dict(zip(workers_id, aslist))
            return free_memory_dictionary
        else:
            free_memory_dictionary = {}
            free_memory_dictionary[0] = cio.getFreeMemoryCaller()
            return free_memory_dictionary

    def get_max_memory_used(self):
        """
        This function returns a dictionary which contains as
        key the gpuID and as value the max memory (bytes)

        Example
        --------
        # single-GPU
        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        >>> max_mem_used = bc.get_max_memory_used()
        >>> print(max_mem_used)
                {0: 4234220154}

        # multi-GPU (4 GPUs):
        >>> from blazingsql import BlazingContext
        >>> from dask_cuda import LocalCUDACluster
        >>> from dask.distributed import Client
        >>> cluster = LocalCUDACluster()
        >>> client = Client(cluster)
        >>> bc = BlazingContext(dask_client=client, network_interface='lo')
        >>> max_mem_used = bc.get_max_memory_used()
        >>> print(max_mem_used)
                {0: 4234220154, 1: 4104210987,
                 2: 4197720291, 3: 3934320116}
        """
        if self.dask_client:
            dask_futures = []
            workers_id = []
            workers = tuple(self.dask_client.scheduler_info()["workers"])
            for worker_id, worker in enumerate(workers):
                free_memory = self.dask_client.submit(
                    cio.getMaxMemoryUsedCaller, workers=[worker], pure=False
                )
                dask_futures.append(free_memory)
                workers_id.append(worker_id)
            aslist = self.dask_client.gather(dask_futures)
            free_memory_dictionary = dict(zip(workers_id, aslist))
            return free_memory_dictionary
        else:
            free_memory_dictionary = {}
            free_memory_dictionary[0] = cio.getMaxMemoryUsedCaller()
            return free_memory_dictionary

    def reset_max_memory_used(self) -> None:
        """
        This function resets the max memory usage counter to 0

        Example
        --------
        # single-GPU
        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        >>> bc.reset_max_memory_used()

        # multi-GPU (4 GPUs):
        >>> from blazingsql import BlazingContext
        >>> from dask_cuda import LocalCUDACluster
        >>> from dask.distributed import Client
        >>> cluster = LocalCUDACluster()
        >>> client = Client(cluster)
        >>> bc = BlazingContext(dask_client=client, network_interface='lo')
        >>> bc.reset_max_memory_used()
        """
        if self.dask_client:
            dask_futures = []
            workers = tuple(self.dask_client.scheduler_info()["workers"])
            for worker_id, worker in enumerate(workers):
                free_memory = self.dask_client.submit(
                    cio.resetMaxMemoryUsedCaller, workers=[worker], pure=False
                )
                dask_futures.append(free_memory)
            self.dask_client.gather(dask_futures)
        else:
            cio.resetMaxMemoryUsedCaller()

    def create_table(self, table_name, input, **kwargs):
        """
        Create a BlazingSQL table.

        Parameters
        ----------

        table_name : string of table name.
        input : data source for table.
                cudf.Dataframe, dask_cudf.DataFrame, pandas.DataFrame,
                filepath for csv, orc, parquet, etc...
        file_format (optional) : string describing the file format
                      (e.g. "csv", "orc", "parquet") this field must
                      only be set if the files do not have an extension.
        local_files (optional) : boolean, must be set to True if workers
                      only have access to a subset of the files
                      belonging to the same table. In such a case,
                      each worker will load their corresponding partitions.
        get_metadata (optional) : boolean, to use parquet and orc metadata,
                      defaults to True. When set to False it will skip
                      the process of getting metadata.

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

        kwargs_validation(kwargs, "create_table")

        get_blazing_logger(is_dask=False).info("create_table start for " + table_name)

        table = None
        extra_kwargs = {}
        in_file = []
        is_hive_input = False
        extra_columns = []
        local_files = kwargs.get("local_files", False)
        get_metadata = kwargs.get("get_metadata", True)

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
            get_blazing_logger(is_dask=False).error(
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
                get_blazing_logger(is_dask=False).error(
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
                get_blazing_logger(is_dask=False).error(
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
                get_blazing_logger(is_dask=False).error(
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
                get_blazing_logger(is_dask=False).warning(
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
        else:
            location = None
            if isinstance(input, str):
                location = input
            elif (
                isinstance(input, list)
                and len(input) == 1
                and isinstance(input[0], str)
            ):
                location = input[0]

            if (
                user_partitions is None
                and user_partitions_schema is None
                and location is not None
            ):
                folder_metadata = cio.inferFolderPartitionMetadataCaller(location)
                if len(folder_metadata) > 0:
                    user_partitions = {}
                    user_partitions_schema = []
                    for metadata in folder_metadata:
                        user_partitions[metadata["name"]] = metadata["values"]
                        user_partitions_schema.append(
                            (metadata["name"], metadata["data_type"])
                        )

            if user_partitions is not None:
                if user_partitions_schema is None:
                    print(
                        """ERROR: When using 'partitions' without a Hive cursor,
                        you also need to set 'partitions_schema' which should be
                        a list of tuples of the column name and column type of
                        the form partitions_schema=
                        [('col_nameA','int32','col_nameB','str')]"""
                    )
                    get_blazing_logger(is_dask=False).error(
                        """ERROR: When using 'partitions' without a Hive cursor,
                        you also need to set 'partitions_schema' which should be
                        a list of tuples of the column name and column type of
                        the form partitions_schema=
                        [('col_nameA','int32','col_nameB','str')]"""
                    )
                    return

                if location is None:
                    print(
                        """ERROR: When using 'partitions' without a Hive cursor,
                        the input needs to be a path to the base folder
                        of the partitioned data"""
                    )
                    get_blazing_logger(is_dask=False).error(
                        """ERROR: When using 'partitions' without a Hive cursor,
                        the input needs to be a path to the base folder
                        of the partitioned data"""
                    )
                    return

                hive_schema = {}
                hive_schema["location"] = location
                hive_schema["partitions"] = getPartitionsFromUserPartitions(
                    user_partitions
                )
                input = getFolderListFromPartitions(
                    hive_schema["partitions"], hive_schema["location"]
                )

                extra_columns = []
                for part_schema in user_partitions_schema:
                    cudf_type = (
                        convertTypeNameStrToCudfType(part_schema[1])
                        if isinstance(part_schema[1], str)
                        else part_schema[1]
                    )
                    extra_columns.append((part_schema[0], cudf_type))

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
        elif isinstance(input, list) and "from_sql" not in kwargs:
            input = resolve_relative_path(input)

            # if we are using user defined partitions without hive,
            # we want to ignore paths we dont find. Also, we should
            # ignore missing files in case some worker hasn't any
            # partition file when local_files is True
            ignore_missing_paths = (user_partitions_schema is not None) or (
                local_files is True
            )

            # /path/to/data/file.txt -> name_file = /path/to/data/file, extension = "txt"
            # /path/to/data/file_wo_extens -> name_file = /path/to/data/file_wo_extens, extension = ''
            # /path/to/data/folder/ -> name_file = /path/to/data/folder/, extension = ''
            name_file, extension = os.path.splitext(input[0])

            if not recognized_extension(extension) and file_format_hint == "undefined":
                raise Exception(
                    "ERROR: Your input file doesn't have a recognized extension, "
                    + "you have to specify the `file_format` parameter. "
                    + "Recognized extensions are: [orc, parquet, csv, json, psv]."
                    + "\nFor example if you are using a *.log file you must pass file_format='csv' "
                    + "with all the needed extra parameters. See https://docs.blazingdb.com/docs/creating-tables"
                )

            if (
                file_format_hint == "undefined"
                and extension == ""
                and input[0][-1] != "/"
            ):
                raise Exception(
                    "ERROR: if your input file doesn't have an extension, you have to specify the `file_format`. Or if its a directory, it needs to end in a slash"
                )

            for p_schema, type_schema in extra_columns:
                if "names" in kwargs:
                    found = p_schema in kwargs["names"]
                    if found:
                        id = kwargs["names"].index(p_schema)
                        kwargs["names"].pop(id)
                        kwargs["dtype"].pop(id)

            parsedSchema, parsed_mapping_files = self._parseSchema(
                input,
                file_format_hint,
                kwargs,
                extra_columns,
                ignore_missing_paths,
                local_files,
            )

            parsedSchema["names"] = [i.decode() for i in parsedSchema["names"]]

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
                local_files=local_files,
                mapping_files=parsed_mapping_files,
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
                    get_blazing_logger(is_dask=False).error(
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

            # this is particularly important for csv files to ensure that if it was set to implicitly determine,
            # it only did so for the first file. For the rest we want to guarantee that they are all returning
            # the same types, so we are setting it in the args
            table.args["names"] = table.column_names
            table.args["has_header_csv"] = parsedSchema["has_header_csv"]

            dtypes_list = []
            for i in range(0, len(table.column_types)):
                dtype_str = cudfTypeToCsvType[table.column_types[i]]
                # cudfTypeToCsvType uses: timestamp[s], timestamp[ms], timestamp[us], timestamp[ns]
                if "timestamp" in dtype_str:
                    dtypes_list.append("date64")
                else:
                    dtypes_list.append(dtype_str)
            table.args["dtype"] = dtypes_list

            if table.local_files is False:
                table.slices = table.getSlices(len(self.nodes))
            else:
                table.slices = table.getSlicesByWorker(len(self.nodes))

            parsedMetadata = None
            if len(uri_values) > 0:
                parsedMetadata = parseHiveMetadata(table, uri_values)
                table.metadata = parsedMetadata

            # When reading CSV files, we want to set up by default the size of chunks
            if "max_bytes_chunk_read" in parsedSchema["args"].keys():
                if parsedSchema["args"]["max_bytes_chunk_read"] <= 0:
                    parsedSchema["args"]["max_bytes_chunk_read"] = 268435456  # 256 MBs
            else:
                parsedSchema["args"]["max_bytes_chunk_read"] = 268435456  # 256 MBs

            # TODO: if still reading ORC metadata has issues then we can skip it
            # using get_metadata argument equals to False
            if get_metadata and (
                parsedSchema["file_type"] == DataType.PARQUET
                or parsedSchema["file_type"] == DataType.ORC
                or parsedSchema["file_type"] == DataType.CSV
            ):
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
                    row_groups_col = row_meta_ids.tolist()
                    row_group_ids = [row_groups_col[i] for i in row_indices]
                    row_groups_ids.append(row_group_ids)

                table.row_groups_ids = row_groups_ids

        elif isinstance(input, dask_cudf.core.DataFrame):
            table = BlazingTable(
                table_name, input, DataType.DASK_CUDF, client=self.dask_client
            )

        if "from_sql" in kwargs:
            sqlEngineName = kwargs["from_sql"]

            try:
                sqlEngineDataType = SQLEngineDataTypeMap[sqlEngineName]
            except KeyError as error:
                raise UnsupportedSQLEngineError(sqlEngineName) from error

            kwargs["table"] = input[0]
            parsedSchema, _ = self._parseSchema(
                input, sqlEngineName, kwargs, extra_columns, False, False
            )

            # TODO: merge parsed schema info about columns and types into tables
            table = BlazingTable(
                table_name,
                input,
                sqlEngineDataType,
                args=kwargs,
                client=self.dask_client,
            )
            table.column_names = parsedSchema["names"]
            table.column_types = parsedSchema["types"]

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

    def list_tables(self):
        """
        Returns a list with the names of all created tables.

        Example
        --------

        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        >>> bc.create_table('product_reviews', "product_reviews/*.parquet")
        >>> bc.create_table('store_sales', "store_sales/*.parquet")
        >>> bc.create_table('nation', "nation/*.parquet")
        >>> tables = bc.list_tables()
        >>> print(tables)
                  ['product_reviews', 'store_sales', 'nation']
        """
        return list(self.tables.keys())

    def describe_table(self, table_name):
        """
        Returns a dictionary with the names of all the columns and their types
        for the specified table. A ValueError is thrown if the table is not found.

        Parameters
        ----------

        table_name : string of the table name to describe

        Example
        --------

        >>> from blazingsql import BlazingContext
        >>> bc = BlazingContext()
        >>> bc.create_table('nation', "nation/*.parquet")
        >>> info_table = bc.describe_table("nation")
        >>> print(info_table)
                  {'n_nationkey': 'int32', 'n_name': 'str',
                   'n_regionkey': 'int32', 'n_comment': 'str'}
        """
        all_table_names = self.list_tables()
        if table_name in all_table_names:
            column_names = self.tables[table_name].column_names
            column_types_int = self.tables[table_name].column_types
            column_types_np = [
                cio.cudf_type_int_to_np_types(t) for t in column_types_int
            ]
            column_types = [t.name for t in column_types_np]
            column_types_friendly = convert_friendly_dtype_to_string(column_types)
            name_type_dictionary = dict(zip(column_names, column_types_friendly))
            return name_type_dictionary
        else:
            raise ValueError("ERROR: Not found table: " + str(table_name))

    def _parseSchema(
        self,
        input,
        file_format_hint,
        kwargs,
        extra_columns,
        ignore_missing_paths,
        local_files,
    ):
        if self.dask_client:
            if local_files is False:
                # just the first worker parse the entire file schemas
                worker = tuple(self.dask_client.scheduler_info()["workers"])[0]
                connection = self.dask_client.submit(
                    cio.parseSchemaCaller,
                    input,
                    file_format_hint,
                    kwargs,
                    extra_columns,
                    ignore_missing_paths,
                    workers=[worker],
                    pure=False,
                )
                parsed_schema = connection.result()
                if len(parsed_schema["files"]) == 0 and file_format_hint not in [
                    "mysql",
                    "postgresql",
                    "sqlite",
                ]:
                    raise Exception(
                        "ERROR: The file pattern specified did not match any files"
                    )
                return parsed_schema, {"localhost": parsed_schema["files"]}
            else:
                # each worker parse all accesible files on the file path
                dask_futures = []

                for worker in list(self.dask_client.scheduler_info()["workers"]):
                    dask_futures.append(
                        (
                            self.dask_client.submit(
                                cio.parseSchemaCaller,
                                input,
                                file_format_hint,
                                kwargs,
                                extra_columns,
                                ignore_missing_paths,
                                workers=[worker],
                                pure=False,
                            ),
                            worker,
                        )
                    )

                # After listing the files accessible by each worker, it could
                # happen that several workers that were started from the same
                # node have more than one shared file.
                # So, to avoid duplicate reads, we will group the files by node
                # as long as the current file does not already exist in another
                # group.
                return_object = {}
                all_files = {}
                for future, worker in dask_futures:
                    result = future.result()

                    for key in result:
                        if key == "files":
                            # Remove possible duplicated files
                            # TODO: This duplicate removal mechanism must
                            # be revisited, consider scenarios of very
                            # varied topologies.
                            # A possible improvement is that if it is detected
                            # that several workers are effectively inside a node
                            # and have access to the same files, the files
                            # should be distributed evenly among all of them.
                            if key in return_object:
                                all_files[worker] = []

                                for file_item in result[key]:
                                    if file_item not in return_object[key]:
                                        all_files[worker].append(file_item)
                            else:
                                all_files[worker] = result[key]
                                return_object[key] = {}

                            return_object[key].update(dict.fromkeys(result[key], None))
                        else:
                            if key not in return_object or (
                                key in return_object and len(result["files"]) > 0
                            ):
                                return_object[key] = result[key]
                if len(return_object["files"]) == 0:
                    raise Exception(
                        "ERROR: The file pattern specified did not match any files"
                    )
                return_object["files"] = list(return_object["files"])
                return return_object, all_files
        else:
            parsed_schema = cio.parseSchemaCaller(
                input, file_format_hint, kwargs, extra_columns, ignore_missing_paths
            )
            return parsed_schema, {"localhost": parsed_schema["files"]}

    def _parseMetadata(self, file_format_hint, currentTableNodes, schema, kwargs):

        # To have compatibility in cython side
        schema["names"] = [i.encode() for i in schema["names"]]
        if "names" in kwargs:
            kwargs["names"] = [i.encode() for i in kwargs["names"]]

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
                        pure=False,
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

    def _sliceRowGroupsByWorker(
        self, numSlices, files, uri_values, row_groups_ids, mapping_files
    ):
        dict_files = {}
        for i in range(len(files)):
            dict_files[files[i]] = row_groups_ids[i]

        all_sliced_files = []
        all_sliced_uri_values = []
        all_sliced_row_groups_ids = []

        for target_files in mapping_files.values():
            sliced_files = [
                file_name for file_name in target_files if file_name in dict_files
            ]
            sliced_uri_values = []
            sliced_rowgroup_ids = [dict_files[file_name] for file_name in sliced_files]

            all_sliced_files.append(sliced_files)
            all_sliced_uri_values.append(sliced_uri_values)
            all_sliced_row_groups_ids.append(sliced_rowgroup_ids)

        return (all_sliced_files, all_sliced_uri_values, all_sliced_row_groups_ids)

    def _expand_to_one_rowgroup_per_file(self, files, uri_values, row_groups_ids):
        files = sum(
            ([files[i]] * len(row_groups_ids[i]) for i in range(len(files))), []
        )
        uri_values = sum(
            ([uri_values[i]] * len(row_groups_ids[i]) for i in range(len(uri_values))),
            [],
        )
        row_groups_ids = [
            [row_groups_ids[i][j]]
            for i in range(len(row_groups_ids))
            for j in range(len(row_groups_ids[i]))
        ]
        return (files, uri_values, row_groups_ids)

    def _optimize_skip_data_getSlices(self, current_table, scan_table_query):
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
                    ].tolist()
                    row_group_ids = [row_groups_col[i] for i in row_indices]
                    row_groups_ids.append(row_group_ids)

        else:
            actual_files = current_table.files
            uri_values = current_table.uri_values
            row_groups_ids = current_table.row_groups_ids

        if self.dask_client is None:
            # for CSV files broken into batches due to `max_bytes_chunk_read`, then lets have just one "row_group" per file
            if current_table.fileType == DataType.CSV:
                (
                    actual_files,
                    uri_values,
                    row_groups_ids,
                ) = self._expand_to_one_rowgroup_per_file(
                    actual_files, uri_values, row_groups_ids
                )

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
            if current_table.local_files is False:
                (
                    all_sliced_files,
                    all_sliced_uri_values,
                    all_sliced_row_groups_ids,
                ) = self._sliceRowGroups(
                    len(self.nodes), actual_files, uri_values, row_groups_ids
                )
            else:
                (
                    all_sliced_files,
                    all_sliced_uri_values,
                    all_sliced_row_groups_ids,
                ) = self._sliceRowGroupsByWorker(
                    len(self.nodes),
                    actual_files,
                    uri_values,
                    row_groups_ids,
                    current_table.mapping_files,
                )
            # for CSV files broken into batches due to `max_bytes_chunk_read`, then lets have just one "row_group" per file
            if current_table.fileType == DataType.CSV:
                # make this into a function:
                for node_ind in range(len(all_sliced_files)):
                    (
                        all_sliced_files[node_ind],
                        all_sliced_uri_values[node_ind],
                        all_sliced_row_groups_ids[node_ind],
                    ) = self._expand_to_one_rowgroup_per_file(
                        all_sliced_files[node_ind],
                        all_sliced_uri_values[node_ind],
                        all_sliced_row_groups_ids[node_ind],
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

    """
    This function has been Deprecated. It is recommended to use ddf.shuffle(on=[colnames])

    """

    def partition(self, input, by=[]):
        print(
            "This function has been Deprecated. It is recommended to use ddf.shuffle(on=[colnames])"
        )

    def _get_results_distributed(self, ctxToken):
        self.do_progress_bar(
            ctxToken,
            self._run_progress_bar_distributed,
            self._wait_completed_distributed,
        )

        dask_futures = []
        for node in self.nodes:
            worker = node["worker"]
            dask_futures.append(
                self.dask_client.submit(
                    getExecuteGraphResult, ctxToken, workers=[worker], pure=False
                )
            )

        try:
            meta_results = self.dask_client.gather(dask_futures)
        except Exception as e:
            distributed_remove_orc_files_from_disk(
                self.dask_client, self.cache_dir_path, ctxToken
            )
            raise e

        futures = []
        for query_partids, meta, worker_id in meta_results:
            for query_partid in query_partids:
                futures.append(
                    self.dask_client.submit(
                        get_element, query_partid, workers=[worker_id], pure=False,
                    )
                )
        self.graphs[ctxToken] = None  # NOTE we need to invalidate the graph
        return dask.dataframe.from_delayed(futures, meta=meta)

    def _get_results_single_node(self, ctxToken):
        graph = self.graphs[ctxToken]
        self.do_progress_bar(
            graph, self._run_progress_bar_single_node, self._wait_completed_single_node,
        )
        self.graphs[ctxToken] = None  # NOTE we need to invalidate the graph
        return cio.getExecuteGraphResultCaller(graph, ctxToken, is_single_node=True)

    def fetch(self, token):
        if self.dask_client is None:
            return self._get_results_single_node(token)
        return self._get_results_distributed(token)

    def sql(
        self, query, algebra=None, config_options={}, return_token: bool = False,
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

        if ") OVER (" in algebra:
            print(
                "WARNING: Window Functions are currently an experimental feature and not fully supported or tested"
            )

        if algebra == "":
            print("Parsing Error")
            return

        table_names = []

        # Make sure the timestamp value be unique for all the nodes
        current_timestamp = str(datetime.now()).encode()

        if len(config_options) == 0:
            query_config_options = self.config_options
        else:
            query_config_options = {}
            for option in config_options:
                query_config_options[option.encode()] = str(
                    config_options[option]
                ).encode()  # make sure all options are encoded strings

        if self.dask_client is None:
            table_names, table_scans = cio.getTableScanInfoCaller(algebra)
        else:
            worker = tuple(self.dask_client.scheduler_info()["workers"])[0]
            connection = self.dask_client.submit(
                cio.getTableScanInfoCaller, algebra, workers=[worker], pure=False
            )
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
                        query_table, table_scans[table_idx]
                    )
                else:
                    # If all files are accessible by all nodes,
                    # it is better to distribute them in the old way
                    # otherwise, each node is responsible for the files
                    # it has access to.
                    if query_table.local_files is False:
                        currentTableNodes = query_table.getSlices(len(self.nodes))
                    else:
                        currentTableNodes = query_table.getSlicesByWorker(
                            len(self.nodes)
                        )
            elif query_table.fileType == DataType.DASK_CUDF:
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

            elif (
                query_table.fileType == DataType.MYSQL
                or query_table.fileType == DataType.SQLITE
                or query_table.fileType == DataType.POSTGRESQL
            ):
                if query_table.has_metadata():
                    currentTableNodes = self._optimize_skip_data_getSlices(
                        query_table, table_scans[table_idx]
                    )
                else:
                    # If all files are accessible by all nodes,
                    # it is better to distribute them in the old way
                    # otherwise, each node is responsible for the files
                    # it has access to.
                    if query_table.local_files is False:
                        currentTableNodes = query_table.getSlices(len(self.nodes))
                    else:
                        currentTableNodes = query_table.getSlicesByWorker(
                            len(self.nodes)
                        )

            for j, nodeList in enumerate(nodeTableList):
                nodeList.append(currentTableNodes[j])

        ctxToken = random.randint(0, np.iinfo(np.int32).max)

        algebra = get_json_plan(algebra)

        if self.dask_client is None:
            try:
                graph = cio.runGenerateGraphCaller(
                    masterIndex,
                    ["self"],
                    nodeTableList[0],
                    table_scans,
                    fileTypes,
                    ctxToken,
                    algebra,
                    query_config_options,
                    query,
                    current_timestamp,
                )
                cio.startExecuteGraphCaller(graph, ctxToken)
                self.graphs[ctxToken] = graph

                if not return_token:
                    return self._get_results_single_node(ctxToken)
                else:
                    return ctxToken
            except cio.RunExecuteGraphError as e:
                remove_orc_files_from_disk(self.cache_dir_path, ctxToken)
                raise e
            except cio.RunGenerateGraphError as e:
                raise e
            except Exception as e:
                raise e
        else:
            worker_ids = []
            for worker in self.dask_client.scheduler_info()["workers"]:
                worker_ids.append(worker)
            graph_futures = []
            i = 0
            for node in self.nodes:
                worker = node["worker"]
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
                        query_config_options,
                        query,
                        current_timestamp,
                        workers=[worker],
                        pure=False,
                    )
                )
                i = i + 1
            graphs = self.dask_client.gather(graph_futures)
            self.graphs[ctxToken] = graphs

            dask_futures = []
            for node in self.nodes:
                worker = node["worker"]
                dask_futures.append(
                    self.dask_client.submit(
                        startExecuteGraph, ctxToken, workers=[worker], pure=False
                    )
                )

            self.dask_client.gather(dask_futures)
            if not return_token:
                return self._get_results_distributed(ctxToken)
            else:
                return ctxToken

    def status(self, token):
        if token not in self.graphs:
            raise Exception(
                "ERROR: The graph associated with the token '"
                + str(token)
                + "' does not exists. Please make sure you have a valid token."
            )

        if self.graphs[token] is None:  # then the executution was done
            return True

        if self.dask_client is None:
            return self.graphs[token].query_is_complete()
        return self._is_query_completed_distributed(token)

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
                os.path.join(log_path, "RAL.*.log") for log_path in self.node_log_paths
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
                local_files=True,
            )

            log_schemas = {
                "bsql_queries": (
                    ["ral_id", "query_id", "start_time", "plan", "query"],
                    ["int32", "int32", "date64", "str", "str"],
                ),
                "bsql_kernels": (
                    [
                        "ral_id",
                        "query_id",
                        "kernel_id",
                        "is_kernel",
                        "kernel_type",
                        "description",
                    ],
                    ["int32", "int32", "int32", "bool", "str", "str"],
                ),
                "bsql_kernels_edges": (
                    ["ral_id", "query_id", "source", "sink"],
                    ["int32", "int32", "int32", "int32"],
                ),
                "bsql_kernel_tasks": (
                    [
                        "time_started",
                        "ral_id",
                        "query_id",
                        "kernel_id",
                        "duration_decaching",
                        "duration_execution",
                        "input_num_rows",
                        "input_num_bytes",
                    ],
                    [
                        "date64",
                        "int32",
                        "int32",
                        "int32",
                        "int64",
                        "int64",
                        "int64",
                        "int64",
                    ],
                ),
                "bsql_cache_events": (
                    [
                        "ral_id",
                        "query_id",
                        "message_id",
                        "cache_id",
                        "num_rows",
                        "num_bytes",
                        "event_type",
                        "timestamp_begin",
                        "timestamp_end",
                        "description",
                    ],
                    [
                        "int32",
                        "int32",
                        "str",
                        "int32",
                        "int64",
                        "int64",
                        "str",
                        "date64",
                        "date64",
                        "str",
                    ],
                ),
                "input_comms": (
                    [
                        "unique_id",
                        "ral_id",
                        "query_id",
                        "kernel_id",
                        "dest_ral_id",
                        "dest_ral_count",
                        "dest_cache_id",
                        "message_id",
                        "phase",
                    ],
                    [
                        "int64",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "str",
                        "str",
                    ],
                ),
                "output_comms": (
                    [
                        "unique_id",
                        "ral_id",
                        "query_id",
                        "kernel_id",
                        "dest_ral_id",
                        "dest_ral_count",
                        "dest_cache_id",
                        "message_id",
                        "phase",
                    ],
                    [
                        "int64",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "int32",
                        "str",
                        "str",
                    ],
                ),
            }

            for log_table_name in log_schemas:

                options = {
                    "ENABLE_TASK_LOGS": ["bsql_kernel_tasks"],
                    "ENABLE_OTHER_ENGINE_LOGS": [
                        "bsql_queries",
                        "bsql_kernels",
                        "bsql_kernels_edges",
                        "bsql_cache_events",
                    ],
                    "ENABLE_GENERAL_ENGINE_LOGS": ["bsql_logs"],
                    "ENABLE_COMMS_LOGS": ["input_comms", "output_comms"],
                }

                if log_table_name in options["ENABLE_TASK_LOGS"]:
                    if (
                        self.config_options["ENABLE_TASK_LOGS".encode()].decode()
                        == "False"
                    ):
                        continue

                if log_table_name in options["ENABLE_OTHER_ENGINE_LOGS"]:
                    if (
                        self.config_options[
                            "ENABLE_OTHER_ENGINE_LOGS".encode()
                        ].decode()
                        == "False"
                    ):
                        continue

                if log_table_name in options["ENABLE_GENERAL_ENGINE_LOGS"]:
                    if (
                        self.config_options[
                            "ENABLE_GENERAL_ENGINE_LOGS".encode()
                        ].decode()
                        == "False"
                    ):
                        continue

                if log_table_name in options["ENABLE_COMMS_LOGS"]:
                    if (
                        self.config_options["ENABLE_COMMS_LOGS".encode()].decode()
                        == "False"
                    ):
                        continue

                log_files = [
                    os.path.join(log_path, log_table_name + ".*.log")
                    for log_path in self.node_log_paths
                ]

                names, dtypes = log_schemas[log_table_name]
                self.create_table(
                    log_table_name,
                    log_files,
                    delimiter="|",
                    dtype=dtypes,
                    names=names,
                    file_format="csv",
                    local_files=True,
                )

            self.logs_initialized = True

        return self.sql(query)

    def _get_progress_bar_format(self):
        pbfmt = "Steps Complete {n_fmt}/{total_fmt}|{bar}|{percentage:3.0f}% ({elapsed} elapsed)"
        return pbfmt

    def _wait_completed_single_node(self, graph):
        query_complete = False
        while not query_complete:
            query_complete = graph.query_is_complete()
            sleep(0.005)

    def _wait_completed_distributed(self, ctxToken):
        query_complete = False
        while not query_complete:
            dask_futures = []
            for node in self.nodes:
                worker = node["worker"]
                dask_futures.append(
                    self.dask_client.submit(
                        getQueryIsComplete, ctxToken, workers=[worker], pure=False
                    )
                )
            workers_is_complete = self.dask_client.gather(dask_futures)
            query_complete = all(workers_is_complete)  # all workers returned true
            sleep(0.005)

    def _run_progress_bar_single_node(self, graph):
        from tqdm.auto import tqdm

        query_complete = False

        progress = graph.get_progress()
        thepdf = queryProgressAsPandas(progress)
        themax = len(thepdf)
        batches_completed = thepdf["batches_completed"].sum()
        thepdf = thepdf.drop(thepdf[~thepdf.finished].index)
        thesteps = len(thepdf)

        pbar = tqdm(
            total=themax,
            miniters=1,
            bar_format=self._get_progress_bar_format(),
            leave=True,
        )
        pbar2 = tqdm(
            miniters=1, bar_format="Total Batches Processed: {n_fmt}", leave=True
        )

        pbar.update(thesteps)
        pbar2.update(batches_completed)
        last = thesteps
        last_sum_batches = batches_completed
        while True:
            query_complete = graph.query_is_complete()
            progress = graph.get_progress()
            pdf = queryProgressAsPandas(progress)
            batches_completed = pdf["batches_completed"].sum()
            pdf = pdf.drop(pdf[~pdf.finished].index)
            thesteps = len(pdf)
            pbar.update(thesteps - last)
            last = thesteps
            pbar2.update(batches_completed - last_sum_batches)
            last_sum_batches = batches_completed
            sleep(0.005)
            if query_complete:
                break
        pbar.close()
        pbar2.close()

    def _check_tqdm(self):
        tqdm_found = True
        try:
            from tqdm import tqdm
        except ImportError:
            tqdm_found = False
            err_msg = "Warning: Could not set the progress bar, please install tqdm"
            print(err_msg)
        return tqdm_found

    def _is_query_completed_distributed(self, ctxToken):
        dask_futures = []
        for node in self.nodes:
            worker = node["worker"]
            dask_futures.append(
                self.dask_client.submit(
                    getQueryIsComplete, ctxToken, workers=[worker], pure=False
                )
            )
        workers_is_complete = self.dask_client.gather(dask_futures)
        query_complete = all(workers_is_complete)  # all workers returned true|false
        return query_complete

    def _run_progress_bar_distributed(self, ctxToken):
        from tqdm.auto import tqdm

        ispbarCreated = False
        pbar = None
        query_complete = False
        last = -1
        themax = -1
        last_sum_batches = -1
        while True:
            query_complete = self._is_query_completed_distributed(ctxToken)

            dask_futures = []
            for node in self.nodes:
                worker = node["worker"]
                dask_futures.append(
                    self.dask_client.submit(
                        getQueryProgress, ctxToken, workers=[worker], pure=False
                    )
                )
            workers_progress = dask.dataframe.from_delayed(dask_futures).compute()
            themax = len(workers_progress)
            pdf = workers_progress
            batches_completed = pdf["batches_completed"].sum()
            pdf = pdf.drop(pdf[~pdf.finished].index)
            thesteps = len(pdf)
            if not ispbarCreated:
                ispbarCreated = True
                pbar = tqdm(
                    total=themax,
                    miniters=1,
                    bar_format=self._get_progress_bar_format(),
                    leave=True,
                )
                pbar.update(thesteps)
                last = thesteps

                pbar2 = tqdm(
                    miniters=1,
                    bar_format="Total Batches Processed: {n_fmt}",
                    leave=True,
                )
                last_sum_batches = batches_completed
            else:
                pbar.update(thesteps - last)
                last = thesteps

                pbar2.update(batches_completed - last_sum_batches)
                last_sum_batches = batches_completed

            sleep(0.005)
            if query_complete:
                break

        if ispbarCreated:
            pbar.close()
            pbar2.close()

    def do_progress_bar(self, arg, progress_bar_fn, wait_fn):
        if not self.enable_progress_bar:
            wait_fn(arg)
            return

        tqdm_found = self._check_tqdm()

        if not tqdm_found:
            wait_fn(arg)
            return

        progress_bar_fn(arg)
