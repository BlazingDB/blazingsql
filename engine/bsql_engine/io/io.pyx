# io.pyx

# Copyright (c) 2018, BlazingDB.

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from collections import OrderedDict

from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.map cimport map
from libcpp.memory cimport unique_ptr, shared_ptr, make_shared, make_unique
from cython.operator cimport dereference as deref, postincrement
from libc.stdint cimport uintptr_t
from libc.stdlib cimport malloc, free
from libc.string cimport strcpy, strlen
from pyarrow.lib cimport *

from enum import IntEnum

import numpy as np
import pandas as pd
import pyarrow as pa

import cudf

from cudf._lib cimport *
from cudf._lib.types import (
    SUPPORTED_NUMPY_TO_LIBCUDF_TYPES as np_to_cudf_types,
    LIBCUDF_TO_SUPPORTED_NUMPY_TYPES as cudf_to_np_types
)
from cudf._lib.cpp.types cimport type_id
from cudf._lib.types cimport underlying_type_t_type_id
from cudf._lib.cpp.io.types cimport compression_type

from bsql_engine.io cimport cio
from bsql_engine.io.cio cimport *
from cpython.ref cimport PyObject
from cython.operator cimport dereference, postincrement

from cudf._lib.utils cimport data_from_unique_ptr

from libcpp.utility cimport pair
import logging

ctypedef int32_t underlying_type_t_compression

class Compression(IntEnum):
    INFER = (
        <underlying_type_t_compression> compression_type.AUTO
    )
    SNAPPY = (
        <underlying_type_t_compression> compression_type.SNAPPY
    )
    GZIP = (
        <underlying_type_t_compression> compression_type.GZIP
    )
    BZ2 = (
        <underlying_type_t_compression> compression_type.BZIP2
    )
    BROTLI = (
        <underlying_type_t_compression> compression_type.BROTLI
    )
    ZIP = (
        <underlying_type_t_compression> compression_type.ZIP
    )
    XZ = (
        <underlying_type_t_compression> compression_type.XZ
    )

# TODO: module for errors and move pyerrors to cpyerrors
class BlazingError(Exception):
    """Base class for blazing errors."""
cdef public PyObject * BlazingError_ = <PyObject *>BlazingError

class InitializeError(BlazingError):
    """Initialization Error."""
cdef public PyObject * InitializeError_ = <PyObject *>InitializeError

class FinalizeError(BlazingError):
    """Finalize Error."""
cdef public PyObject * FinalizeError_ = <PyObject *>FinalizeError

class GetFreeMemoryError(BlazingError):
    """GetFreeMemory Error."""
cdef public PyObject * GetFreeMemoryError_ = <PyObject *>GetFreeMemoryError

class ResetMaxMemoryUsedError(BlazingError):
    """ResetMaxUsedMemoryError Error."""
cdef public PyObject * ResetMaxMemoryUsedError_ = <PyObject *>ResetMaxMemoryUsedError

class GetMaxMemoryUsedError(BlazingError):
    """GetMaxMemoryUsedError Error."""
cdef public PyObject * GetMaxMemoryUsedError_ = <PyObject *>GetMaxMemoryUsedError

class GetProductDetailsError(BlazingError):
    """GetProductDetails Error."""
cdef public PyObject * GetProductDetailsError_ = <PyObject *>GetProductDetailsError

class PerformPartitionError(BlazingError):
    """PerformPartitionError Error."""
cdef public PyObject * PerformPartitionError_ = <PyObject *>PerformPartitionError

class RunGenerateGraphError(BlazingError):
    """RunGenerateGraph Error."""
cdef public PyObject * RunGenerateGraphError_ = <PyObject *>RunGenerateGraphError

class RunExecuteGraphError(BlazingError):
    """RunExecuteGraph Error."""
cdef public PyObject * RunExecuteGraphError_ = <PyObject *>RunExecuteGraphError

class RunSkipDataError(BlazingError):
    """RunSkipData Error."""
cdef public PyObject *RunSkipDataError_ = <PyObject *>RunSkipDataError

class ParseSchemaError(BlazingError):
    """ParseSchema Error."""
cdef public PyObject * ParseSchemaError_ = <PyObject *>ParseSchemaError

class RegisterFileSystemHDFSError(BlazingError):
    """RegisterFileSystemHDFS Error."""
cdef public PyObject * RegisterFileSystemHDFSError_ = <PyObject *>RegisterFileSystemHDFSError

class RegisterFileSystemGCSError(BlazingError):
    """RegisterFileSystemGCS Error."""
cdef public PyObject * RegisterFileSystemGCSError_ = <PyObject *>RegisterFileSystemGCSError

class RegisterFileSystemS3Error(BlazingError):
    """RegisterFileSystemS3 Error."""
cdef public PyObject * RegisterFileSystemS3Error_ = <PyObject *>RegisterFileSystemS3Error

class RegisterFileSystemLocalError(BlazingError):
    """RegisterFileSystemLocal Error."""
cdef public PyObject * RegisterFileSystemLocalError_ = <PyObject *>RegisterFileSystemLocalError

class InferFolderPartitionMetadataError(BlazingError):
    """InferFolderPartitionMetadata Error."""
cdef public PyObject * InferFolderPartitionMetadataError_ = <PyObject *>InferFolderPartitionMetadataError

cdef cio.TableSchema parseSchemaPython(vector[string] files, string file_format_hint, vector[string] arg_keys, vector[string] arg_values,vector[pair[string,type_id]] extra_columns, bool ignore_missing_paths) nogil except *:
    with nogil:
        return cio.parseSchema(files, file_format_hint, arg_keys, arg_values, extra_columns, ignore_missing_paths)

cdef unique_ptr[cio.ResultSet] parseMetadataPython(vector[string] files, pair[int,int] offset, cio.TableSchema schema, string file_format_hint, vector[string] arg_keys, vector[string] arg_values) nogil except *:
    with nogil:
        return blaz_move( cio.parseMetadata(files, offset, schema, file_format_hint,arg_keys,arg_values) )

cdef shared_ptr[cio.graph] runGenerateGraphPython(uint32_t masterIndex,vector[string] worker_ids, vector[string] tableNames, vector[string] tableScans, vector[TableSchema] tableSchemas, vector[vector[string]] tableSchemaCppArgKeys, vector[vector[string]] tableSchemaCppArgValues, vector[vector[string]] filesAll, vector[int] fileTypes, int ctxToken, string query, vector[vector[map[string,string]]] uri_values_cpp, map[string,string] config_options, string sql, string current_timestamp) except *:
    return cio.runGenerateGraph(masterIndex, worker_ids, tableNames, tableScans, tableSchemas, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query, uri_values_cpp, config_options, sql, current_timestamp)

cdef string runGeneratePhysicalGraphPython(uint32_t masterIndex, vector[string] worker_ids, int ctxToken, string query) except *:
    return cio.runGeneratePhysicalGraph(masterIndex, worker_ids, ctxToken, query)

cdef unique_ptr[cio.PartitionedResultSet] startExecuteGraphPython(shared_ptr[cio.graph] graph, int ctx_token) except *:
    with nogil:
      cio.startExecuteGraph(graph,ctx_token)

cdef unique_ptr[cio.PartitionedResultSet] getExecuteGraphResultPython(shared_ptr[cio.graph] graph, int ctx_token) except *:
    with nogil:
      return blaz_move(cio.getExecuteGraphResult(graph,ctx_token))



#cdef unique_ptr[cio.ResultSet] performPartitionPython(int masterIndex, int ctxToken, BlazingTableView blazingTableView, vector[string] column_names) nogil except +:
#    with nogil:
#        return blaz_move(cio.performPartition(masterIndex,  ctxToken, blazingTableView, column_names))

cdef unique_ptr[cio.ResultSet] runSkipDataPython(BlazingTableView metadata, vector[string] all_column_names, string query) nogil except +:
    with nogil:
        return blaz_move(cio.runSkipData( metadata, all_column_names, query))

cdef cio.TableScanInfo getTableScanInfoPython(string logicalPlan) nogil:
    with nogil:
        temp = cio.getTableScanInfo(logicalPlan)
    return temp

cdef pair[pair[shared_ptr[cio.CacheMachine], shared_ptr[cio.CacheMachine] ], int] initializePython(uint16_t ralId, string worker_id, string network_iface_name,
    int ralCommunicationPort, vector[NodeMetaDataUCP] workers_ucp_info, bool singleNode, map[string,string] config_options,
    string allocation_mode, size_t initial_pool_size, size_t maximum_pool_size, bool enable_logging) nogil except +:
    with nogil:
        return cio.initialize( ralId, worker_id, network_iface_name, ralCommunicationPort, workers_ucp_info, singleNode, config_options, allocation_mode, initial_pool_size, maximum_pool_size, enable_logging)


cdef void finalizePython(vector[int] ctx_tokens) nogil except +:
    with nogil:
        cio.finalize(ctx_tokens)

cdef size_t getFreeMemoryPython() nogil except *:
    with nogil:
        return cio.getFreeMemory()

cdef size_t getMaxMemoryUsedPython() nogil except *:
    with nogil:
        return cio.getMaxMemoryUsed()

cdef void resetMaxMemoryUsedPython() nogil except *:
    with nogil:
        cio.resetMaxMemoryUsed(0)

cdef map[string, string] getProductDetailsPython() nogil except *:
    with nogil:
        return cio.getProductDetails()

cdef vector[cio.FolderPartitionMetadata] inferFolderPartitionMetadataPython(string folder_path) nogil except *:
    with nogil:
        return cio.inferFolderPartitionMetadata(folder_path)

cpdef pair[bool, string] registerFileSystemCaller(fs, root, authority):
    cdef HDFS hdfs
    cdef S3 s3
    cdef GCS gcs
    if fs['type'] == 'hdfs':
        hdfs.host = str.encode(fs['host'])
        hdfs.port = fs['port']
        hdfs.user = str.encode(fs['user'])
        driver = fs['driver']
        if 'libhdfs' == driver: # Increment the iterator to the net element
            hdfs.DriverType = 1
        elif 'libhdfs3' == driver:
            hdfs.DriverType = 2
        else:
            raise ValueError('Invalid hdfs driver: ' + driver)
        hdfs.kerberosTicket = str.encode(fs['kerberos_ticket'])
        return cio.registerFileSystemHDFS( hdfs, str.encode( root), str.encode(authority))
    if fs['type'] == 's3':
        s3.bucketName = str.encode(fs['bucket_name'])
        s3.encryptionType = fs['encryption_type']
        s3.kmsKeyAmazonResourceName = str.encode(fs['kms_key_amazon_resource_name'])
        s3.accessKeyId = str.encode(fs['access_key_id'])
        s3.secretKey = str.encode(fs['secret_key'])
        s3.sessionToken = str.encode(fs['session_token'])
        s3.endpointOverride = str.encode(fs['endpoint_override'])
        s3.region = str.encode(fs['region'])
        return cio.registerFileSystemS3( s3,  str.encode(root), str.encode(authority))
    if fs['type'] == 'gs':
        gcs.projectId = str.encode(fs['project_id'])
        gcs.bucketName = str.encode(fs['bucket_name'])
        gcs.useDefaultAdcJsonFile = fs['use_default_adc_json_file']
        gcs.adcJsonFile = str.encode(fs['adc_json_file'])
        return cio.registerFileSystemGCS( gcs,  str.encode(root), str.encode(authority))
    if fs['type'] == 'local':
        return cio.registerFileSystemLocal( str.encode( root), str.encode(authority))


cdef class PyBlazingCache:
    cdef shared_ptr[cio.CacheMachine] c_cache

    def add_to_cache_with_meta(self,cudf_data,metadata):
        cdef cio.MetadataDictionary c_metadata
        cdef map[string,string] metadata_map
        cdef string c_key
        for key in metadata.keys():
          if key != "worker_ids":
            c_key = key.encode()
            metadata_map[c_key] = metadata[key].encode()

        c_metadata.set_values(metadata_map)
        cdef vector[string] column_names
        for column_name in cudf_data:
           column_names.push_back(str.encode(column_name))
        cdef vector[column_view] column_views
        cdef Column cython_col
        for cython_col in cudf_data._data.values():
           column_views.push_back(cython_col.view())
        cdef unique_ptr[BlazingTable] blazing_table = make_unique[BlazingTable](table_view(column_views), column_names)
        deref(blazing_table).ensureOwnership()
        cdef unique_ptr[cio.GPUCacheData] ptr = make_unique[cio.GPUCacheData](move(blazing_table),c_metadata)
        cdef string msg_id = metadata["message_id"].encode()
        with nogil:
            deref(self.c_cache).addCacheData(blaz_move2(ptr),msg_id,1)


    def has_next_now(self,):
        return deref(self.c_cache).has_next_now()

    def add_to_cache(self,cudf_data):
        cdef vector[string] column_names
        for column_name in cudf_data:
            column_names.push_back(str.encode(column_name))

        cdef vector[column_view] column_views
        cdef Column cython_col
        for cython_col in cudf_data._data.values():
            column_views.push_back(cython_col.view())
        cdef unique_ptr[BlazingTable] blazing_table = make_unique[BlazingTable](table_view(column_views), column_names)
        deref(blazing_table).ensureOwnership()
        cdef string message_id
        with nogil:
            deref(self.c_cache).addToCache(blaz_move(blazing_table),message_id,1)

    def pull_from_cache(self):
        cdef unique_ptr[CacheData] cache_data
        with nogil:
            cache_data = blaz_move(deref(self.c_cache).pullCacheData())
        cdef MetadataDictionary metadata = deref(cache_data).getMetadata()
        cdef unique_ptr[BlazingTable] table = deref(cache_data).decache()

        metadata_temp = metadata.get_values()
        metadata_py = {}
        for key_val in metadata_temp:
            key = key_val.first.decode('utf-8')
            val = key_val.second.decode('utf-8')
            if(key == "worker_ids"):
                metadata_py[key] = val.split(",")
            else:
                metadata_py[key] = val


        decoded_names = []
        for i in range(deref(table).names().size()):
            decoded_names.append(deref(table).names()[i].decode('utf-8'))

        df = cudf.DataFrame._from_data(*data_from_unique_ptr(
            blaz_move(deref(table).releaseCudfTable()),
            column_names=decoded_names
        ))

        return df, metadata_py

cpdef initializeCaller(uint16_t ralId, string worker_id, string network_iface_name,  int ralCommunicationPort, vector[NodeMetaDataUCP] workers_ucp_info,
        bool singleNode, map[string,string] config_options, string allocation_mode, size_t initial_pool_size, size_t maximum_pool_size, bool enable_logging):
    init_output = initializePython( ralId, worker_id, network_iface_name,  ralCommunicationPort, workers_ucp_info, singleNode, config_options,
        allocation_mode, initial_pool_size, maximum_pool_size, enable_logging)
    caches = init_output.first
    port = init_output.second
    transport_out = PyBlazingCache()
    transport_out.c_cache = caches.first
    transport_in = PyBlazingCache()
    transport_in.c_cache = caches.second
    return (transport_out,transport_in, port)


cpdef finalizeCaller(ctx_tokens: [int]):
    cdef vector[int] tks
    for ctx_token in ctx_tokens:
        tks.push_back(ctx_token)
    finalizePython(tks)

cpdef getFreeMemoryCaller():
    return getFreeMemoryPython()

cpdef getMaxMemoryUsedCaller():
    return getMaxMemoryUsedPython()

cpdef resetMaxMemoryUsedCaller():
    resetMaxMemoryUsedPython()

cpdef getProductDetailsCaller():
    my_map = getProductDetailsPython()
    cdef map[string,string].iterator it = my_map.begin()
    new_map = OrderedDict()
    while(it != my_map.end()):
        key = dereference(it).first
        key = key.decode('utf-8')

        value = dereference(it).second
        value = value.decode('utf-8')

        new_map[key] = value

        postincrement(it)

    return new_map


cpdef parseSchemaCaller(fileList, file_format_hint, args, extra_columns, ignore_missing_paths):
    cdef vector[string] files
    cdef vector[string] arg_keys
    cdef vector[string] arg_values
    cdef underlying_type_t_compression c_compression

    if 'compression' in args:
        if args['compression'] is None:
            c_compression = <underlying_type_t_compression> compression_type.NONE
        else:
            c_compression = <underlying_type_t_compression> Compression[args['compression'].upper()]
        args.update(compression=c_compression)

    for file in fileList:
      files.push_back(str.encode(file))

    for key, value in args.items():
      arg_keys.push_back(str.encode(key))
      arg_values.push_back(str.encode(str(value)))

    cdef vector[pair[string,type_id]] extra_columns_cpp
    cdef pair[string,type_id] extra_column_cpp

    for extra_column in extra_columns:
        extra_column_cpp.first = extra_column[0].encode()
        extra_column_cpp.second = <type_id>(<underlying_type_t_type_id>(extra_column[1]))
        extra_columns_cpp.push_back(extra_column_cpp)

    tableSchema = parseSchemaPython(files,str.encode(file_format_hint),arg_keys,arg_values, extra_columns_cpp, ignore_missing_paths)

    return_object = {}
    return_object['datasource'] = files
    return_object['files'] = tableSchema.files
    return_object['file_type'] = tableSchema.data_type
    return_object['args'] = args
    return_object['types'] = []
    for type in tableSchema.types:
        return_object['types'].append(<underlying_type_t_type_id>(type))
    return_object['names'] = tableSchema.names
    return_object['calcite_to_file_indices']= tableSchema.calcite_to_file_indices
    return_object['has_header_csv']= tableSchema.has_header_csv

    return return_object


cpdef parseMetadataCaller(fileList, offset, schema, file_format_hint, args):
    cdef vector[string] files
    for file in fileList:
      files.push_back(str.encode(file))

    cdef vector[string] arg_keys
    cdef vector[string] arg_values
    cdef TableSchema cpp_schema
    cdef type_id tid

    for col in schema['names']:
        cpp_schema.names.push_back(col)

    for col_type in schema['types']:
        tid = <type_id>(<underlying_type_t_type_id>(col_type))
        cpp_schema.types.push_back(tid)

    for key, value in args.items():
      arg_keys.push_back(str.encode(key))
      arg_values.push_back(str.encode(str(value)))

    resultSet = blaz_move(parseMetadataPython(files, offset, cpp_schema, str.encode(file_format_hint), arg_keys,arg_values))

    names = dereference(resultSet).names
    decoded_names = []
    for i in range(names.size()): # Increment the iterator to the net element
        decoded_names.append(names[i].decode('utf-8'))

    df = cudf.DataFrame._from_data(*data_from_unique_ptr(
        blaz_move(dereference(resultSet).cudfTable),
        column_names=decoded_names
    ))

    return df

cpdef inferFolderPartitionMetadataCaller(folder_path):
    folderMetadataArr = inferFolderPartitionMetadataPython(folder_path.encode())

    return_array = []
    for metadata in folderMetadataArr:
        decoded_values = []
        for value in metadata.values:
            decoded_values.append(value.decode('utf-8'))

        return_array.append({
            'name': metadata.name.decode('utf-8'),
            'values': decoded_values,
            'data_type': <underlying_type_t_type_id>(metadata.data_type)
        })

    return return_array


cdef class PyBlazingGraph:
    cdef shared_ptr[cio.graph] ptr

    def get_kernel_output_cache(self,kernel_id, cache_id):
        cache = PyBlazingCache()
        cache.c_cache = deref(self.ptr).get_kernel_output_cache(int(kernel_id),str.encode(cache_id))
        return cache

    cpdef set_input_and_output_caches(self, PyBlazingCache input_cache, PyBlazingCache output_cache):
        deref(self.ptr).set_input_and_output_caches(input_cache.c_cache, output_cache.c_cache)

    cpdef query_is_complete(self):
        return deref(self.ptr).query_is_complete()

    cpdef get_progress(self):
        return deref(self.ptr).get_progress()

cpdef runGeneratePhysicalGraphCaller(uint32_t masterIndex, worker_ids, int ctxToken, queryPy):
    cdef string query
    query = str.encode(queryPy)

    cdef vector[string] worker_ids_c
    for worker_id in worker_ids:
        worker_ids_c.push_back(worker_id.encode())

    physicalPlan = runGeneratePhysicalGraphPython(masterIndex, worker_ids_c, ctxToken, query)
    return physicalPlan.decode('UTF-8')

cpdef runGenerateGraphCaller(uint32_t masterIndex, worker_ids, tables,  table_scans, vector[int] fileTypes, int ctxToken, queryPy, map[string,string] config_options, sql, current_timestamp):
    cdef string sql_c
    sql_c = sql.encode()
    cdef string query
    query = str.encode(queryPy)

    cdef vector[TableSchema] tableSchemaCpp
    cdef vector[vector[string]] tableSchemaCppArgKeys
    cdef vector[vector[string]] tableSchemaCppArgValues
    cdef vector[string] currentTableSchemaCppArgKeys
    cdef vector[string] currentTableSchemaCppArgValues
    cdef vector[string] tableNames
    cdef vector[string] tableScans
    cdef vector[type_id] types
    cdef vector[string] names
    cdef TableSchema currentTableSchemaCpp

    cdef vector[vector[string]] filesAll
    cdef vector[string] currentFilesAll
    cdef vector[BlazingTableView] blazingTableViews

    cdef vector[vector[map[string,string]]] uri_values_cpp_all
    cdef vector[map[string,string]] uri_values_cpp
    cdef map[string,string] cur_uri_values

    cdef vector[column_view] column_views
    cdef Column cython_col

    cdef PyBlazingGraph pyGraph = PyBlazingGraph()

    for tableIndex in range(len(tables)):
      uri_values_cpp.clear()
      for uri_value in tables[tableIndex].uri_values:
        cur_uri_values.clear()
        for column_tuple in uri_value:
          key = column_tuple[0]
          value = column_tuple[1]
          cur_uri_values[key.encode()] = value.encode()

        uri_values_cpp.push_back(cur_uri_values)

      uri_values_cpp_all.push_back(uri_values_cpp)

      tableNames.push_back(str.encode(tables[tableIndex].name))
      tableScans.push_back(str.encode(table_scans[tableIndex]))
      table = tables[tableIndex]
      currentFilesAll.resize(0)
      if table.files is not None:
        for file in table.files:
          currentFilesAll.push_back(file)
      filesAll.push_back(currentFilesAll)
      types.resize(0)
      names.resize(0)
      fileType = fileTypes[tableIndex]

      if len(table.file_column_names) == 0:
        for col_name in table.column_names:
          if type(col_name) == np.str:
              names.push_back(col_name.encode())
          else: # from file
              names.push_back(col_name)
      else:
        for col_name in table.file_column_names:
          if type(col_name) == np.str:
              names.push_back(col_name.encode())
          else: # from file
              names.push_back(col_name)

      for col_type in table.column_types:
        types.push_back(<type_id>(<underlying_type_t_type_id>(col_type)))

      if table.fileType in (4, 5): # if cudf DataFrame or dask.cudf DataFrame
        blazingTableViews.resize(0)
        for cython_table in table.input:
          column_views.resize(0)
          for cython_col in cython_table._data.columns:
            column_views.push_back(cython_col.view())
          blazingTableViews.push_back(BlazingTableView(table_view(column_views), names))
        currentTableSchemaCpp.blazingTableViews = blazingTableViews

      if table.fileType == 6: # if arrow Table
        currentTableSchemaCpp.arrow_table =  pyarrow_unwrap_table(table.arrow_table)

      currentTableSchemaCpp.names = names
      currentTableSchemaCpp.types = types

      currentTableSchemaCpp.datasource = table.datasource
      if table.calcite_to_file_indices is not None:
        currentTableSchemaCpp.calcite_to_file_indices = table.calcite_to_file_indices
      currentTableSchemaCpp.in_file = table.in_file
      currentTableSchemaCppArgKeys.resize(0)
      currentTableSchemaCppArgValues.resize(0)
      tableSchemaCppArgKeys.push_back(currentTableSchemaCppArgKeys)
      tableSchemaCppArgValues.push_back(currentTableSchemaCppArgValues)
      for key, value in table.args.items():
          tableSchemaCppArgKeys[tableIndex].push_back(str.encode(key))
          tableSchemaCppArgValues[tableIndex].push_back(str.encode(str(value)))

      if table.row_groups_ids is not None:
        currentTableSchemaCpp.row_groups_ids = table.row_groups_ids
      else:
        currentTableSchemaCpp.row_groups_ids = []

      tableSchemaCpp.push_back(currentTableSchemaCpp)

    cdef vector[string] worker_ids_c
    for worker_id in worker_ids:
        worker_ids_c.push_back(worker_id.encode())

    pyGraph.ptr = runGenerateGraphPython(masterIndex, worker_ids_c, tableNames, tableScans, tableSchemaCpp, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query, uri_values_cpp_all, config_options, sql_c, current_timestamp)
    return pyGraph

cpdef startExecuteGraphCaller(PyBlazingGraph graph, int ctx_token):

    cdef shared_ptr[cio.graph] ptr = graph.ptr
    startExecuteGraphPython(blaz_move(ptr),ctx_token)

cpdef getExecuteGraphResultCaller(PyBlazingGraph graph, int ctx_token, bool is_single_node):

    cdef shared_ptr[cio.graph] ptr = graph.ptr
    graph = None
    resultSet = blaz_move(getExecuteGraphResultPython(blaz_move(ptr),ctx_token))
    names = dereference(resultSet).names
    decoded_names = []
    for i in range(names.size()):
        decoded_names.append(names[i].decode('utf-8'))

    if is_single_node: # the engine returns a concatenated dataframe
        return cudf.DataFrame._from_data(*data_from_unique_ptr(
            blaz_move(dereference(resultSet).cudfTables[0]),
            column_names=decoded_names
        ))
    else: # the engine returns a vector of dataframes
        dfs = []
        for i in range(dereference(resultSet).cudfTables.size()):
            dfs.append(cudf.DataFrame._from_data(*data_from_unique_ptr(
                blaz_move(dereference(resultSet).cudfTables[i]),
                column_names=decoded_names
            )))
        return dfs

cpdef runSkipDataCaller(table, queryPy):
    cdef string query
    cdef BlazingTableView metadata
    cdef vector[string] all_column_names
    cdef vector[column_view] column_views
    cdef Column cython_col

    query = str.encode(queryPy)
    all_column_names.resize(0)

    for col_name in table.column_names:
      if type(col_name) == np.str:
        all_column_names.push_back(col_name.encode())
      else: # from file
        all_column_names.push_back(col_name)

    column_views.resize(0)
    metadata_col_names = [name.encode() for name in table.metadata._data.keys()]
    for cython_col in table.metadata._data.values():
      column_views.push_back(cython_col.view())
    metadata = BlazingTableView(table_view(column_views), metadata_col_names)

    resultSet = blaz_move(runSkipDataPython( metadata, all_column_names, query))

    return_object = {}
    return_object['skipdata_analysis_fail'] = dereference(resultSet).skipdata_analysis_fail
    if return_object['skipdata_analysis_fail']:
        return_object['metadata'] = cudf.DataFrame()
    else:
        names = dereference(resultSet).names
        decoded_names = []
        for i in range(names.size()):
            decoded_names.append(names[i].decode('utf-8'))

        df = cudf.DataFrame._from_data(*data_from_unique_ptr(
            blaz_move(dereference(resultSet).cudfTable),
            column_names=decoded_names
        ))
        return_object['metadata'] = df
    return return_object

cpdef getTableScanInfoCaller(logicalPlan):
    temp = getTableScanInfoPython(str.encode(logicalPlan))

    table_names = [name.decode('utf-8') for name in temp.table_names]
    table_scans = [step.decode('utf-8') for step in temp.relational_algebra_steps]
    return table_names, table_scans


cpdef np_to_cudf_types_int(dtype):
    return <underlying_type_t_type_id> ( np_to_cudf_types[dtype])

cpdef cudf_type_int_to_np_types(type_int):
    return cudf_to_np_types[<underlying_type_t_type_id> (type_int)]
