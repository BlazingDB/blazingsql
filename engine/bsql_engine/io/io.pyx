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

import enum

import numpy as np
import pandas as pd
import pyarrow as pa

import cudf
from cudf.utils import cudautils
from cudf.utils.dtypes import is_categorical_dtype
from cudf.utils.utils import mask_dtype, mask_bitsize

from cudf.core.column.column import build_column
import rmm
from cudf._lib cimport *

from bsql_engine.io cimport cio
from bsql_engine.io.cio cimport *
from cpython.ref cimport PyObject
from cython.operator cimport dereference, postincrement

from cudf._lib.table cimport Table as CudfXxTable
from cudf._lib.types import np_to_cudf_types, cudf_to_np_types

from libcpp.utility cimport pair
import logging

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

class BlazingSetAllocatorError(BlazingError):
    """BlazingSetAllocator Error."""
cdef public PyObject * BlazingSetAllocatorError_ = <PyObject *>BlazingSetAllocatorError

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

cdef cio.TableSchema parseSchemaPython(vector[string] files, string file_format_hint, vector[string] arg_keys, vector[string] arg_values,vector[pair[string,type_id]] extra_columns, bool ignore_missing_paths):
    temp = cio.parseSchema(files,file_format_hint,arg_keys,arg_values,extra_columns, ignore_missing_paths)
    return temp

cdef unique_ptr[cio.ResultSet] parseMetadataPython(vector[string] files, pair[int,int] offset, cio.TableSchema schema, string file_format_hint, vector[string] arg_keys, vector[string] arg_values):
    return blaz_move( cio.parseMetadata(files, offset, schema, file_format_hint,arg_keys,arg_values) )

cdef shared_ptr[cio.graph] runGenerateGraphPython(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, vector[string] tableNames, vector[string] tableScans, vector[TableSchema] tableSchemas, vector[vector[string]] tableSchemaCppArgKeys, vector[vector[string]] tableSchemaCppArgValues, vector[vector[string]] filesAll, vector[int] fileTypes, int ctxToken, string query, unsigned long accessToken,vector[vector[map[string,string]]] uri_values_cpp, map[string,string] config_options) except *:
    return cio.runGenerateGraph(masterIndex, tcpMetadata, tableNames, tableScans, tableSchemas, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query, accessToken, uri_values_cpp, config_options)

cdef unique_ptr[cio.PartitionedResultSet] runExecuteGraphPython(shared_ptr[cio.graph] graph) except *:
    return blaz_move(cio.runExecuteGraph(graph))

cdef unique_ptr[cio.ResultSet] performPartitionPython(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, int ctxToken, BlazingTableView blazingTableView, vector[string] column_names) except *:
    return blaz_move(cio.performPartition(masterIndex, tcpMetadata, ctxToken, blazingTableView, column_names))

cdef unique_ptr[cio.ResultSet] runSkipDataPython(BlazingTableView metadata, vector[string] all_column_names, string query) except *:
    return blaz_move(cio.runSkipData( metadata, all_column_names, query))

cdef cio.TableScanInfo getTableScanInfoPython(string logicalPlan):
    temp = cio.getTableScanInfo(logicalPlan)
    return temp

cdef pair[shared_ptr[cio.CacheMachine], shared_ptr[cio.CacheMachine] ] initializePython(int ralId, int gpuId, string network_iface_name, string ralHost, int ralCommunicationPort, bool singleNode, map[string,string] config_options) except *:
    return cio.initialize( ralId,  gpuId, network_iface_name,  ralHost,  ralCommunicationPort, singleNode, config_options)

cdef void finalizePython() except *:
    cio.finalize()

cdef void blazingSetAllocatorPython(string allocation_mode, size_t initial_pool_size, map[string,string] config_options) except *:
    cio.blazingSetAllocator(allocation_mode, initial_pool_size, config_options)

cdef map[string, string] getProductDetailsPython() except *:
    return cio.getProductDetails()

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
        print("a")
        deref(blazing_table).ensureOwnership()
        deref(self.c_cache).addCacheData(blaz_move2(make_unique[cio.GPUCacheDataMetaData](move(blazing_table),c_metadata)),metadata["kernel_id"])

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
        deref(self.c_cache).addToCache(blaz_move(blazing_table),message_id)

    def pull_from_cache(self):
        cdef unique_ptr[CacheData] cache_data_generic = deref(self.c_cache).pullCacheData()
        cdef unique_ptr[GPUCacheDataMetaData] cache_data = cast_cache_data_to_gpu_with_meta(blaz_move(cache_data_generic))
        cdef pair[unique_ptr[BlazingTable], MetadataDictionary ] table_and_meta = deref(cache_data).decacheWithMetaData()
        table = blaz_move(table_and_meta.first)
        metadata = table_and_meta.second
        metadata_py = {}
        cdef map[string,string].iterator it = metadata.get_values().begin()
        while(it != metadata.get_values().end()):
            metadata_py[dereference(it).first] = dereference(it).second
            postincrement(it)
        decoded_names = []
        for i in range(deref(table).names().size()):
            decoded_names.append(deref(table).names()[i].decode('utf-8'))
        df = cudf.DataFrame(CudfXxTable.from_unique_ptr(blaz_move(deref(table).releaseCudfTable()), decoded_names)._data)
        df._rename_columns(decoded_names)
        return df, metadata_py

cpdef initializeCaller(int ralId, int gpuId, string network_iface_name, string ralHost, int ralCommunicationPort, bool singleNode, map[string,string] config_options):
    caches = initializePython( ralId,  gpuId, network_iface_name,  ralHost,  ralCommunicationPort, singleNode, config_options)
    transport_out = PyBlazingCache()
    transport_out.c_cache = caches.first
    transport_in = PyBlazingCache()
    transport_in.c_cache = caches.second
    return (transport_out,transport_in)


cpdef finalizeCaller():
    finalizePython()

cpdef blazingSetAllocatorCaller(string allocation_mode, size_t initial_pool_size, map[string,string] config_options):
    blazingSetAllocatorPython(allocation_mode, initial_pool_size, config_options)


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
    for file in fileList:
      files.push_back(str.encode(file))

    cdef vector[string] arg_keys
    cdef vector[string] arg_values

    for key, value in args.items():
      arg_keys.push_back(str.encode(key))
      arg_values.push_back(str.encode(str(value)))

    cdef vector[pair[string,type_id]] extra_columns_cpp
    cdef pair[string,type_id] extra_column_cpp

    for extra_column in extra_columns:
        extra_column_cpp = (extra_column[0].encode(),extra_column[1])
        extra_columns_cpp.push_back(extra_column_cpp)

    tableSchema = parseSchemaPython(files,str.encode(file_format_hint),arg_keys,arg_values, extra_columns_cpp, ignore_missing_paths)
    return_object = {}
    return_object['datasource'] = files
    return_object['files'] = tableSchema.files
    return_object['file_type'] = tableSchema.data_type
    return_object['args'] = args
    return_object['types'] = tableSchema.types
    return_object['names'] = tableSchema.names
    return_object['calcite_to_file_indices']= tableSchema.calcite_to_file_indices

    return return_object


cpdef parseMetadataCaller(fileList, offset, schema, file_format_hint, args):
    cdef vector[string] files
    for file in fileList:
      files.push_back(str.encode(file))

    cdef vector[string] arg_keys
    cdef vector[string] arg_values
    cdef TableSchema cpp_schema

    for col in schema['names']:
        cpp_schema.names.push_back(col)

    for col_type in schema['types']:
        cpp_schema.types.push_back(col_type)

    for key, value in args.items():
      arg_keys.push_back(str.encode(key))
      arg_values.push_back(str.encode(str(value)))

    resultSet = blaz_move(parseMetadataPython(files, offset, cpp_schema, str.encode(file_format_hint), arg_keys,arg_values))

    names = dereference(resultSet).names
    decoded_names = []
    for i in range(names.size()): # Increment the iterator to the net element
        decoded_names.append(names[i].decode('utf-8'))

    df = cudf.DataFrame(CudfXxTable.from_unique_ptr(blaz_move(dereference(resultSet).cudfTable), decoded_names)._data)
    df._rename_columns(decoded_names)
    return df

cpdef performPartitionCaller(int masterIndex, tcpMetadata, int ctxToken, input, by):
    cdef vector[NodeMetaDataTCP] tcpMetadataCpp
    cdef NodeMetaDataTCP currentMetadataCpp
    cdef vector[string] column_names

    cdef vector[string] names
    names.resize(0)
    column_names.resize(0)

    cdef vector[column_view] column_views
    cdef Column cython_col

    for column_name in by:
      column_names.push_back(str.encode(column_name))

    for column_name in input.columns.to_list():
      names.push_back(str.encode(column_name))

    for currentMetadata in tcpMetadata:
        currentMetadataCpp.ip = currentMetadata['ip'].encode()
        currentMetadataCpp.communication_port = currentMetadata['communication_port']
        tcpMetadataCpp.push_back(currentMetadataCpp)

    for cython_col in input._data.values():
        column_views.push_back(cython_col.view())

    resultSet = blaz_move(performPartitionPython(masterIndex, tcpMetadataCpp, ctxToken, BlazingTableView(table_view(column_views), names), column_names))
    names = dereference(resultSet).names
    decoded_names = []
    for i in range(names.size()):
        decoded_names.append(names[i].decode('utf-8'))

    df = cudf.DataFrame(CudfXxTable.from_unique_ptr(blaz_move(dereference(resultSet).cudfTable), decoded_names)._data)

    return df

cdef class PyBlazingGraph:
    cdef shared_ptr[cio.graph] ptr

cpdef runGenerateGraphCaller(int masterIndex,  tcpMetadata,  tables,  table_scans, vector[int] fileTypes, int ctxToken, queryPy, unsigned long accessToken, map[string,string] config_options):
    cdef string query
    query = str.encode(queryPy)
    cdef vector[NodeMetaDataTCP] tcpMetadataCpp
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
    cdef NodeMetaDataTCP currentMetadataCpp
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
        types.push_back(col_type)

      if table.fileType in (4, 5): # if cudf DataFrame or dask.cudf DataFrame
        blazingTableViews.resize(0)
        for cython_table in table.input:
          column_views.resize(0)
          for cython_col in cython_table._data.values():
            column_views.push_back(cython_col.view())
          blazingTableViews.push_back(BlazingTableView(table_view(column_views), names))
        currentTableSchemaCpp.blazingTableViews = blazingTableViews

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

    for currentMetadata in tcpMetadata:
        currentMetadataCpp.ip = currentMetadata['ip'].encode()
        currentMetadataCpp.communication_port = currentMetadata['communication_port']
        tcpMetadataCpp.push_back(currentMetadataCpp)

    pyGraph.ptr = runGenerateGraphPython(masterIndex, tcpMetadataCpp, tableNames, tableScans, tableSchemaCpp, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query,accessToken,uri_values_cpp_all, config_options)
    return pyGraph

cpdef runExecuteGraphCaller(PyBlazingGraph graph, bool is_single_node):

    resultSet = blaz_move(runExecuteGraphPython(graph.ptr))

    names = dereference(resultSet).names
    decoded_names = []
    for i in range(names.size()):
        decoded_names.append(names[i].decode('utf-8'))

    if is_single_node: # the engine returns a concatenated dataframe
        df = cudf.DataFrame(CudfXxTable.from_unique_ptr(blaz_move(dereference(resultSet).cudfTables[0]), decoded_names)._data)
        return df
    else: # the engine returns a vector of dataframes
        dfs = []
        for i in range(dereference(resultSet).cudfTables.size()):
            dfs.append(cudf.DataFrame(CudfXxTable.from_unique_ptr(blaz_move(dereference(resultSet).cudfTables[i]), decoded_names)._data))
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
      return return_object
    else:
      names = dereference(resultSet).names
      decoded_names = []
      for i in range(names.size()):
          decoded_names.append(names[i].decode('utf-8'))

      df = cudf.DataFrame(CudfXxTable.from_unique_ptr(blaz_move(dereference(resultSet).cudfTable), decoded_names)._data)
      return_object['metadata'] = df
      return return_object

cpdef getTableScanInfoCaller(logicalPlan,tables):
    temp = getTableScanInfoPython(str.encode(logicalPlan))

    table_names = [name.decode('utf-8') for name in temp.table_names]
    relational_algebra = [step.decode('utf-8') for step in temp.relational_algebra_steps]

    new_tables = []
    table_scans = []
    for table_name, table_columns, scan_string in zip(table_names, temp.table_columns,relational_algebra ):
      new_tables.append(tables[table_name])
      table_scans.append(scan_string)

    return new_tables, table_scans

# cpdef getTableScanInfoCaller(logicalPlan,tables):
#     temp = getTableScanInfoPython(str.encode(logicalPlan))
#     #print(temp)
#     new_tables = {}
#     table_names = [name.decode('utf-8') for name in temp.table_names]

#     relational_algebra = [step.decode('utf-8') for step in temp.relational_algebra_steps]
#     relational_algebra_steps = {}
#     for table_name, table_columns, scan_string in zip(table_names, temp.table_columns,relational_algebra ):

#         new_table = tables[table_name]

#         # TODO percy c.gonzales felipe
#         if new_table.fileType == 6:
#           if table_name in new_tables:
#             #TODO: this is not yet implemented the function unionColumns needs to be NotImplemented
#             #for this to work
#             temp_table = new_tables[table_name].filterAndRemapColumns(table_columns)
#             new_tables[table_name] = temp_table.unionColumns(new_tables[table_name])
#           else:
#             if len(table_columns) != 0:
#               new_table = new_table.filterAndRemapColumns(table_columns)
#             else:
#               new_table = new_table.convertForQuery()
#         if table_name in relational_algebra_steps:
#           relational_algebra_steps[table_name]['table_scans'].append(scan_string)
#           relational_algebra_steps[table_name]['table_columns'].append(table_columns)
#         else:
#           relational_algebra_steps[table_name] = {}
#           relational_algebra_steps[table_name]['table_scans'] = [scan_string,]
#           relational_algebra_steps[table_name]['table_columns'] = [table_columns,]

#         new_table.column_names = tables[table_name].column_names
#         new_tables[table_name] = new_table

#     return new_tables, relational_algebra_steps
