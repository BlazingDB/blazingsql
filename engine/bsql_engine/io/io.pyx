# io.pyx


# Copyright (c) 2018, NVIDIA CORPORATION.

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from cudf._lib.cudf cimport *
from cudf._lib.GDFError import GDFError
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.map cimport map
from libcpp.memory cimport unique_ptr
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
from cudf.utils.utils import calc_chunk_size, mask_dtype, mask_bitsize

from cudf.core.column.column import build_column
import rmm
import nvstrings
import nvcategory

from cudf._libxx.lib cimport *
from cudf._libxx.table cimport *
from cudf._lib.cudf cimport *
from cudf._lib.cudf import *

from cudf._libxx.column import cudf_to_np_types

from bsql_engine.io cimport cio
from bsql_engine.io.cio cimport *
from cpython.ref cimport PyObject
from cython.operator cimport dereference

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

class RunQueryError(BlazingError):
    """RunQuery Error."""
cdef public PyObject *RunQueryError_ = <PyObject *>RunQueryError

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

cdef cio.TableSchema parseSchemaPython(vector[string] files, string file_format_hint, vector[string] arg_keys, vector[string] arg_values,vector[pair[string,gdf_dtype]] extra_columns):
    temp = cio.parseSchema(files,file_format_hint,arg_keys,arg_values,extra_columns)
    return temp

cdef cio.TableSchema parseMetadataPython(vector[string] files, pair[int,int] offset, cio.TableSchema schema, string file_format_hint, vector[string] arg_keys, vector[string] arg_values,vector[pair[string,gdf_dtype]] extra_columns):
    temp = cio.parseMetadata(files, offset, schema, file_format_hint,arg_keys,arg_values,extra_columns)
    return temp

cdef unique_ptr[cio.ResultSet] runQueryPython(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, vector[string] tableNames, vector[TableSchema] tableSchemas, vector[vector[string]] tableSchemaCppArgKeys, vector[vector[string]] tableSchemaCppArgValues, vector[vector[string]] filesAll, vector[int] fileTypes, int ctxToken, string query, unsigned long accessToken,vector[vector[map[string,gdf_scalar]]] uri_values_cpp,vector[vector[map[string,string]]] string_values_cpp,vector[vector[map[string,bool]]] is_column_string) except *:
    return blaz_move(cio.runQuery( masterIndex, tcpMetadata, tableNames, tableSchemas, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query, accessToken,uri_values_cpp,string_values_cpp,is_column_string))

cdef unique_ptr[cio.ResultSet] runSkipDataPython(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, vector[string] tableNames, vector[TableSchema] tableSchemas, vector[vector[string]] tableSchemaCppArgKeys, vector[vector[string]] tableSchemaCppArgValues, vector[vector[string]] filesAll, vector[int] fileTypes, int ctxToken, string query, unsigned long accessToken,vector[vector[map[string,gdf_scalar]]] uri_values_cpp,vector[vector[map[string,string]]] string_values_cpp,vector[vector[map[string,bool]]] is_column_string) except *:
    return blaz_move(cio.runSkipData( masterIndex, tcpMetadata, tableNames, tableSchemas, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query, accessToken,uri_values_cpp,string_values_cpp,is_column_string))

cdef cio.TableScanInfo getTableScanInfoPython(string logicalPlan):
    temp = cio.getTableScanInfo(logicalPlan)
    return temp

cdef void initializePython(int ralId, int gpuId, string network_iface_name, string ralHost, int ralCommunicationPort, bool singleNode) except *:
    cio.initialize( ralId,  gpuId, network_iface_name,  ralHost,  ralCommunicationPort, singleNode)

cdef void finalizePython() except *:
    cio.finalize()

cpdef pair[bool, string] registerFileSystemCaller(fs, root, authority):
    cdef HDFS hdfs
    cdef S3 s3
    cdef GCS gcs
    if fs['type'] == 'hdfs':
        hdfs.host = str.encode(fs['host'])
        hdfs.port = fs['port']
        hdfs.user = str.encode(fs['user'])
        driver = fs['driver']
        if 'libhdfs' == driver:
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
        return cio.registerFileSystemS3( s3,  str.encode(root), str.encode(authority))
    if fs['type'] == 'gs':
        gcs.projectId = str.encode(fs['project_id'])
        gcs.bucketName = str.encode(fs['bucket_name'])
        gcs.useDefaultAdcJsonFile = fs['use_default_adc_json_file']
        gcs.adcJsonFile = str.encode(fs['adc_json_file'])
        return cio.registerFileSystemGCS( gcs,  str.encode(root), str.encode(authority))
    if fs['type'] == 'local':
        return cio.registerFileSystemLocal( str.encode( root), str.encode(authority))

cpdef initializeCaller(int ralId, int gpuId, string network_iface_name, string ralHost, int ralCommunicationPort, bool singleNode):
    initializePython( ralId,  gpuId, network_iface_name,  ralHost,  ralCommunicationPort, singleNode)

cpdef finalizeCaller():
    finalizePython()

cpdef parseSchemaCaller(fileList, file_format_hint, args, extra_columns):
    cdef vector[string] files
    for file in fileList:
      files.push_back(str.encode(file))

    cdef vector[string] arg_keys
    cdef vector[string] arg_values

    for key, value in args.items():
      arg_keys.push_back(str.encode(key))
      arg_values.push_back(str.encode(str(value)))

    cdef vector[pair[string,gdf_dtype]] extra_columns_cpp
    cdef pair[string,gdf_dtype] extra_column_cpp
    for extra_column in extra_columns:
        extra_column_cpp = (extra_column[0].encode(),gdf_dtype_from_value(None,extra_column[1]))
        extra_columns_cpp.push_back(extra_column_cpp)
    tableSchema = parseSchemaPython(files,str.encode(file_format_hint),arg_keys,arg_values, extra_columns_cpp)
    return_object = {}
    return_object['datasource'] = files
    return_object['files'] = tableSchema.files
    return_object['file_type'] = tableSchema.data_type
    return_object['args'] = args
    return_object['types'] = tableSchema.types
    return_object['names'] = tableSchema.names
    return_object['calcite_to_file_indices']= tableSchema.calcite_to_file_indices
    return_object['num_row_groups']= tableSchema.num_row_groups
    i = 0
    #for column in temp.columns:
      # column.col_name = return_object['names'][i]
      # return_object['columns'][return_object['names'][i].decode('utf-8')] = (gdf_column_to_column(column))
      # i = i + 1

	# temp is TableSchema and temp.column is array of cudf::column_view
    #for column in tableSchema.columns:
        #return_object['columns'][return_object['names'][i].decode('utf-8')] = column
    #    i = i + 1

    return return_object


cpdef parseMetadataCaller(fileList, offset, schema, file_format_hint, args, extra_columns):
    cdef vector[string] files
    for file in fileList:
      print('file', file)
      files.push_back(str.encode(file))

    cdef vector[string] arg_keys
    cdef vector[string] arg_values
    cdef TableSchema cpp_schema

    for col in schema['names']:
        #cpp_schema.columns.push_back(column_view_from_column(schema['columns'][col]._column))
        cpp_schema.names.push_back(col)

    for col_type in schema['types']:
        cpp_schema.types.push_back(col_type)

    for key, value in args.items():
      arg_keys.push_back(str.encode(key))
      arg_values.push_back(str.encode(str(value)))

    cdef vector[pair[string,gdf_dtype]] extra_columns_cpp
    cdef pair[string,gdf_dtype] extra_column_cpp
    for extra_column in extra_columns:
        extra_column_cpp = (extra_column[0].encode(),gdf_dtype_from_value(None,extra_column[1]))
        extra_columns_cpp.push_back(extra_column_cpp)
    temp = parseMetadataPython(files, offset, cpp_schema, str.encode(file_format_hint), arg_keys,arg_values, extra_columns_cpp)
    return_object = {}
    return_object['datasource'] = files
    # return_object['files'] = temp.files
    return_object['file_type'] = temp.data_type
    return_object['args'] = args
    return_object['types'] = temp.types
    return_object['names'] = temp.names
    # return_object['calcite_to_file_indices']= temp.calcite_to_file_indices
    # return_object['num_row_groups']= temp.num_row_groups

    # i = 0
    # for column in temp.columns:
    #   column.col_name = return_object['names'][i]
    #   return_object['columns'][return_object['names'][i].decode('utf-8')] = (gdf_column_to_column(column))
    #   i = i + 1
    # return return_object['columns']


    df = cudf.DataFrame()
    i = 0
    # for column in temp.columns:
    #   column.col_name =  <char *> malloc((strlen(temp.names[i].c_str()) + 1) * sizeof(char))
    #   strcpy(column.col_name, temp.names[i].c_str())
    #   df.add_column(temp.names[i].decode('utf-8'),gdf_column_to_column(column))
    #   i = i + 1
    return df

cpdef runQueryCaller(int masterIndex,  tcpMetadata,  tables,  vector[int] fileTypes, int ctxToken, queryPy, unsigned long accessToken):
    cdef string query
    query = str.encode(queryPy)
    cdef vector[NodeMetaDataTCP] tcpMetadataCpp
    cdef vector[TableSchema] tableSchemaCpp
    cdef vector[vector[string]] tableSchemaCppArgKeys
    cdef vector[vector[string]] tableSchemaCppArgValues
    cdef vector[string] currentTableSchemaCppArgKeys
    cdef vector[string] currentTableSchemaCppArgValues
    cdef vector[string] tableNames
    cdef vector[type_id] types
    cdef vector[string] names
    cdef TableSchema currentTableSchemaCpp
    cdef NodeMetaDataTCP currentMetadataCpp
    cdef vector[vector[string]] filesAll
    cdef vector[string] currentFilesAll

    cdef vector[vector[map[string,gdf_scalar]]] uri_values_cpp_all
    cdef vector[map[string,gdf_scalar]] uri_values_cpp
    cdef map[string,gdf_scalar] cur_uri_values


    cdef vector[vector[map[string,string]]] string_values_cpp_all
    cdef vector[map[string,string]] string_values_cpp
    cdef map[string,string] cur_string_values

    cdef vector[vector[map[string,bool]]] is_string_column_all
    cdef vector[map[string,bool]] is_string_column
    cdef map[string,bool] cur_is_string_column

    cdef gdf_scalar_ptr scalar_ptr
    cdef gdf_scalar scalar

    cdef vector[column_view] column_views
    cdef Column cython_col

    tableIndex = 0
    for tableName in tables:
      string_values_cpp.empty()
      uri_values_cpp.empty()
      for uri_value in tables[tableName].uri_values:
        cur_uri_values.clear()
        cur_string_values.clear()
        for column_tuple in uri_value:
          key = column_tuple[0]
          value = column_tuple[1]
          if type(value) == np.str:
            cur_is_string_column[key.encode()] = True
            cur_string_values[key.encode()] = value.encode()
          else:
            scalar_ptr = gdf_scalar_from_scalar(value)
            scalar = scalar_ptr[0]
            free(scalar_ptr)
            cur_is_string_column[key.encode()] = False
            cur_uri_values[key.encode()] = scalar
        uri_values_cpp.push_back(cur_uri_values)
        string_values_cpp.push_back(cur_string_values)
        is_string_column.push_back(cur_is_string_column)
      uri_values_cpp_all.push_back(uri_values_cpp)
      string_values_cpp_all.push_back(string_values_cpp)
      is_string_column_all.push_back(is_string_column)

      is_string_column.push_back(cur_is_string_column)
      tableNames.push_back(str.encode(tableName))
      table = tables[tableName]
      currentFilesAll.resize(0)
      if table.files is not None:
        for file in table.files:
          currentFilesAll.push_back(file)
      filesAll.push_back(currentFilesAll)
      types.resize(0)
      names.resize(0)
      fileType = fileTypes[tableIndex]
      # TODO: TableSchema will be refactorized
      
      for col_name in table.column_names:
        if table.fileType == 4: #if from gdf
            names.push_back(col_name.encode())
        else: # from file
            names.push_back(col_name)

      for col_type in table.column_types:
        types.push_back(col_type)

      #currentTableSchemaCpp.columns = columns

      # TODO: Remove 4 == DataType.CUDF. Now there is a cython conflict with pyarrow.DataType
      if table.fileType == 4:
          names = [str.encode(x) for x in table.input.dtypes.keys()]
          for cython_col in table.input._data.values():
              column_views.push_back(cython_col.view())
          currentTableSchemaCpp.blazingTableView = BlazingTableView(table_view(column_views), names)

      currentTableSchemaCpp.names = names
      currentTableSchemaCpp.types = types
      
      currentTableSchemaCpp.datasource = table.datasource
      if table.calcite_to_file_indices is not None:
        currentTableSchemaCpp.calcite_to_file_indices = table.calcite_to_file_indices
      if table.num_row_groups is not None:
        currentTableSchemaCpp.num_row_groups = table.num_row_groups
      currentTableSchemaCpp.in_file = table.in_file
      currentTableSchemaCppArgKeys.resize(0)
      currentTableSchemaCppArgValues.resize(0)
      tableSchemaCppArgKeys.push_back(currentTableSchemaCppArgKeys)
      tableSchemaCppArgValues.push_back(currentTableSchemaCppArgValues)
      for key, value in table.args.items():
          tableSchemaCppArgKeys[tableIndex].push_back(str.encode(key))
          tableSchemaCppArgValues[tableIndex].push_back(str.encode(str(value)))

      # if table.row_groups_ids is not None:
      #   currentTableSchemaCpp.row_groups_ids = table.row_groups_ids
      # else:
      #   currentTableSchemaCpp.row_groups_ids = []

      tableSchemaCpp.push_back(currentTableSchemaCpp);
      tableIndex = tableIndex + 1
    for currentMetadata in tcpMetadata:
        currentMetadataCpp.ip = currentMetadata['ip'].encode()
        #print(currentMetadata['communication_port'])
        currentMetadataCpp.communication_port = currentMetadata['communication_port']
        tcpMetadataCpp.push_back(currentMetadataCpp)

    temp = blaz_move(runQueryPython(masterIndex, tcpMetadataCpp, tableNames, tableSchemaCpp, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query,accessToken,uri_values_cpp_all,string_values_cpp_all,is_string_column_all))
    
    # TODO WSM. When we migrate to cudf 0.13 we will likely only need to do something like:
    # cudf.DataFrame(Table.from_unique_ptr(blaz_move(dereference(temp).cudfTable), names)._data)
    # and we dont have to call _rename_columns. We are doing that here only because the current from_ptr is not properly setting the column names
    names = dereference(temp).names
    decoded_names = []
    for i in range(names.size()):
        decoded_names.append(names[i].decode('utf-8'))
    # names = [name.decode('utf-8') for name in names]
    df = cudf.DataFrame(_Table.from_ptr(blaz_move(dereference(temp).cudfTable), decoded_names)._data)
    df._rename_columns(decoded_names)
    return df
    
cpdef runSkipDataCaller(int masterIndex,  tcpMetadata,  table_obj,  vector[int] fileTypes, int ctxToken, queryPy, unsigned long accessToken):
    cdef string query
    query = str.encode(queryPy)
    cdef vector[NodeMetaDataTCP] tcpMetadataCpp
    cdef vector[TableSchema] tableSchemaCpp
    cdef vector[vector[string]] tableSchemaCppArgKeys
    cdef vector[vector[string]] tableSchemaCppArgValues
    cdef vector[string] currentTableSchemaCppArgKeys
    cdef vector[string] currentTableSchemaCppArgValues
    cdef vector[string] tableNames
    cdef vector[gdf_column_ptr] columns
    cdef vector[string] names
    cdef TableSchema currentTableSchemaCpp
    cdef NodeMetaDataTCP currentMetadataCpp
    cdef vector[vector[string]] filesAll
    cdef vector[string] currentFilesAll

    cdef vector[vector[map[string,gdf_scalar]]] uri_values_cpp_all
    cdef vector[map[string,gdf_scalar]] uri_values_cpp
    cdef map[string,gdf_scalar] cur_uri_values


    cdef vector[vector[map[string,string]]] string_values_cpp_all
    cdef vector[map[string,string]] string_values_cpp
    cdef map[string,string] cur_string_values

    cdef vector[vector[map[string,bool]]] is_string_column_all
    cdef vector[map[string,bool]] is_string_column
    cdef map[string,bool] cur_is_string_column

    cdef gdf_scalar_ptr scalar_ptr
    cdef gdf_scalar scalar

    tableName, table = table_obj

    tableIndex = 0
    string_values_cpp.empty()
    uri_values_cpp.empty()
    for uri_value in table.uri_values:
      cur_uri_values.clear()
      cur_string_values.clear()
      for column_tuple in uri_value:
        key = column_tuple[0]
        value = column_tuple[1]
        if type(value) == np.str:
          cur_is_string_column[key.encode()] = True
          cur_string_values[key.encode()] = value.encode()
        else:
          scalar_ptr = gdf_scalar_from_scalar(value)
          scalar = scalar_ptr[0]
          free(scalar_ptr)
          cur_is_string_column[key.encode()] = False
          cur_uri_values[key.encode()] = scalar
      uri_values_cpp.push_back(cur_uri_values)
      string_values_cpp.push_back(cur_string_values)
      is_string_column.push_back(cur_is_string_column)
    uri_values_cpp_all.push_back(uri_values_cpp)
    string_values_cpp_all.push_back(string_values_cpp)
    is_string_column_all.push_back(is_string_column)

    is_string_column.push_back(cur_is_string_column)
    tableNames.push_back(str.encode(tableName))
    currentFilesAll.resize(0)
    if table.files is not None:
      for file in table.files:
        currentFilesAll.push_back(file)
    filesAll.push_back(currentFilesAll)
    columns.resize(0)
    names.resize(0)
    fileType = fileTypes[tableIndex]
    # TODO: TableSchema will be refactorized
    for col in table.input:
      names.push_back(col.encode())
      #columns.push_back(column_view_from_column(table.input[col]._column))
    #currentTableSchemaCpp.columns = columns
    currentTableSchemaCpp.names = names
    currentTableSchemaCpp.datasource = table.datasource
    if table.calcite_to_file_indices is not None:
      currentTableSchemaCpp.calcite_to_file_indices = table.calcite_to_file_indices
    if table.num_row_groups is not None:
      currentTableSchemaCpp.num_row_groups = table.num_row_groups
    currentTableSchemaCpp.in_file = table.in_file
    currentTableSchemaCppArgKeys.resize(0)
    currentTableSchemaCppArgValues.resize(0)
    tableSchemaCppArgKeys.push_back(currentTableSchemaCppArgKeys)
    tableSchemaCppArgValues.push_back(currentTableSchemaCppArgValues)
    for key, value in table.args.items():
        tableSchemaCppArgKeys[tableIndex].push_back(str.encode(key))
        tableSchemaCppArgValues[tableIndex].push_back(str.encode(str(value)))

    for col in table.metadata:
      currentTableSchemaCpp.metadata.push_back(column_view_from_column(table.metadata[col]._column))

    tableSchemaCpp.push_back(currentTableSchemaCpp);

    for currentMetadata in tcpMetadata:
        currentMetadataCpp.ip = currentMetadata['ip'].encode()
        print(currentMetadata['communication_port'])
        currentMetadataCpp.communication_port = currentMetadata['communication_port']
        tcpMetadataCpp.push_back(currentMetadataCpp)
    temp = blaz_move(runSkipDataPython(masterIndex, tcpMetadataCpp, tableNames, tableSchemaCpp, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query,accessToken,uri_values_cpp_all,string_values_cpp_all,is_string_column_all))

    df = cudf.DataFrame()
    i = 0
    # for column in temp.columns:
      # column.col_name =  <char *> malloc((strlen(temp.names[i].c_str()) + 1) * sizeof(char))
      # strcpy(column.col_name, temp.names[i].c_str())
      # df.add_column(temp.names[i].decode('utf-8'),gdf_column_to_column(column))
      # i = i + 1
    return df

cpdef getTableScanInfoCaller(logicalPlan,tables):
    temp = getTableScanInfoPython(str.encode(logicalPlan))
    #print(temp)
    new_tables = {}
    table_names = [name.decode('utf-8') for name in temp.table_names]

    relational_algebra = [step.decode('utf-8') for step in temp.relational_algebra_steps]
    relational_algebra_steps = {}
    for table_name, table_columns, scan_string in zip(table_names, temp.table_columns,relational_algebra ):

        new_table = tables[table_name]

        # TODO percy c.gonzales felipe
        if new_table.fileType == 6:
          if table_name in new_tables:
            #TODO: this is not yet implemented the function unionColumns needs to be NotImplemented
            #for this to work
            temp_table = new_tables[table_name].filterAndRemapColumns(table_columns)
            new_tables[table_name] = temp_table.unionColumns(new_tables[table_name])
          else:
            if len(table_columns) != 0:
              new_table = new_table.filterAndRemapColumns(table_columns)
            else:
              new_table = new_table.convertForQuery()
        if table_name in relational_algebra_steps:
          relational_algebra_steps[table_name]['table_scans'].append(scan_string)
          relational_algebra_steps[table_name]['table_columns'].append(table_columns)
        else:
          relational_algebra_steps[table_name] = {}
          relational_algebra_steps[table_name]['table_scans'] = [scan_string,]
          relational_algebra_steps[table_name]['table_columns'] = [table_columns,]

        new_table.column_names = tables[table_name].column_names
        new_tables[table_name] = new_table
        
    return new_tables, relational_algebra_steps
