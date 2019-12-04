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
from libc.stdint cimport uintptr_t
from libc.stdlib cimport malloc, free
from libc.string cimport strcpy, strlen

import numpy as np
import pandas as pd
import pyarrow as pa

import cudf
from cudf.utils import cudautils
from cudf.utils.dtypes import is_categorical_dtype
from cudf.utils.utils import calc_chunk_size, mask_dtype, mask_bitsize
from cudf.core.buffer import Buffer
from cudf.core.column.column import build_column
import rmm
import nvstrings
import nvcategory

from cudf._lib.cudf cimport *
from cudf._lib.cudf import *


from bsql_engine.io cimport cio
from bsql_engine.io.cio cimport *
from cpython.ref cimport PyObject

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

cdef cio.ResultSet runQueryPython(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, vector[string] tableNames, vector[TableSchema] tableSchemas, vector[vector[string]] tableSchemaCppArgKeys, vector[vector[string]] tableSchemaCppArgValues, vector[vector[string]] filesAll, vector[int] fileTypes, int ctxToken, string query, unsigned long accessToken,vector[vector[map[string,gdf_scalar]]] uri_values_cpp,vector[vector[map[string,string]]] string_values_cpp,vector[vector[map[string,bool]]] is_column_string) except *:
    temp = cio.runQuery( masterIndex, tcpMetadata, tableNames, tableSchemas, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query, accessToken,uri_values_cpp,string_values_cpp,is_column_string)
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
        hdfs.DriverType = 1
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
    temp = parseSchemaPython(files,str.encode(file_format_hint),arg_keys,arg_values, extra_columns_cpp)
    return_object = {}
    return_object['files'] = files
    return_object['file_type'] = temp.data_type
    return_object['args'] = args
    return_object['columns'] = cudf.DataFrame()
    return_object['names'] = temp.names
    return_object['calcite_to_file_indices']= temp.calcite_to_file_indices
    return_object['num_row_groups']= temp.num_row_groups
    i = 0
    for column in temp.columns:
      column.col_name = return_object['names'][i]
      return_object['columns'][return_object['names'][i].decode('utf-8')] = (gdf_column_to_column(column))
      i = i + 1
    return return_object

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
      columns.resize(0)
      names.resize(0)
      fileType = fileTypes[tableIndex]
      for col in table.input:
        names.push_back(col.encode())
        columns.push_back(column_view_from_column(table.input[col]._column))
      currentTableSchemaCpp.columns = columns
      currentTableSchemaCpp.names = names
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

      tableSchemaCpp.push_back(currentTableSchemaCpp);
      tableIndex = tableIndex + 1
    for currentMetadata in tcpMetadata:
        currentMetadataCpp.ip = currentMetadata['ip'].encode()
        print(currentMetadata['communication_port'])
        currentMetadataCpp.communication_port = currentMetadata['communication_port']
        tcpMetadataCpp.push_back(currentMetadataCpp)

    temp = runQueryPython(masterIndex, tcpMetadataCpp, tableNames, tableSchemaCpp, tableSchemaCppArgKeys, tableSchemaCppArgValues, filesAll, fileTypes, ctxToken, query,accessToken,uri_values_cpp_all,string_values_cpp_all,is_string_column_all)
    
    df = cudf.DataFrame()
    i = 0
    for column in temp.columns:
      column.col_name =  <char *> malloc((strlen(temp.names[i].c_str()) + 1) * sizeof(char))
      strcpy(column.col_name, temp.names[i].c_str())
      df.add_column(temp.names[i].decode('utf-8'),gdf_column_to_column(column))
      i = i + 1
    return df
