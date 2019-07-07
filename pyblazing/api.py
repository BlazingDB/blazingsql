import cudf as gd

import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from blazingdb.protocol.calcite.errors import SyntaxError
from blazingdb.messages.blazingdb.protocol.Status import Status

from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType
from blazingdb.protocol.gdf import gdf_columnSchema

from librmm_cffi import librmm as rmm

import pyarrow as pa
from cudf.bindings.cudf_cpp import *

from cudf.dataframe.string import StringColumn
from cudf import DataFrame
from cudf.dataframe.buffer import Buffer
from cudf.dataframe.columnops import build_column
from cudf.utils.utils import calc_chunk_size, mask_dtype, mask_bitsize

import numpy as np
import pandas as pd

import time
import nvstrings
from collections import OrderedDict

# NDarray device helper
from numba import cuda
from numba.cuda.cudadrv import driver, devices
require_context = devices.require_context
current_context = devices.get_context
gpus = devices.gpus

#Todo Rommel Percy : avoid using global variables, i.e. move these properties with the free function _to_table_group
dataColumnTokens = {}
validColumnTokens = {}

# connection_path is a ip/host when tcp and can be unix socket when ipc
def _send_request(connection_path, connection_port, requestBuffer):
    connection = blazingdb.protocol.UnixSocketConnection(connection_path)
    client = blazingdb.protocol.Client(connection)
    return client.send(requestBuffer)


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class PyConnector(metaclass=Singleton):
    
    def __init__(self, orchestrator_path, orchestrator_port):
        self._orchestrator_path = orchestrator_path
        self._orchestrator_port = orchestrator_port
        self._accessToken = None

    def __del__(self):
        self.close_connection()

    def connect(self):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("open connection")
        if self._accessToken is not None:
            print("Already connected to the Orchestrator")
            return

        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthOpen, authSchema)

        responseBuffer = _send_request(self._orchestrator_path, self._orchestrator_port, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            print("Connection to the Orchestrator could not be started")
            raise RuntimeError(errorResponse.errors)
        responsePayload = blazingdb.protocol.orchestrator.AuthResponseSchema.From(
            response.payload)
        
        print('connection established')
        self._accessToken = responsePayload.accessToken

    def close_connection(self):   
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("close connection")
        if self._accessToken is None:
            print("Already disconnected")
            return

        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            OrchestratorMessageType.AuthClose, self._accessToken, authSchema)

        responseBuffer = _send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            print("Can't close connection, probably it was lost")
            raise RuntimeError(errorResponse.errors.decode('utf-8'))

        print('Successfully disconnected')
        self._accessToken = None

    def is_connected(self):
        return self._accessToken is not None

    def run_ddl_create_table(self, 
                                 tableName,
                                 columnNames,
                                 columnTypes,
                                 dbName,
                                 schemaType,
                                 blazing_table,
                                 files,
                                 csvDelimiter,
                                 csvLineTerminator,
                                 csvSkipRows,
                                 csvHeader):

        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDDLCreateTableRequestSchema(name=tableName,
                                                                                       columnNames=columnNames,
                                                                                       columnTypes=columnTypes,
                                                                                       dbName=dbName,
                                                                                       schemaType=schemaType,
                                                                                       gdf=blazing_table,
                                                                                       files=files,
                                                                                       csvDelimiter=csvDelimiter,
                                                                                       csvLineTerminator=csvLineTerminator,
                                                                                       csvSkipRows=csvSkipRows,
                                                                                       csvHeader=csvHeader)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_CREATE_TABLE,
                                                                               self._accessToken, dmlRequestSchema)

        responseBuffer = _send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise RuntimeError(errorResponse.errors)

        return response.status

    def run_dml_query_token(self, query, tableGroup):
        dmlRequestSchema = blazingdb.protocol.io.BuildFileSystemDMLRequestSchema(query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML_FS,
                                                                               self._accessToken, dmlRequestSchema)
        responseBuffer = _send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            if b'SqlSyntaxException' in errorResponse.errors:
                raise SyntaxError(errorResponse.errors.decode('utf-8'))
            elif b'SqlValidationException' in errorResponse.errors:
                raise ValueError(errorResponse.errors.decode('utf-8'))
            raise RuntimeError(errorResponse.errors.decode('utf-8'))
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path.decode('utf8'), dmlResponseDTO.nodeConnection.port, dmlResponseDTO.calciteTime

    def run_ddl_drop_table(self, tableName, dbName):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print('drop table: ' + tableName)

        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLDropTableRequestSchema(
            name=tableName, dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_DROP_TABLE,
                                                                               self._accessToken, dmlRequestSchema)
        responseBuffer = _send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise RuntimeError(errorResponse.errors.decode('utf-8'))

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

        return response.status

    def free_memory(self, interpreter_path, interpreter_port):
        result_token = 2433423
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.FreeMemory, self._accessToken, getResultRequest)

        responseBuffer = _send_request(
            interpreter_path, interpreter_port, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        # TODO find a way to print only for debug mode (add verbose arg)
        #print('free result OK!')

    def free_result(self, result_token, interpreter_path, interpreter_port):
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.FreeResult, self._accessToken, getResultRequest)

        responseBuffer = _send_request(
            interpreter_path, interpreter_port, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        # TODO find a way to print only for debug mode (add verbose arg)
        #print('free result OK!')

    def _get_result(self, result_token, interpreter_path, interpreter_port):
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.GetResult, self._accessToken, getResultRequest)

        responseBuffer = _send_request(
            interpreter_path, interpreter_port, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        queryResult = blazingdb.protocol.interpreter.GetQueryResultFrom(
            response.payload)

        if queryResult.metadata.status.decode() == "Error":
            raise RuntimeError(queryResult.metadata.message.decode('utf-8'))

        return queryResult

def _get_client():
    __orchestrator_ip = '/tmp/orchestrator.socket'
    __orchestrator_port = 8890
    client = PyConnector(__orchestrator_ip, __orchestrator_port)

    if not client.is_connected():
        client.connect()

    return client


class ResultSetHandle:

    def __init__(self, columns, columnTokens, resultToken, interpreter_path, interpreter_port, handle, client, calciteTime, ralTime, totalTime, error_message):
        self.columns = columns
        self.columnTokens = columnTokens

        self._buffer_ids = []
        if columns is not None:
            if columns.columns.size > 0:
                for idx, column in enumerate(self.columns.columns):
                    dataframe_column = self.columns._cols[column]
                    data_id = id(dataframe_column._column._data)
                    dataColumnTokens[data_id] = columnTokens[idx]
                    self._buffer_ids.append(data_id)
                    if dataframe_column.null_count > 0:
                        nulmask_id = id(dataframe_column._column._mask)
                        validColumnTokens[nulmask_id] = columnTokens[idx]
                        self._buffer_ids.append(nulmask_id)
            else:
                self.columns.resultToken = resultToken

        self.resultToken = resultToken
        self.interpreter_path = interpreter_path
        self.interpreter_port = interpreter_port
        self.handle = handle
        self.client = client
        self.calciteTime = calciteTime
        self.ralTime = ralTime
        self.totalTime = totalTime
        self.error_message = error_message

    def __del__(self):
        for key in self._buffer_ids:
            dataColumnTokens.pop(key, None)
            validColumnTokens.pop(key, None)
        
        if self.handle is not None:
            for ipch in self.handle: #todo add NVStrings handles
                ipch.close()
            del self.handle
            self.client.free_result(self.resultToken,self.interpreter_path,self.interpreter_port)

    def __str__(self):
        return ('''columns = %(columns)s
                    resultToken = %(resultToken)s
                    interpreter_path = %(interpreter_path)s
                    interpreter_port = %(interpreter_port)s
                    handle = %(handle)s
                    client = %(client)s
                    calciteTime = %(calciteTime)d
                    ralTime = %(ralTime)d
                    totalTime = %(totalTime)d
                    error_message = %(error_message)s''' % {
                            'columns': self.columns,
                            'resultToken': self.resultToken,
                            'interpreter_path': self.interpreter_path,
                            'interpreter_port': self.interpreter_port,
                            'handle': self.handle,
                            'client': self.client,
                            'calciteTime' : self.calciteTime if self.calciteTime is not None else 0,
                            'ralTime' : self.ralTime if self.ralTime is not None else 0,
                            'totalTime' : self.totalTime if self.totalTime is not None else 0,
                            'error_message' : self.error_message,
                        })

    def __repr__(self):
        return str(self)


def get_ipc_handle_for_data(dataframe_column):

    if id(dataframe_column._column._data) in dataColumnTokens:
        return None
    else:
        if get_np_dtype_to_gdf_dtype(dataframe_column.dtype) == gdf_dtype.GDF_STRING:
            return None
        else:
            ipch = dataframe_column._column._data.mem.get_ipc_handle()
            return bytes(ipch._ipc_handle.handle)

def get_ipc_handle_for_valid(dataframe_column):

    if id(dataframe_column._column._mask) in validColumnTokens:
        return None
    elif dataframe_column.null_count > 0:
        ipch = dataframe_column._column._mask.mem.get_ipc_handle()
        return bytes(ipch._ipc_handle.handle)
    else:
        return None

def get_ipc_handle_for_strings(dataframe_column):

    if id(dataframe_column._column._data) in dataColumnTokens:
        return None
    elif get_np_dtype_to_gdf_dtype(dataframe_column.dtype) == gdf_dtype.GDF_STRING:
        return dataframe_column._column._data.get_ipc_data()       
    else:
        return None

class gdf_dtype(object):
    GDF_invalid = 0
    GDF_INT8 = 1
    GDF_INT16 = 2
    GDF_INT32 = 3
    GDF_INT64 = 4
    GDF_FLOAT32 = 5
    GDF_FLOAT64 = 6
    GDF_BOOL8 = 7
    GDF_DATE32 = 8
    GDF_DATE64 = 9
    GDF_TIMESTAMP = 10
    GDF_CATEGORY = 11
    GDF_STRING = 12
    GDF_STRING_CATEGORY = 13
    N_GDF_TYPES = 14

def get_np_dtype_to_gdf_dtype_str(dtype):
 
    dtypes = {
        np.dtype('float64'):    'GDF_FLOAT64',
        np.dtype('float32'):    'GDF_FLOAT32',
        np.dtype('int64'):      'GDF_INT64',
        np.dtype('int32'):      'GDF_INT32',
        np.dtype('int16'):      'GDF_INT16',
        np.dtype('int8'):       'GDF_INT8',
        np.dtype('bool_'):      'GDF_BOOL8',
        np.dtype('datetime64[ms]'): 'GDF_DATE64',
        np.dtype('datetime64'): 'GDF_DATE64',        
        np.dtype('object_'):    'GDF_STRING',
        np.dtype('str_'):       'GDF_STRING',
        np.dtype('<M8[ms]'):    'GDF_DATE64',        
    }
    return dtypes[dtype]

def gdf_dtypes_to_gdf_dtype_strs(dtypes):
    values = []
    def gdf_type(type_name):
        dicc = {
            gdf_dtype.GDF_invalid : 'GDF_invalid',
            gdf_dtype.GDF_INT8 : 'GDF_INT8',
            gdf_dtype.GDF_INT16 : 'GDF_INT16',
            gdf_dtype.GDF_INT32 : 'GDF_INT32',
            gdf_dtype.GDF_INT64 : 'GDF_INT64',
            gdf_dtype.GDF_FLOAT32 : 'GDF_FLOAT32',
            gdf_dtype.GDF_FLOAT64 : 'GDF_FLOAT64',
            gdf_dtype.GDF_BOOL8 : 'GDF_BOOL8',
            gdf_dtype.GDF_DATE32 : 'GDF_DATE32',
            gdf_dtype.GDF_DATE64 : 'GDF_DATE64',
            gdf_dtype.GDF_TIMESTAMP : 'GDF_TIMESTAMP',
            gdf_dtype.GDF_CATEGORY : 'GDF_CATEGORY',
            gdf_dtype.GDF_STRING : 'GDF_STRING',
            gdf_dtype.GDF_STRING_CATEGORY : 'GDF_STRING_CATEGORY',
            gdf_dtype.N_GDF_TYPES : 'N_GDF_TYPES'
        }
        if dicc.get(type_name):
            return dicc[type_name]
        return ''

    for key in dtypes:
        values.append(gdf_type(key))

    return values

def get_np_dtype_to_gdf_dtype(dtype):
 
    dtypes = {
        np.dtype('float64'):    gdf_dtype.GDF_FLOAT64,
        np.dtype('float32'):    gdf_dtype.GDF_FLOAT32,
        np.dtype('int64'):      gdf_dtype.GDF_INT64,
        np.dtype('int32'):      gdf_dtype.GDF_INT32,
        np.dtype('int16'):      gdf_dtype.GDF_INT16,
        np.dtype('int8'):       gdf_dtype.GDF_INT8,
        np.dtype('bool_'):      gdf_dtype.GDF_BOOL8,
        np.dtype('datetime64[ms]'): gdf_dtype.GDF_DATE64,
        np.dtype('datetime64'): gdf_dtype.GDF_DATE64,        
        np.dtype('object_'):    gdf_dtype.GDF_STRING,
        np.dtype('str_'):       gdf_dtype.GDF_STRING,
        np.dtype('<M8[ms]'):    gdf_dtype.GDF_DATE64,        
    }
    return dtypes[dtype]

def get_dtype_values(dtypes):
    values = []
    def gdf_type(type_name):
        dicc = {
            'str': gdf_dtype.GDF_STRING,
            'date': gdf_dtype.GDF_DATE64,
            'date64': gdf_dtype.GDF_DATE64,
            'date32': gdf_dtype.GDF_DATE32,
            'timestamp': gdf_dtype.GDF_TIMESTAMP,
            'category': gdf_dtype.GDF_CATEGORY,
            'float': gdf_dtype.GDF_FLOAT32,
            'double': gdf_dtype.GDF_FLOAT64,
            'float32': gdf_dtype.GDF_FLOAT32,
            'float64': gdf_dtype.GDF_FLOAT64,
            'short': gdf_dtype.GDF_INT16,
            'long': gdf_dtype.GDF_INT64,
            'int': gdf_dtype.GDF_INT32,
            'int32': gdf_dtype.GDF_INT32,
            'int64': gdf_dtype.GDF_INT64,
        }
        if dicc.get(type_name):
            return dicc[type_name]
        return gdf_dtype.GDF_INT64

    for key in dtypes:
        values.append(gdf_type(key))

    return values



#  converts to data structure used for sending via blazingdb-protocol
def gdf_to_BlazingTable(gdf):
    blazing_table = {}
    blazing_columns = []
    columnTokens = []

    for column in gdf.columns:
        dataframe_column = gdf._cols[column]
       
        data_sz = len(dataframe_column)
        dtype = get_np_dtype_to_gdf_dtype(dataframe_column.dtype)
        null_count = dataframe_column.null_count

        try:
            data_ipch = get_ipc_handle_for_data(dataframe_column)
        except:
            print("ERROR: when getting the IPC handle for data")

        try:
            valid_ipch = get_ipc_handle_for_valid(dataframe_column)
        except:
            print("ERROR: when getting the IPC handle for valid")

        try:
            ipc_data = get_ipc_handle_for_strings(dataframe_column)
        except:
            print("ERROR: when getting the IPC handle for strings")
        
        dtype_info = {
            'time_unit': 0, #TODO dummy value
        }

        blazing_column = {
            'data': data_ipch,
            'valid': valid_ipch,
            'size': data_sz,
            'dtype': dtype,
            'null_count': null_count,
            'dtype_info': dtype_info
        }

        if ipc_data is not None:
            dtype = gdf_dtype.GDF_STRING # TODO: open issue, it must be a GDF_STRING

            blazing_column['dtype'] = dtype
            #custrings_data
            blazing_column['custrings_data'] = ipc_data

        column_data_id = id(dataframe_column._column._data)
        if column_data_id in dataColumnTokens:
            columnTokens.append(dataColumnTokens[column_data_id])
        else:
            columnTokens.append(0)

        blazing_columns.append(blazing_column)

    blazing_table['columns'] = blazing_columns
    if hasattr(gdf, 'resultToken'):
        blazing_table['resultToken'] = gdf.resultToken
    else:
        blazing_table['resultToken'] = 0

    blazing_table['columnTokens'] = columnTokens
    return blazing_table

def make_empty_BlazingTable():
    empty_dtype_info = {'time_unit': 0}
    # empty_gdf_column_handler = {'data':[], 'valid':[], 'size':0, 'dtype':0, 'dtype_info':empty_dtype_info, 'null_count':0, 'custrings_data': []}
    empty_gdf_column_handler = {'data':None, 'valid':None, 'size':0, 'dtype':0, 'dtype_info':None, 'null_count':0, 'custrings_data': None}
    blazing_table = {'columns': [empty_gdf_column_handler], 'columnTokens': [], 'resultToken': 0}
    return blazing_table


from librmm_cffi import librmm as rmm

def _open_ipc_array(handle, shape, dtype, strides=None, offset=0):
    dtype = np.dtype(dtype)
    # compute size
    size = np.prod(shape) * dtype.itemsize
    # manually recreate the IPC mem handle
    handle = driver.drvapi.cu_ipc_mem_handle(*handle)
    # use *IpcHandle* to open the IPC memory
    ipchandle = driver.IpcHandle(None, handle, size, offset=offset)
    return ipchandle, ipchandle.open_array(current_context(), shape=shape,
                                           strides=strides, dtype=dtype)

def _private_get_result(resultToken, interpreter_path, interpreter_port, calciteTime):
    client = _get_client()

    #print(interpreter_path)
    #print(interpreter_port)
    resultSet = client._get_result(resultToken, interpreter_path, interpreter_port)

    gdf_columns = []
    ipchandles = []
    for i, c in enumerate(resultSet.columns):

        # todo: remove this if when C gdf struct is replaced by pyarrow object
        # this workaround is only for the python object. The RAL knows the column_token and will know what its dtype actually is
        if c.dtype == gdf_dtype.GDF_DATE32:
            c.dtype = gdf_dtype.GDF_INT32

        if c.dtype == gdf_dtype.GDF_DATE64:
            np_dtype = np.dtype('datetime64[ms]')
        else:
            np_dtype = gdf_to_np_dtype(c.dtype)

        if c.size != 0 :
            if c.dtype == gdf_dtype.GDF_STRING:
                new_strs = nvstrings.create_from_ipc(c.custrings_data)
                newcol = StringColumn(new_strs)

                gdf_columns.append(newcol.view(StringColumn, dtype='object'))
            else:
                if c.dtype == gdf_dtype.GDF_STRING_CATEGORY:
                    print("ERROR _private_get_result received a GDF_STRING_CATEGORY")
                    
                assert len(c.data) == 64,"Data ipc handle was not 64 bytes"
                                
                ipch_data, data_ptr = _open_ipc_array(
                        c.data, shape=c.size, dtype=np_dtype)
                ipchandles.append(ipch_data)

                valid_ptr = None
                if (c.null_count > 0):
                    assert len(c.valid) == 64,"Valid ipc handle was not 64 bytes"
                    ipch_valid, valid_ptr = _open_ipc_array(
                        c.valid, shape=calc_chunk_size(c.size, mask_bitsize), dtype=np.int8)
                    ipchandles.append(ipch_valid)

                if (valid_ptr is None):
                    gdf_columns.append(build_column(Buffer(data_ptr), np_dtype))
                else:
                    gdf_columns.append(build_column(Buffer(data_ptr), np_dtype, Buffer(valid_ptr)))
                
        else:
            if c.dtype == gdf_dtype.GDF_STRING:
                gdf_columns.append(StringColumn(nvstrings.to_device([])))
            else:
                if c.dtype == gdf_dtype.GDF_DATE32:
                    c.dtype = gdf_dtype.GDF_INT32
                    
                gdf_columns.append(build_column(Buffer.null(np_dtype), np_dtype))

    gdf = DataFrame()
    for k, v in zip(resultSet.columnNames, gdf_columns):
        assert k != "", "Column name was an empty string"
        gdf[k.decode("utf-8")] = v

    resultSet.columns = gdf
    return resultSet, ipchandles

def run_query_get_token(sql):
    return _run_query_get_token(sql)

def _run_query_get_token(sql):
    startTime = time.time()

    resultToken = 0
    interpreter_path = None
    interpreter_port = None
    calciteTime = 0
    error_message = ''

    try:
        client = _get_client()

        tableGroup = _create_dummy_table_group()

        resultToken, interpreter_path, interpreter_port, calciteTime = client.run_dml_query_token(sql, tableGroup)
    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception:
        error_message = "Unexpected error on " + _run_query_get_token.__name__ + ", " + str(error)

    if error_message is not '':
        print(error_message)

    metaToken = {"client" : client, "resultToken" : resultToken, "interpreter_path" : interpreter_path, "interpreter_port" : interpreter_port, "startTime" : startTime, "calciteTime" : calciteTime}
    return metaToken

def run_query_get_results(metaToken):
    return _run_query_get_results(metaToken)

def _run_query_get_results(metaToken):
    error_message = ''

    try:
        resultSet, ipchandles = _private_get_result(metaToken["resultToken"], metaToken["interpreter_path"], metaToken["interpreter_port"], metaToken["calciteTime"])

        totalTime = (time.time() - metaToken["startTime"]) * 1000  # in milliseconds
        return_result = ResultSetHandle(resultSet.columns, resultSet.columnTokens, metaToken["resultToken"], metaToken["interpreter_path"], metaToken["interpreter_port"], ipchandles, metaToken["client"], metaToken["calciteTime"], resultSet.metadata.time, totalTime, '')
        return return_result
    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception as error:
        error_message = "Unexpected error on " + _run_query_get_results.__name__ + ", " + str(error)

    if error_message is not '':
        print(error_message)
    return_result = ResultSetHandle(None, None, metaToken["resultToken"], metaToken["interpreter_path"], metaToken["interpreter_port"], None, metaToken["client"], metaToken["calciteTime"], 0, 0, error_message)
    return return_result


from collections import namedtuple
from blazingdb.protocol.transport.channel import MakeRequestBuffer
from blazingdb.protocol.transport.channel import ResponseSchema
from blazingdb.protocol.transport.channel import ResponseErrorSchema
from blazingdb.protocol.orchestrator import OrchestratorMessageType
from blazingdb.protocol.io  import FileSystemRegisterRequestSchema, FileSystemDeregisterRequestSchema
from blazingdb.protocol.io import DriverType, FileSystemType, EncryptionType, FileSchemaType
import numpy as np
import pandas as pd

class SchemaFrom:
    CsvFile = 0
    ParquetFile = 1
    Gdf = 2


#cambiar para success or failed
def create_table(tableName, **kwargs):
    return_result = None
    error_message = ''

    columnNames = kwargs.get('names', [])
    columnTypes = kwargs.get('dtypes', [])
    dbName = 'main'
    schemaType = kwargs.get('type', None)
    gdf = kwargs.get('gdf', None)
    files = kwargs.get('path', [])
    csvDelimiter = kwargs.get('delimiter', '|')
    csvLineTerminator = kwargs.get('line_terminator', '\n')
    csvSkipRows = kwargs.get('skip_rows', 0)
    csvHeader = kwargs.get('header')

    if gdf is None:
        blazing_table = make_empty_BlazingTable()
    else:
        blazing_table = gdf_to_BlazingTable(gdf)

    if (len(columnTypes) > 0):
        columnTypes = gdf_dtypes_to_gdf_dtype_strs(columnTypes)

    try:
        client = _get_client()
        return_result = client.run_ddl_create_table(tableName, 
                        columnNames,columnTypes,dbName,schemaType,blazing_table,files,csvDelimiter,csvLineTerminator,csvSkipRows, csvHeader)
         
    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception as error:
        error_message = "Unexpected error on " + create_table.__name__ + ", " + str(error)

    if error_message is not '':
        print(error_message)

    #Todo Rommel check if this error happens
    #print("ERROR: unknown schema type")
    return return_result



def register_file_system(authority, type, root, params = None):
    if params is not None:
        params = namedtuple("FileSystemConnection", params.keys())(*params.values())
    client = _get_client()
    schema = FileSystemRegisterRequestSchema(authority, root, type, params)
    request_buffer = MakeRequestBuffer(OrchestratorMessageType.RegisterFileSystem,
                                       client.accessToken,
                                       schema)
    response_buffer = _send_request( client._orchestrator_path, client._orchestrator_port, request_buffer)
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        raise RuntimeError(ResponseErrorSchema.From(response.payload).errors)
    return response.status

def deregister_file_system(authority):
    schema = FileSystemDeregisterRequestSchema(authority)
    client = _get_client()
    request_buffer = MakeRequestBuffer(OrchestratorMessageType.DeregisterFileSystem,
                                       client.accessToken,
                                       schema)
    response_buffer = _send_request(client._orchestrator_path, client._orchestrator_port, request_buffer)
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        raise RuntimeError(ResponseErrorSchema.From(response.payload).errors)
    return response.status

def _create_dummy_table_group():
    database_name = 'main'
    tableGroup = OrderedDict([('name', database_name), ('tables', [])])
    return tableGroup
