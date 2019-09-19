import time
import re

from collections import OrderedDict

import cudf as gd

from collections import namedtuple

import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel

from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.errors import Error
from blazingdb.protocol.calcite.errors import SyntaxError
from blazingdb.protocol.transport.channel import MakeRequestBuffer
from blazingdb.protocol.transport.channel import ResponseSchema
from blazingdb.protocol.transport.channel import ResponseErrorSchema
from blazingdb.protocol.orchestrator import OrchestratorMessageType
from blazingdb.protocol.io  import FileSystemRegisterRequestSchema, FileSystemDeregisterRequestSchema
from blazingdb.protocol.io import DriverType, FileSystemType, EncryptionType, FileSchemaType
 
from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType, NodeTableSchema
from blazingdb.protocol.gdf import gdf_columnSchema

import pyarrow as pa
from cudf.bindings.cudf_cpp import *

from cudf.dataframe.string import StringColumn
from cudf import DataFrame
from cudf.dataframe.buffer import Buffer
from cudf.dataframe.columnops import build_column
from cudf.utils.utils import calc_chunk_size, mask_dtype, mask_bitsize

import numpy as np
import pandas as pd

import nvstrings

# NDarray device helper
from numba import cuda
from numba.cuda.cudadrv import driver, devices

require_context = devices.require_context
current_context = devices.get_context
gpus = devices.gpus

#Todo Rommel Percy : avoid using global variables, i.e. move these properties with the free function _to_table_group
dataColumnTokens = {}
validColumnTokens = {}


# BEGIN DataSource internal utils


# NOTE _CsvArgs() is an internal class for CSV args parsing & validation
class _CsvArgs():

    def __init__(self, paths, **kwargs):
        self.paths = paths
        self.column_names = kwargs.get('names', [])
        self.column_types = kwargs.get('dtype', [])
        self.delimiter = kwargs.get('delimiter', None)  # the actual default value will be set in the validation funcion
        self.skiprows = kwargs.get('skiprows', 0)
        self.lineterminator = kwargs.get('lineterminator', '\n')
        self.skipinitialspace = kwargs.get('skipinitialspace', False)
        self.delim_whitespace = kwargs.get('delim_whitespace', False)
        self.header = kwargs.get('header', -1)
        self.nrows = kwargs.get('nrows', None)  # the actual default value will be set in the validation funcion
        self.skip_blank_lines = kwargs.get('skip_blank_lines', True)
        self.quotechar = kwargs.get('quotechar', '\"')
        self.quoting = kwargs.get('quoting', 0)
        self.doublequote = kwargs.get('doublequote', True)
        self.decimal = kwargs.get('decimal', '.')
        self.skipfooter = kwargs.get('skipfooter', 0)
        self.keep_default_na = kwargs.get('keep_default_na', True)
        self.na_filter = kwargs.get('na_filter', True)
        self.dayfirst = kwargs.get('dayfirst', False)
        self.thousands = kwargs.get('thousands', '\0')
        self.comment = kwargs.get('comment', '\0')
        self.true_values = kwargs.get('true_values', [])
        self.false_values = kwargs.get('false_values', [])
        self.na_values = kwargs.get('na_values', [])

    # Validate especific params when a csv or psv file is not sent
    def validate_empty(self):
        self.delimiter = ','
        self.nrows = -1

    # Validate input params
    def validation(self):

        # delimiter
        if self.delimiter == None:
            first_path = self.paths[0]
            if first_path[-4:] == '.csv':
                self.delimiter = ","
            elif first_path[-4:] == '.psv':
                self.delimiter = "|"
            else:
                self.delimiter = ","

        # lineterminator
        if isinstance(self.lineterminator, bool):
            raise TypeError("object of type 'bool' has no len()")
        elif isinstance(self.lineterminator, int):
            raise TypeError("object of type 'int' has no len()")
        if len(self.lineterminator) > 1:
            raise ValueError("Only length-1 decimal markers supported")

        # skiprows
        if self.skiprows == None or self.skiprows < 0:
            self.skiprows = 0
        elif isinstance(self.skiprows, str):
            raise TypeError("an integer is required")

        # header
        if self.header == -1 and len(self.column_names) == 0:
            self.header = 0
        if self.header == None or self.header < -1 :
            self.header = -1
        elif isinstance(self.header, str):
            raise TypeError("header must be integer or list of integers")

        # nrows
        if self.nrows == None:
            self.nrows = -1
        elif self.nrows < 0:
            raise ValueError("'nrows' must be an integer >= 0")

        # skipinitialspace
        if self.skipinitialspace == None:
            raise TypeError("an integer is required")
        elif self.skipinitialspace == False:
            self.skipinitialspace = False
        else:
            self.skipinitialspace = True

        # delim_whitespace
        if self.delim_whitespace == None or self.delim_whitespace == False:
            self.delim_whitespace = False
        elif isinstance(self.delim_whitespace, str):
            raise TypeError("an integer is required")
        else:
            self.delim_whitespace = True

        # skip_blank_lines
        if self.skip_blank_lines == None or isinstance(self.skip_blank_lines, str):
            raise TypeError("an integer is required")
        if self.skip_blank_lines != False:
            self.skip_blank_lines = True

        # quotechar
        if self.quotechar == None:
            raise TypeError("quotechar must be set if quoting enabled")
        elif isinstance(self.quotechar, int):
            raise TypeError("quotechar must be string, not int")
        elif isinstance(self.quotechar, bool):
            raise TypeError("quotechar must be string, not bool")
        elif len(self.quotechar) > 1 :
            raise TypeError("quotechar must be a 1-character string")

        # quoting
        if isinstance(self.quoting, int) :
            if self.quoting < 0 or self.quoting > 3 :
                raise TypeError("bad 'quoting' value")
        else:
            raise TypeError(" 'quoting' must be an integer")

        # doublequote
        if self.doublequote == None or not isinstance(self.doublequote, int):
            raise TypeError("an integer is required")
        elif self.doublequote != False:
            self.doublequote = True

        # decimal
        if self.decimal == None:
            raise TypeError("object of type 'NoneType' has no len()")
        elif isinstance(self.decimal, bool):
            raise TypeError("object of type 'bool' has no len()")
        elif isinstance(self.decimal, int):
            raise TypeError("object of type 'int' has no len()")
        if len(self.decimal) > 1:
            raise ValueError("Only length-1 decimal markers supported")

        # skipfooter
        if self.skipfooter == True or isinstance(self.skipfooter, str):
            raise TypeError("skipfooter must be an integer")
        elif self.skipfooter == False or self.skipfooter == None:
            self.skipfooter = 0
        if self.skipfooter < 0:
            self.skipfooter = 0

        # keep_default_na
        if self.keep_default_na == False or self.keep_default_na == 0:
            self.keep_default_na = False
        else:
            self.keep_default_na = True

        # na_filter
        if self.na_filter == False or self.na_filter == 0:
            self.na_filter = False
        else:
            self.na_filter = True

        # dayfirst
        if self.dayfirst == True or self.dayfirst == 1:
            self.dayfirst = True
        else:
            self.dayfirst = False

        # thousands
        if self.thousands == None:
            self.thousands = '\0'
        elif isinstance(self.thousands, bool):
            raise TypeError("object of type 'bool' has no len()")
        elif isinstance(self.thousands, int):
            raise TypeError("object of type 'int' has no len()")
        if len(self.thousands) > 1:
            raise ValueError("Only length-1 decimal markers supported")

        # comment
        if self.comment == None:
            self.comment = '\0'
        elif isinstance(self.comment, bool):
            raise TypeError("object of type 'bool' has no len()")
        elif isinstance(self.comment, int):
            raise TypeError("object of type 'int' has no len()")
        if len(self.comment) > 1:
            raise ValueError("Only length-1 decimal markers supported")

        # true_values
        if isinstance(self.true_values, bool):
            raise TypeError("'bool' object is not iterable")
        elif isinstance(self.true_values, int):
            raise TypeError("'int' object is not iterable")
        elif self.true_values == None:
            self.true_values = []
        elif isinstance(self.true_values, str):
            self.true_values = self.true_values.split(',')

        # false_values
        if isinstance(self.false_values, bool):
            raise TypeError("'bool' object is not iterable")
        elif isinstance(self.false_values, int):
            raise TypeError("'int' object is not iterable")
        elif self.false_values == None:
            self.false_values = []
        elif isinstance(self.false_values, str):
            self.false_values = self.false_values.split(',')

        # na_values
        if isinstance(self.na_values , int) or isinstance(self.na_values , bool):
            self.na_values = str(self.na_values).split(',')
        elif self.na_values == None:
            self.na_values = []


# NOTE _OrcArgs() is an internal class for ORC args parsing & validation
class _OrcArgs():

    def __init__(self, **kwargs):
        self.stripe = kwargs.get('stripe', -1)
        self.skip_rows = kwargs.get('skip_rows', None)  # the actual default value will be set in the validation funcion
        self.num_rows = kwargs.get('num_rows', None)  # the actual default value will be set in the validation funcion
        self.use_index = kwargs.get('use_index', False)

    # Validate especific params when a csv or psv file is not sent
    def validate_empty(self):
        self.skip_rows = 0
        self.num_rows = -1

    # Validate input params
    def validation(self):

        # skip_rows
        if self.skip_rows == None:
            self.skip_rows = 0
        elif self.skip_rows < 0:
            raise ValueError("'skip_rows' must be an integer >= 0")

        # num_rows
        if self.num_rows == None:
            self.num_rows = -1
        elif self.num_rows < 0:
            raise ValueError("'num_rows' must be an integer >= 0")


def _make_default_orc_arg(**kwargs):
    orc_args = _OrcArgs(**kwargs)
    orc_args.validate_empty()
    return orc_args


def _make_default_csv_arg(**kwargs):
    paths = kwargs.get('path', [])
    csv_args = _CsvArgs(paths, **kwargs)
    csv_args.validate_empty()
    return csv_args

# END DataSource internal utils

# connection_path is a ip/host when tcp and can be unix socket when ipc
def _send_request(connection_path, connection_port, requestBuffer):
    connection = blazingdb.protocol.TcpSocketConnection(connection_path, connection_port)
    client = blazingdb.protocol.Client(connection)
    return client.send(requestBuffer)

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class PyConnector(metaclass=Singleton):
    def __init__(self):
        self._orchestrator_path = '127.0.0.1'
        self._orchestrator_port = 8889
        self._accessToken = None

    def __del__(self):
        try:
            self.close_connection()
        except:
            pass

    def connect(self, orchestrator_path, orchestrator_port):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("open connection")
        self._orchestrator_path = orchestrator_path
        self._orchestrator_port = orchestrator_port

        if self._accessToken is not None:
            print("Already connected to the Orchestrator")
            return

        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthOpen, authSchema)

        responseBuffer = _send_request(self._orchestrator_path,
            self._orchestrator_port, requestBuffer)

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
                             resultToken,
                             csv_args,
                             jsonLines,
                             orc_args,
                             daskTables):

        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDDLCreateTableRequestSchema(name=tableName,
                                                                                       dbName=dbName,
                                                                                       schemaType=schemaType,
                                                                                       gdf=blazing_table,
                                                                                       files=files,
                                                                                       resultToken=resultToken,
                                                                                       columnNames=columnNames,
                                                                                       columnTypes=columnTypes,
                                                                                       csvDelimiter=csv_args.delimiter,
                                                                                       csvLineTerminator=csv_args.lineterminator,
                                                                                       csvSkipRows=csv_args.skiprows,
                                                                                       csvHeader=csv_args.header,
                                                                                       csvNrows=csv_args.nrows,
                                                                                       csvSkipinitialspace=csv_args.skipinitialspace,
                                                                                       csvDelimWhitespace=csv_args.delim_whitespace,
                                                                                       csvSkipBlankLines=csv_args.skip_blank_lines,
                                                                                       csvQuotechar=csv_args.quotechar,
                                                                                       csvQuoting=csv_args.quoting,
                                                                                       csvDoublequote=csv_args.doublequote,
                                                                                       csvDecimal=csv_args.decimal,
                                                                                       csvSkipfooter=csv_args.skipfooter,
                                                                                       csvNaFilter=csv_args.na_filter,
                                                                                       csvKeepDefaultNa=csv_args.keep_default_na,
                                                                                       csvDayfirst=csv_args.dayfirst,
                                                                                       csvThousands=csv_args.thousands,
                                                                                       csvComment=csv_args.comment,
                                                                                       csvTrueValues=csv_args.true_values,
                                                                                       csvFalseValues=csv_args.false_values,
                                                                                       csvNaValues=csv_args.na_values,
                                                                                       jsonLines=jsonLines,
                                                                                       orcStripe=orc_args.stripe,
                                                                                       orcSkipRows=orc_args.skip_rows,
                                                                                       orcNumRows=orc_args.num_rows,
                                                                                       orcUseIndex=orc_args.use_index,
                                                                                       daskTables=daskTables)

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

        distributed_response = blazingdb.protocol.orchestrator.DMLDistributedResponseSchema.From(response.payload)

        return list(item for item in distributed_response.responses)


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

    def run_scan_datasource(self, directory, wildcard):
        datasourceSchema = blazingdb.protocol.orchestrator.BuildDataSourceRequestSchema(directory = directory, wildcard = wildcard)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.ScanDataSource, self._accessToken, datasourceSchema)

        responseBuffer = _send_request(self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)

        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
            raise RuntimeError(errorResponse.errors.decode("utf-8"))

        datasource_response = blazingdb.protocol.orchestrator.DataSourceResponseSchema.From(response.payload)
        files = list(item.decode("utf-8") for item in datasource_response.files)

        return files


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
    return PyConnector()

class ResultSetHandle:

    def __init__(self, columns, columnTokens, resultToken, interpreter_path, interpreter_port, handle, client, calciteTime, ralTime, totalTime, error_message, total_nodes, n_crashed_nodes):
        self.columns = columns
        self.columnTokens = columnTokens

        self._buffer_ids = []
        if columns is not None:
            if columns.columns.size>0 and columnTokens is not None:
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
        self.total_nodes =  total_nodes
        self.n_crashed_nodes = n_crashed_nodes

    def __del__(self):
        for key in self._buffer_ids:
            dataColumnTokens.pop(key, None)
            validColumnTokens.pop(key, None)

        if self.handle is not None:
            for ipch in self.handle: #todo add NVStrings handles
                ipch.close()
            del self.handle
            try :
                self.client.free_result(self.resultToken,self.interpreter_path,self.interpreter_port)
            except:
                pass



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
                            'error_message' : self.error_message
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
            'boolean':gdf_dtype.GDF_BOOL8
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


def SetupOrchestratorConnection(orchestrator_host_ip, orchestrator_port):
    client = PyConnector()
    client.connect(orchestrator_host_ip, orchestrator_port)

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

# interpreter_path is the TCP protocol port for RAL
def _private_get_result(resultToken, interpreter_path, interpreter_port, calciteTime):
    client = _get_client()

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

        dist_token = client.run_dml_query_token(sql, tableGroup)

        return dist_token
    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception:
        error_message = "Unexpected error on " + _run_query_get_token.__name__

    if error_message is not '':
        print(error_message)

    # metaToken = {"client" : client, "resultToken" : resultToken, "interpreter_path" : interpreter_path, "interpreter_port" : interpreter_port, "startTime" : startTime, "calciteTime" : calciteTime}
    # return metaToken

    # TODO make distributed result set if there is error

def run_query_get_results(metaToken, startTime):
    return _run_query_get_results(metaToken, startTime)

def _run_query_get_results(distMetaToken, startTime):
    error_message = ''

    client = _get_client()
    totalTime = 0
    total_nodes = 1
    n_crashed_nodes = 0

    result_list = []
    for result in distMetaToken:
        try:
            resultSet, ipchandles = _private_get_result(result.resultToken,
                                                        result.nodeConnection.path.decode('utf8'),
                                                        result.nodeConnection.port,
                                                        result.calciteTime)

            totalTime = (time.time() - startTime) * 1000  # in milliseconds

            result_list.append({'result': result, 'resultSet': resultSet, 'ipchandles': ipchandles, 'totalTime':totalTime, 'error_message':''})

        except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
            error_message = error
        except Error as error:
            error_message = str(error)
        except Exception as error:
            error_message = "Unexpected error on " + _run_query_get_results.__name__ + ", " + str(error)

        if error_message is not '':
            print(error_message)
            result_list.append({'result': result, 'resultSet': None, 'ipchandles': None, 'totalTime':0, 'error_message':error_message})

        if error_message is not '':
            print(error_message)
            n_crashed_nodes = n_crashed_nodes + 1

    result_set_list = []

    for result in result_list:
        if result['error_message'] is not '':
            result_set_list.append(ResultSetHandle(None,
                                                   None,
                                                   result['result'].resultToken,
                                                   result['result'].nodeConnection.path.decode('utf8'),
                                                   result['result'].nodeConnection.port,
                                                   None,
                                                   client,
                                                   result['result'].calciteTime,
                                                   None,
                                                   0,
                                                   result['error_message'],
                                                   total_nodes,  #total_nodes
                                                   n_crashed_nodes   #n_crashed_nodes
                                                   ))
        else:
            result_set_list.append(ResultSetHandle(result['resultSet'].columns,
                                                   result['resultSet'].columnTokens,
                                                   result['result'].resultToken,
                                                   result['result'].nodeConnection.path.decode('utf8'),
                                                   result['result'].nodeConnection.port,
                                                   result['ipchandles'],
                                                   client,
                                                   result['result'].calciteTime,
                                                   result['resultSet'].metadata.time,
                                                   result['totalTime'],
                                                   result['error_message'],
                                                   total_nodes,  #total_nodes
                                                   n_crashed_nodes   #n_crashed_nodes
                                                   ))

    if len(result_set_list) == 1:
        result_set_list = result_set_list[0]

    return result_set_list


def _get_result_dask(resultToken, interpreter_path, interpreter_port, calciteTime,client):

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

def convert_result_msg(metaToken,connection):

    resultSet, ipchandles = _get_result_dask(metaToken[0].resultToken,"127.0.0.1",8891,0,connection)

    totalTime = 0  # in milliseconds

    result = {'result': metaToken[0], 'resultSet': resultSet, 'ipchandles': ipchandles}

    return ResultSetHandle(result['resultSet'].columns,
                           result['resultSet'].columnTokens,
                           result['result'].resultToken,
                           result['result'].nodeConnection.path.decode('utf8'),
                           result['result'].nodeConnection.port,
                           result['ipchandles'],
                           connection,
                           result['result'].calciteTime,
                           result['resultSet'].metadata.time,
                           totalTime,
                           '',
                           1,  # TODO: Use connection to get total_nodes
                           0)  # and n_crashed_nodes


def convert_to_dask(metaToken,connection):
  if metaToken:  # TODO: check why metaToken can equals None (check RAL)
        result_set = convert_result_msg(metaToken,connection)
        return result_set.columns.copy(deep=True)

def run_query_get_concat_results(metaToken, startTime):
    return _run_query_get_concat_results(metaToken, startTime)

def _run_query_get_concat_results(distMetaToken, startTime):

    from cudf.multi import concat

    client = _get_client()

    all_error_messages = ''
    result_list = []
    ral_count = 0
    sum_calcite_time = 0
    sum_ral_time = 0
    sum_total_time = 0
    total_nodes = 0
    n_crashed_nodes = 0

    for result in distMetaToken:
        ral_count = ral_count + 1
        error_message = ''
        try:
            totalTime = 0
            resultSet, ipchandles = _private_get_result(result.resultToken,
                                                        result.nodeConnection.path.decode('utf8'),
                                                        result.nodeConnection.port,
                                                        result.calciteTime)

            totalTime = (time.time() - startTime) * 1000  # in milliseconds

            sum_calcite_time = sum_calcite_time + result.calciteTime
            sum_ral_time =  sum_ral_time  + resultSet.metadata.time
            sum_total_time =  sum_total_time + totalTime

            result_list.append(resultSet)
        except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
            error_message = error
        except Error as error:
            error_message = str(error)
        except Exception as error:
            error_message = "Unexpected error on " + _run_query_get_results.__name__ + ", " + str(error)

        total_nodes = total_nodes + 1

        if error_message is not '':
            print(error_message)
            all_error_messages = all_error_messages + " Node " + str(ral_count) + ":" + str(error_message)
            n_crashed_nodes = n_crashed_nodes + 1

    need_to_concat = sum([len(result.columns) > 0 for result in result_list]) > 1

    gdf =  None

    if (need_to_concat):
        all_gdfs = [result.columns for result in result_list]
        gdf =  concat(all_gdfs, ignore_index=True)
    else:
        for result in result_list:  # if we dont need to concatenate, likely we only have one, or only one that has data
            if (len(result.columns) > 0): # this is the one we want to return, but we need to deep copy it first. We only need to deepcopy the non strings.
                 gdf = result.columns
                 for col_name, col in gdf._cols.items():
                     if (col.dtype != 'object'):
                         gdf[col_name] = gdf[col_name].copy(deep=True)

    resultSetHandle = ResultSetHandle(gdf,
                                       None,
                                       None,
                                       None,
                                       None,
                                       None,
                                       None,
                                       sum_calcite_time,
                                       sum_ral_time,
                                       sum_total_time,
                                       all_error_messages,
                                       total_nodes, #total_nodes
                                       n_crashed_nodes  #n_crashed_nodes
                                       )

    return resultSetHandle



from collections import namedtuple
from blazingdb.protocol.transport.channel import MakeRequestBuffer
from blazingdb.protocol.transport.channel import ResponseSchema
from blazingdb.protocol.transport.channel import ResponseErrorSchema
from blazingdb.protocol.orchestrator import OrchestratorMessageType
from blazingdb.protocol.io  import FileSystemRegisterRequestSchema, FileSystemDeregisterRequestSchema
from blazingdb.protocol.io import DriverType, FileSystemType, EncryptionType, FileSchemaType
import numpy as np
import pandas as pd
import pyblazing

class SchemaFrom:
    CsvFile = 0
    ParquetFile = 1
    Gdf = 2
    Distributed = 3
    JsonFile = 4
    OrcFile = 5
    Dask = 6


#cambiar para success or failed
def create_table(tableName, **kwargs):
    return_result = None
    error_message = ''
    columnNames = kwargs.get('names', [])
    columnTypes = kwargs.get('dtypes', [])
    dbName = 'main'
    schemaType = kwargs.get('type', None)
    gdf = kwargs.get('gdf', None)
    dask_cudf = kwargs.get('dask_cudf', None)
    files = kwargs.get('path', [])
    resultToken = kwargs.get('resultToken', 0)
    csv_args = kwargs.get('csv_args', None)
    jsonLines = kwargs.get('lines', True)
    orc_args = kwargs.get('orc_args', None)

    if orc_args == None:
        orc_args = _make_default_orc_arg(**kwargs) # create a OrcArgs with default args

    if gdf is None:
        blazing_table = make_empty_BlazingTable()
    else:
        blazing_table = gdf_to_BlazingTable(gdf)

    if dask_cudf is None:
        dask_tables = []
    else:
        dask_client = kwargs['dask_client']
        dask_tables = dask_cudf_to_BlazingDaskTable(dask_cudf, dask_client)

    if (len(columnTypes) > 0):
        columnTypes = gdf_dtypes_to_gdf_dtype_strs(columnTypes)

    if csv_args is not None:
        if (len(csv_args.column_types) > 0):
            columnTypes = gdf_dtypes_to_gdf_dtype_strs(get_dtype_values(csv_args.column_types))
        columnNames=csv_args.column_names
    else:
        csv_args = _make_default_csv_arg(**kwargs)

    try:
        client = _get_client()
        return_result = client.run_ddl_create_table(tableName,
                                                    columnNames,
                                                    columnTypes,
                                                    dbName,
                                                    schemaType,
                                                    blazing_table,
                                                    files,
                                                    resultToken,
                                                    csv_args,
                                                    jsonLines,
                                                    orc_args,
                                                    dask_tables)

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


def get_machine_and_blazing_table(partition):
    return socket.gethostname(), gdf_to_BlazingTable(gdf)


def columnSchemaFrom(dask_column):
    from blazingdb.protocol.gdf import (cudaIpcMemHandle_tSchema,
                                        gdf_dtype_extra_infoSchema,
                                        custringsData_tSchema,
                                        gdf_columnSchema)

    raw_data = dask_column['data']
    data = cudaIpcMemHandle_tSchema(reserved=raw_data if raw_data else b'')

    raw_valid = dask_column['valid']
    valid = cudaIpcMemHandle_tSchema(reserved=raw_valid if raw_valid else b'')

    raw_custrings_data = (dask_column['custrings_data']
                          if 'custrings_data' in dask_column
                          else None)
    custrings_data = custringsData_tSchema(
        reserved=raw_custrings_data if raw_custrings_data else b'')

    dtype_info = gdf_dtype_extra_infoSchema(time_unit=0)

    return gdf_columnSchema(data=data,
                            valid=valid,
                            size=dask_column['size'],
                            dtype=dask_column['dtype'],
                            dtype_info=dtype_info,
                            null_count=dask_column['null_count'],
                            custrings_data=custrings_data)


def tableSchemaFrom(dask_cudf):
    from blazingdb.protocol.orchestrator import BlazingTableSchema
    return BlazingTableSchema(columns=[columnSchemaFrom(dask_column)
                                       for dask_column in dask_cudf['columns']],
                              columnTokens=dask_cudf['columnTokens'],
                              resultToken=dask_cudf['resultToken'])


def dask_cudf_to_BlazingDaskTable(dask_cudf, dask_client):
    persisted_cudf = dask_cudf

    distributedBlazingTables = persisted_cudf.map_partitions(
        gdf_to_BlazingTable).compute()

    who_has = dask_client.who_has()
    ips = [re.findall(r'(?:\d+\.){3}\d+', who_has[str(k)][0])[0]
           for k in dask_cudf.dask.keys()]

    dask_cudf_ret = [NodeTableSchema(ip=p[0], gdf=tableSchemaFrom(p[1]))
                     for p in zip(ips, distributedBlazingTables)]

    return dask_cudf_ret



def register_file_system(authority, type, root, params = None):
    if params is not None:
        params = namedtuple("FileSystemConnection", params.keys())(*params.values())
    client = _get_client()
    schema = FileSystemRegisterRequestSchema(authority, root, type, params)
    request_buffer = MakeRequestBuffer(OrchestratorMessageType.RegisterFileSystem,
                                       client._accessToken,
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
                                       client._accessToken,
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

def gdf_to_np_dtype(dtype):
    """Util to convert gdf dtype to numpy dtype.
    """
    gdf_dtypes = {
        gdf_dtype.GDF_FLOAT64: np.float64,
        gdf_dtype.GDF_FLOAT32: np.float32,
        gdf_dtype.GDF_INT64: np.int64,
        gdf_dtype.GDF_INT32: np.int32,
        gdf_dtype.GDF_INT16: np.int16,
        gdf_dtype.GDF_INT8: np.int8,
        gdf_dtype.GDF_BOOL8: np.bool_,
        gdf_dtype.GDF_DATE64: np.datetime64,
        gdf_dtype.GDF_TIMESTAMP: np.datetime64,
        gdf_dtype.GDF_CATEGORY: np.int32,
        gdf_dtype.GDF_STRING_CATEGORY: np.object_,
        gdf_dtype.GDF_STRING: np.object_,
        gdf_dtype.N_GDF_TYPES: np.int32
    }
    return np.dtype(gdf_dtypes[dtype])



def scan_datasource(directory, wildcard):
    return_result = None
    error_message = ''
    files = None
    
    try:
        client = _get_client()
        files = client.run_scan_datasource(directory, wildcard)

    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception as error:
        error_message = "Unexpected error on " + scan_datasource.__name__ + ", " + str(error)

    if error_message is not '':
        raise RuntimeError(error_message)

    return files
