import cudf as gd

import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from blazingdb.protocol.errors import Error
from blazingdb.protocol.calcite.errors import SyntaxError
from blazingdb.messages.blazingdb.protocol.Status import Status

from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType
from blazingdb.protocol.gdf import gdf_columnSchema

from librmm_cffi import librmm as rmm
from libgdf_cffi import ffi, libgdf
from cudf.dataframe.datetime import DatetimeColumn
from cudf.dataframe.numerical import NumericalColumn

import pyarrow as pa
from cudf import _gdf
from cudf.dataframe.column import Column
from cudf import DataFrame
from cudf.dataframe.dataframe import Series
from cudf.dataframe.buffer import Buffer
from cudf import utils
from cudf.utils.utils import calc_chunk_size, mask_dtype, mask_bitsize

from numba import cuda
import numpy as np
import pandas as pd

import time

# NDarray device helper
from numba import cuda
from numba.cuda.cudadrv import driver, devices
require_context = devices.require_context
current_context = devices.get_context
gpus = devices.gpus

class ResultSetHandle:

    columns = None
    columnTokens = None
    resultToken = 0
    interpreter_path = None # if tcp is the host/ip, if ipc is unix socket path
    interpreter_port = None # if tcp is valid, if ipc is None
    handle = None
    client = None
    error_message = None # when empty the query ran succesfully

    def __init__(self, columns, columnTokens, resultToken, interpreter_path, interpreter_port, handle, client, calciteTime, ralTime, totalTime, error_message):
        self.columns = columns
        self.columnTokens = columnTokens

        if columns is not None:
            if columns.columns.size>0:
                idx = 0
                for col in self.columns.columns:
                    self.columns[col].columnToken = columnTokens[idx]
                    idx = idx + 1
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
        # @todo
        if self.handle is not None:
            for ipch in self.handle:
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



def run_query(sql, tables):
    """
    Run a SQL query over a dictionary of GPU DataFrames.
    Parameters
    ----------
    sql : str
        The SQL query.
    tables : dict[str]:GPU ``DataFrame``
        A dictionary where each key is the table name and each value is the
        associated GPU ``DataFrame`` object.
    Returns
    -------
    A GPU ``DataFrame`` object that contains the SQL query result.
    Examples
    --------
    >>> import cudf as gd
    >>> import pyblazing
    >>> products = gd.DataFrame({'month': [2, 8, 11], 'sales': [12.1, 20.6, 13.79]})
    >>> cats = gd.DataFrame({'age': [12, 28, 19], 'weight': [5.3, 9, 7.68]})
    >>> tables = {'products': products, 'cats': cats}
    >>> result = pyblazing.run_query('select * from products, cats limit 2', tables)
    >>> type(result)
    cudf.dataframe.DataFrame
    """
    return _private_run_query(sql, tables)

def run_query_get_token(sql, tables):
    return _run_query_get_token(sql, tables)

def run_query_get_results(metaToken):
    return _run_query_get_results(metaToken)

def run_query_pandas(sql, tables):
    """
    Run a SQL query over a dictionary of Pandas DataFrames.
    This convenience function will convert each table from Pandas DataFrame
    to GPU ``DataFrame`` and then will use ``run_query``.
    Parameters
    ----------
    sql : str
        The SQL query.
    tables : dict[str]:Pandas DataFrame
        A dictionary where each key is the table name and each value is the
        associated Pandas DataFrame object.
    Returns
    -------
    A GPU ``DataFrame`` object that contains the SQL query result.
    Examples
    --------
    >>> import pandas as pd
    >>> import pyblazing
    >>> products = pd.DataFrame({'month': [2, 8, 11], 'sales': [12.1, 20.6, 13.79]})
    >>> cats = pd.DataFrame({'age': [12, 28, 19], 'weight': [5.3, 9, 7.68]})
    >>> tables = {'products': products, 'cats': cats}
    >>> result = pyblazing.run_query_pandas('select * from products, cats limit 2', tables)
    >>> type(result)
    cudf.dataframe.DataFrame
    """

    gdf_tables = {}
    for table, df in tables.items():
        gdf = gd.DataFrame.from_pandas(df)
        gdf_tables[table] = gdf

    return run_query(sql, gdf_tables)


# TODO complete API docs
# WARNING EXPERIMENTAL
def _run_query_arrow(sql, tables):
    """
    Run a SQL query over a dictionary of pyarrow.Table.
    This convenience function will convert each table from pyarrow.Table
    to Pandas DataFrame and then will use ``run_query_pandas``.
    Parameters
    ----------
    sql : str
        The SQL query.
    tables : dict[str]:pyarrow.Table
        A dictionary where each key is the table name and each value is the
        associated pyarrow.Table object.
    Returns
    -------
    A GPU ``DataFrame`` object that contains the SQL query result.
    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyblazing
    >>> products = pa.RecordBatchStreamReader('products.arrow').read_all()
    >>> cats = pa.RecordBatchStreamReader('cats.arrow').read_all()
    >>> tables = {'products': products, 'cats': cats}
    >>> result = pyblazing.run_query_arrow('select * from products, cats limit 2', tables)
    >>> type(result)
    cudf.dataframe.DataFrame
    """

    pandas_tables = {}
    for table, arr in tables.items():
        df = arr.to_pandas()
        pandas_tables[table] = df

    return run_query_pandas(sql, pandas_tables)



def _lots_of_stuff():
    pass


class PyConnector:
    def __init__(self, orchestrator_path, orchestrator_port):
        self._orchestrator_path = orchestrator_path
        self._orchestrator_port = orchestrator_port

    def __del__(self):
        try:
            self.close_connection()
        except:
            print("Can't close connection, probably it was lost")

    def connect(self):
        self.accessToken = 0
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("open connection")
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthOpen, authSchema)

        responseBuffer = self._send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            print(errorResponse.errors)
            raise Error(errorResponse.errors)
        responsePayload = blazingdb.protocol.orchestrator.AuthResponseSchema.From(
            response.payload)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(responsePayload.accessToken)
        self.accessToken = responsePayload.accessToken

    # connection_path is a ip/host when tcp and can be unix socket when ipc
    def _send_request(self, connection_path, connection_port, requestBuffer):
        connection = blazingdb.protocol.UnixSocketConnection(connection_path)
        client = blazingdb.protocol.Client(connection)
        return client.send(requestBuffer)

    def run_dml_load_parquet_schema(self, path):
        print('load parquet file')
        ## todo use rowGroupIndices, and columnIndices, someway??
        requestSchema = blazingdb.protocol.io.ParquetFileSchema(path=path, rowGroupIndices=[], columnIndices=[])

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.LoadParquetSchema,
                                                                               self.accessToken, requestSchema)
        responseBuffer = self._send_request(self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            if b'SqlSyntaxException' in errorResponse.errors:
                raise SyntaxError(errorResponse.errors.decode('utf-8'))
            elif b'SqlValidationException' in errorResponse.errors:
                raise ValueError(errorResponse.errors.decode('utf-8'))
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path, dmlResponseDTO.nodeConnection.port

    def run_dml_load_csv_schema(self, path, names, dtypes, delimiter = '|', line_terminator='\n', skip_rows=0):
        print('load csv file')
        requestSchema = blazingdb.protocol.io.CsvFileSchema(path=path, delimiter=delimiter, lineTerminator = line_terminator, skipRows=skip_rows, names=names, dtypes=dtypes)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.LoadCsvSchema,
                                                                                self.accessToken, requestSchema)
        responseBuffer = self._send_request(self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            if b'SqlSyntaxException' in errorResponse.errors:
                raise SyntaxError(errorResponse.errors.decode('utf-8'))
            elif b'SqlValidationException' in errorResponse.errors:
                raise ValueError(errorResponse.errors.decode('utf-8'))
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)

        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path.decode('utf8'), dmlResponseDTO.nodeConnection.port

    def run_dml_query_token(self, query, tableGroup):
        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDMLRequestSchema(query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML, self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
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
            raise Error(errorResponse.errors.decode('utf-8'))
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)

        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path.decode('utf8'), dmlResponseDTO.nodeConnection.port, dmlResponseDTO.calciteTime

    def run_dml_query_filesystem_token(self, query, tableGroup):
        dmlRequestSchema = blazingdb.protocol.io.BuildFileSystemDMLRequestSchema(query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML_FS,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
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
            raise Error(errorResponse.errors.decode('utf-8'))
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path.decode('utf8'), dmlResponseDTO.nodeConnection.port, dmlResponseDTO.calciteTime

    def run_dml_query(self, query, tableGroup):
        # TODO find a way to print only for debug mode (add verbose arg)
        # print(query)
        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDMLRequestSchema(
            query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return self._get_result(dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path, dmlResponseDTO.nodeConnection.port)

    def run_ddl_create_table(self, tableName, columnNames, columnTypes, dbName):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print('create table: ' + tableName)
        # print(columnNames)
        # print(columnTypes)
        # print(dbName)
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLCreateTableRequestSchema(name=tableName,
                                                                                       columnNames=columnNames,
                                                                                       columnTypes=columnTypes,
                                                                                       dbName=dbName)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(dmlRequestSchema)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_CREATE_TABLE,
                                                                               self.accessToken, dmlRequestSchema)

        responseBuffer = self._send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

        return response.status

    def run_ddl_drop_table(self, tableName, dbName):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print('drop table: ' + tableName)

        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLDropTableRequestSchema(
            name=tableName, dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_DROP_TABLE,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors.decode('utf-8'))

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

        return response.status

    def close_connection(self):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("close connection")

        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            OrchestratorMessageType.AuthClose, self.accessToken, authSchema)

        responseBuffer = self._send_request(
            self._orchestrator_path, self._orchestrator_port, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors.decode('utf-8'))

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

    def free_memory(self, interpreter_path, interpreter_port):
        result_token = 2433423
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.FreeMemory, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
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
            InterpreterMessage.FreeResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
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
            InterpreterMessage.GetResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
            interpreter_path, interpreter_port, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        queryResult = blazingdb.protocol.interpreter.GetQueryResultFrom(
            response.payload)

        if queryResult.metadata.status.decode() == "Error":
            raise Error(queryResult.metadata.message.decode('utf-8'))

        return queryResult


def gen_data_frame(nelem, name, dtype):
    pdf = pd.DataFrame()
    pdf[name] = np.arange(nelem, dtype=dtype)
    df = DataFrame.from_pandas(pdf)
    return df


def get_ipc_handle_for_data(column, dataframe_column):

    if hasattr(dataframe_column, 'columnToken'):
        return None
    else:
       ipch = dataframe_column._column._data.mem.get_ipc_handle()
       return bytes(ipch._ipc_handle.handle)

def get_ipc_handle_for_valid(column, dataframe_column):

    if hasattr(dataframe_column, 'columnToken'):
        return None
    else:
       ipch = dataframe_column._column._mask.mem.get_ipc_handle()
       return bytes(ipch._ipc_handle.handle)


def gdf_column_type_to_str(dtype):
    str_dtype = {
        0: 'GDF_invalid',
        1: 'GDF_INT8',
        2: 'GDF_INT16',
        3: 'GDF_INT32',
        4: 'GDF_INT64',
        5: 'GDF_FLOAT32',
        6: 'GDF_FLOAT64',
        7: 'GDF_DATE32',
        8: 'GDF_DATE64',
        9: 'GDF_TIMESTAMP',
        10: 'GDF_CATEGORY',
        11: 'GDF_STRING',
        12: 'GDF_UINT8',
        13: 'GDF_UINT16',
        14: 'GDF_UINT32',
        15: 'GDF_UINT64',
        16: 'N_GDF_TYPES'
    }

    return str_dtype[dtype]


def _get_table_def_from_gdf(gdf):
    
    colNames = []
    types = []
    for colName, column in gdf._cols.items():
        dtype = column._column.cffi_view.dtype
        colNames.append(colName)
        types.append(gdf_column_type_to_str(dtype))
    return colNames, types


def _reset_table(client, table, gdf):
    client.run_ddl_drop_table(table, 'main')
    cols, types = _get_table_def_from_gdf(gdf)
    client.run_ddl_create_table(table, cols, types, 'main')

# TODO add valid support


def _to_table_group(tables):
    database_name = 'main'
    tableGroup = {'name': database_name}
    blazing_tables = []
    for table, gdf in tables.items():

        # TODO columnNames should have the columns of the query (check this)
        blazing_table = {'name': database_name + '.' + table,
                         'columnNames': gdf.columns.values.tolist()}
        blazing_columns = []
        columnTokens = []

        for column in gdf.columns:
            dataframe_column = gdf._cols[column]
            # TODO support more column types

            numerical_column = dataframe_column._column
            data_sz = numerical_column.cffi_view.size
            dtype = numerical_column.cffi_view.dtype
            null_count = numerical_column.cffi_view.null_count

            data_ipch = get_ipc_handle_for_data(column, dataframe_column)
            valid_ipch = None

            if (null_count > 0):
                valid_ipch = get_ipc_handle_for_valid(column, dataframe_column)

            blazing_column = {
                'data': data_ipch,
                'valid': valid_ipch,
                'size': data_sz,
                'dtype': dtype,
                'null_count': null_count,
                'dtype_info': 0
            }

            if hasattr(gdf[column], 'columnToken'):
                columnTokens.append(gdf[column].columnToken)
            else:
                columnTokens.append(0)

            blazing_columns.append(blazing_column)

        blazing_table['columns'] = blazing_columns
        if hasattr(gdf, 'resultToken'):
            blazing_table['resultToken'] = gdf.resultToken
        else:
            blazing_table['resultToken'] = 0

        blazing_table['columnTokens'] = columnTokens
        blazing_tables.append(blazing_table)

    tableGroup['tables'] = blazing_tables
    return tableGroup


def _get_client_internal(orchestrator_ip, orchestrator_port):
    client = PyConnector(orchestrator_ip, orchestrator_port)

    try:
        client.connect()
    except Error as err:
        print(err)
    except RuntimeError as err:
        print("Connection to the Orchestrator could not be started")

    return client

__orchestrator_ip = '/tmp/orchestrator.socket'
__orchestrator_port = 8890
__blazing__global_client = _get_client_internal(__orchestrator_ip, __orchestrator_port)

def _get_client():
    return __blazing__global_client


from librmm_cffi import librmm as rmm

# TODO: this function was copied from column.py in cudf  but fixed so that it can handle a null mask. cudf has a bug there
def cffi_view_to_column_mem(cffi_view):
    intaddr = int(ffi.cast("uintptr_t", cffi_view.data))
    data = rmm.device_array_from_ptr(intaddr,
                                     nelem=cffi_view.size,
                                     dtype=_gdf.gdf_to_np_dtype(cffi_view.dtype),
                                     finalizer=rmm._make_finalizer(0, 0))

    if cffi_view.valid:
        intaddr = int(ffi.cast("uintptr_t", cffi_view.valid))
        mask = rmm.device_array_from_ptr(intaddr,
                                         nelem=calc_chunk_size(cffi_view.size,
                                                               mask_bitsize),
                                         dtype=_gdf.mask_dtype,
                                         finalizer=rmm._make_finalizer(0,
                                                                       0))
    else:
        mask = None

    return data, mask

# TODO: this function was copied from column.py in cudf  but fixed so that it can handle a null mask. cudf has a bug there
def from_cffi_view(cffi_view):
    """Create a Column object from a cffi struct gdf_column*.
    """
    data_mem, mask_mem = cffi_view_to_column_mem(cffi_view)
    data_buf = Buffer(data_mem)

    if mask_mem is not None:
        mask_buf = Buffer(mask_mem)
    else:
        mask_buf = None

    return Column(data=data_buf, mask=mask_buf)


# TODO: this code does not seem to handle nulls at all. This will need to be addressed
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

 # TODO: this function was copied from _gdf.py in cudf  but fixed so that it can handle a null mask. cudf has a bug there
def columnview_from_devary(data_devary, mask_devary, dtype=None):
    return _gdf._columnview(size=data_devary.size,  data=_gdf.unwrap_devary(data_devary),
               mask=_gdf.unwrap_mask(mask_devary)[0] if mask_devary is not None else ffi.NULL, dtype=dtype or data_devary.dtype,
               null_count=0)

def _private_get_result(resultToken, interpreter_path, interpreter_port, calciteTime):
    client = _get_client()

    #print(interpreter_path)
    #print(interpreter_port)
    resultSet = client._get_result(resultToken, interpreter_path, interpreter_port)

    gdf_columns = []
    ipchandles = []
    for i, c in enumerate(resultSet.columns):
        if c.size != 0 :
            assert len(c.data) == 64
            ipch_data, data_ptr = _open_ipc_array(
                c.data, shape=c.size, dtype=_gdf.gdf_to_np_dtype(c.dtype))
            ipchandles.append(ipch_data)

            valid_ptr = None
            if (c.null_count > 0):
                ipch_valid, valid_ptr = _open_ipc_array(
                    c.valid, shape=calc_chunk_size(c.size, mask_bitsize), dtype=np.int8)
                ipchandles.append(ipch_valid)

            # TODO: this code imitates what is in io.py from cudf in read_csv . The way it handles datetime indicates that we will need to fix this for better handling of timestemp and other datetime data types
            cffi_view = columnview_from_devary(data_ptr, valid_ptr, ffi.NULL)
            newcol = from_cffi_view(cffi_view)
            if (newcol.dtype == np.dtype('datetime64[ms]')):
                gdf_columns.append(newcol.view(DatetimeColumn, dtype='datetime64[ms]'))
            else:
                gdf_columns.append(newcol.view(NumericalColumn, dtype=newcol.dtype))

    gdf = DataFrame()
    for k, v in zip(resultSet.columnNames, gdf_columns):
        assert k != ""
        gdf[k.decode("utf-8")] = v

    resultSet.columns = gdf
    return resultSet, ipchandles

def exceptions_wrapper(f):
    def applicator(*args, **kwargs):
        try:
            f(*args,**kwargs)
        except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
            print(error)
        except Error as error:
            print(str(error))
        except Exception as error:
            print("Unexpected error on " + f.__name__ + ": " + error)
            # Todo: print traceback.print_exc() when debug mode is enabled
    return applicator

#@exceptions_wrapper
def _run_query_get_token(sql, tables):
    startTime = time.time()

    resultToken = 0
    interpreter_path = None
    interpreter_port = None
    calciteTime = 0
    error_message = ''

    try:
        client = _get_client()

        for table, gdf in tables.items():
            _reset_table(client, table, gdf)

        tableGroup = _to_table_group(tables)

        resultToken, interpreter_path, interpreter_port, calciteTime = client.run_dml_query_token(sql, tableGroup)
    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception:
        error_message = "Unexpected error on " + _run_query_get_results.__name__ + ", " + str(error)

    if error_message is not '':
        print(error_message)

    metaToken = {"client" : client, "resultToken" : resultToken, "interpreter_path" : interpreter_path, "interpreter_port" : interpreter_port, "startTime" : startTime, "calciteTime" : calciteTime}
    return metaToken

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

def _private_run_query(sql, tables):
    metaToken = _run_query_get_token(sql, tables)
    return _run_query_get_results(metaToken)
    
    # startTime = time.time()
    # client = _get_client()
    # try:
    #     for table, gdf in tables.items():
    #         _reset_table(client, table, gdf)
    # except Error as err:
    #     print(err)

    # resultSet = None
    # token = 0
    # interpreter_path = None
    # interpreter_port = None
    # try:
    #     tableGroup = _to_table_group(tables)
    #     token, interpreter_path, interpreter_port, calciteTime = client.run_dml_query_token(sql, tableGroup)
    #     resultSet, ipchandles = _private_get_result(token, interpreter_path, interpreter_port, calciteTime)
    #     totalTime = (time.time() - startTime) * 1000  # in milliseconds

    #     return ResultSetHandle(resultSet.columns, token, interpreter_path, interpreter_port, ipchandles, client, calciteTime, resultSet.metadata.time, totalTime)

    # except SyntaxError as error:
    #     raise error
    # except Error as err:
    #     print(err)

    # return None

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
    Gdf = 0
    ParquetFile = 1
    CsvFile = 2


class TableSchema:
    def __init__(self, type, **kwargs):
        schema_type = self._get_schema(**kwargs)
        assert schema_type == type

        self.kwargs = kwargs
        self.schema_type = type
        self.column_names = []
        self.column_types = []
        self.gdf= None

    def set_table_name(self, name):
        self.table_name = name

    def set_column_names(self, names):
        self.column_names = names

    def set_column_types(self, types):
        self.column_types = types

    def set_gdf(self, gdf):
        self.gdf = gdf

    def __hash__(self):
        return hash( (self.schema_type, self.table_name) )

    def __eq__(self, other):
        return self.schema_type == other.schema_type and self.table_name == other.table_name

    def _get_schema(self, **kwargs):
        """
        :param table_name:
        :param kwargs:
                csv: names, dtypes
                gdf: gpu data frame
                parquet: path
        :return:
        """
        column_names = kwargs.get('names', None)
        column_types = kwargs.get('dtypes', None)
        gdf = kwargs.get('gdf', None)
        path = kwargs.get('path', None)

        if column_names is not None and column_types is not None:
            return SchemaFrom.CsvFile
        elif gdf is not None:
            self.gdf = gdf
            return SchemaFrom.Gdf
        elif path is not None:
            return SchemaFrom.ParquetFile

        # schema_logger = logging.getLogger('Schema')
        # schema_logger.critical('Not schema found')



# TODO complete API docs
# WARNING EXPERIMENTAL
def read_csv_table_from_filesystem(table_name, schema):
    print('create csv table')
    error_message = ''

    try:
        client = _get_client()

        resultToken, interpreter_path, interpreter_port = client.run_dml_load_csv_schema(**schema.kwargs)
        resultSet, ipchandles = _private_get_result(resultToken, interpreter_path, interpreter_port, 0)

        return ResultSetHandle(resultSet.columns, resultSet.columnTokens, resultToken, interpreter_path, interpreter_port, ipchandles, client, 0, resultSet.metadata.time, 0, error_message)

    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception as error:
        error_message = "Unexpected error on " + read_csv_table_from_filesystem.__name__ + ", " + str(error)

    if error_message is not '':
        print(error_message)

    return ResultSetHandle(None, None, None, None, None, None, None, None, None, None, error_message)    


def read_parquet_table_from_filesystem(table_name, schema):
    print('create parquet table')
    error_message = ''

    try:
        client = _get_client()

        resultToken, interpreter_path, interpreter_port = client.run_dml_load_parquet_schema(**schema.kwargs)
        resultSet, ipchandles = _private_get_result(resultToken, interpreter_path, interpreter_port, 0)

        return ResultSetHandle(resultSet.columns, resultSet.columnTokens, resultToken, interpreter_path, interpreter_port, ipchandles, client, 0, resultSet.metadata.time, 0, error_message)

    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception as error:
        error_message = "Unexpected error on " + run_query_filesystem.__name__ + ", " + str(error)

    if error_message is not '':
        print(error_message)

    return ResultSetHandle(None, None, None, None, None, None, None, None, None, None, error_message)

    


def create_table(table_name, **kwargs):
    schema = TableSchema(**kwargs)
    return_result = None
    if schema.schema_type == SchemaFrom.CsvFile:  # csv
        return_result = read_csv_table_from_filesystem(table_name, schema)
    elif schema.schema_type == SchemaFrom.ParquetFile: # parquet
        return_result = read_parquet_table_from_filesystem(table_name, schema)
    return_result.name = table_name
    return return_result


def register_table_schema(table_name, **kwargs):
    schema = TableSchema(**kwargs)
    return_result = None
    if schema.schema_type == SchemaFrom.CsvFile:  # csv
        return_result = read_csv_table_from_filesystem(table_name, schema)
    elif schema.schema_type == SchemaFrom.ParquetFile: # parquet
        return_result = read_parquet_table_from_filesystem(table_name, schema)
    else:
        print("ERROR: unknown schema type")

    schema.set_table_name(table_name)
    col_names, types = _get_table_def_from_gdf(return_result.columns)
    schema.set_column_names(col_names)
    schema.set_column_types(types)
    schema.set_gdf(return_result.columns)
    return schema


def register_file_system(authority, type, root, params = None):
    if params is not None:
        params = namedtuple("FileSystemConnection", params.keys())(*params.values())
    client = _get_client()
    schema = FileSystemRegisterRequestSchema(authority, root, type, params)
    request_buffer = MakeRequestBuffer(OrchestratorMessageType.RegisterFileSystem,
                                       client.accessToken,
                                       schema)
    response_buffer = client._send_request( client._orchestrator_path, client._orchestrator_port, request_buffer)
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        raise Error(ResponseErrorSchema.From(response.payload).errors)
    return response.status

def deregister_file_system(authority):
    schema = FileSystemDeregisterRequestSchema(authority)
    client = _get_client()
    request_buffer = MakeRequestBuffer(OrchestratorMessageType.DeregisterFileSystem,
                                       client.accessToken,
                                       schema)
    response_buffer = client._send_request(client._orchestrator_path, client._orchestrator_port, request_buffer)
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        raise Error(ResponseErrorSchema.From(response.payload).errors)
    return response.status

from collections import OrderedDict
def _sql_data_to_table_group(sql_data):
    database_name = 'main'
    tableGroup = OrderedDict([('name', database_name), ('tables', [])])
    blazing_tables = []
    for schema, files in sql_data.items():
        blazing_table = OrderedDict([('name',  database_name + '.' + schema.table_name ), ('columnNames', schema.column_names)])
        if schema.schema_type == SchemaFrom.ParquetFile:
            blazing_table['schemaType'] = FileSchemaType.PARQUET
            blazing_table['parquet'] = schema.kwargs
            blazing_table['csv'] = None
        else:
            blazing_table['schemaType'] = FileSchemaType.CSV
            blazing_table['csv'] = schema.kwargs
            blazing_table['parquet'] = None

        blazing_table['files'] = files
        blazing_tables.append(blazing_table)

    tableGroup['tables'] = blazing_tables
    return tableGroup

#@exceptions_wrapper
def run_query_filesystem(sql, sql_data):
    error_message = ''
    startTime = time.time()

    try:
        client = _get_client()

        for schema, files in sql_data.items():
            _reset_table(client, schema.table_name, schema.gdf)

        resultSet = None
        resultToken = 0
        interpreter_path = None
        interpreter_port = None

        tableGroup = _sql_data_to_table_group(sql_data)
        resultToken, interpreter_path, interpreter_port, calciteTime = client.run_dml_query_filesystem_token(sql, tableGroup)
        resultSet, ipchandles = _private_get_result(resultToken, interpreter_path, interpreter_port, calciteTime)
        totalTime = (time.time() - startTime) * 1000  # in milliseconds

        return ResultSetHandle(resultSet.columns, resultSet.columnTokens, resultToken, interpreter_path, interpreter_port, ipchandles, client, calciteTime,
                                    resultSet.metadata.time, totalTime, '')
    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        error_message = error
    except Error as error:
        error_message = str(error)
    except Exception as error:
        error_message = "Unexpected error on " + run_query_filesystem.__name__ + ", " + str(error)

    if error_message is not '':
        print(error_message)

    return ResultSetHandle(None, None, None, None, None, None, None, None,
                                    None, None, error_message)
