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
from cudf.utils.utils import calc_chunk_size, mask_bitsize

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
    token = None
    interpreter_path = None
    handle = None
    client = None

    def __init__(self,columns, token, interpreter_path, handle, client, calciteTime, ralTime, totalTime):
        self.columns = columns
        self.token = token
        self.interpreter_path = interpreter_path
        self.handle = handle
        self.client = client
        self.calciteTime = calciteTime
        self.ralTime = ralTime
        self.totalTime = totalTime

    def __del__(self):
        del self.handle
        self.client.free_result(self.token,self.interpreter_path)

    def __str__(self):
      return ('''columns = %(columns)s
token = %(token)s
interpreter_path = %(interpreter_path)s
handle = %(handle)s
client = %(client)s
calciteTime = %(calciteTime)d
ralTime = %(ralTime)d
totalTime = %(totalTime)d''' % {
        'columns': self.columns,
        'token': self.token,
        'interpreter_path': self.interpreter_path,
        'handle': self.handle,
        'client': self.client,
        'calciteTime' : self.calciteTime,
        'ralTime' : self.ralTime,
        'totalTime' : self.totalTime,
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
    def __init__(self, orchestrator_path):
        self._orchestrator_path = orchestrator_path

    def __del__(self):
        self.close_connection()

    def connect(self):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("open connection")
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthOpen, authSchema)

        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)

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

    def _send_request(self, unix_path, requestBuffer):
        connection = blazingdb.protocol.UnixSocketConnection(unix_path)
        client = blazingdb.protocol.Client(connection)
        return client.send(requestBuffer)

    def run_dml_query_token(self, query, tableGroup):
        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDMLRequestSchema(query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML, self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            if b'SqlSyntaxException' in errorResponse.errors:
                raise SyntaxError(errorResponse.errors.decode('utf-8'))
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path, dmlResponseDTO.calciteTime

    def run_dml_query(self, query, tableGroup):
        # TODO find a way to print only for debug mode (add verbose arg)
        # print(query)
        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDMLRequestSchema(
            query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return self._get_result(dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path)

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
            self._orchestrator_path, requestBuffer)
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
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

        return response.status

    def close_connection(self):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("close connection")

        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthClose, authSchema)

        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            print(errorResponse.errors)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

    def free_result(self, result_token, interpreter_path):
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.FreeResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
            interpreter_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        # TODO find a way to print only for debug mode (add verbose arg)
        #print('free result OK!')

    def _get_result(self, result_token, interpreter_path):
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.GetResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
            interpreter_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        queryResult = blazingdb.protocol.interpreter.GetQueryResultFrom(
            response.payload)

        return queryResult


def gen_data_frame(nelem, name, dtype):
    pdf = pd.DataFrame()
    pdf[name] = np.arange(nelem, dtype=dtype)
    df = DataFrame.from_pandas(pdf)
    return df


def get_ipc_handle_for(df):
    cffiView = df._column.cffi_view
    ipch = df._column._data.mem.get_ipc_handle()
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
    cols = gdf.columns.values.tolist()

    # TODO find a way to print only for debug mode (add verbose arg)
    # print(cols)

    types = []
    for key, column in gdf._cols.items():
        dtype = column._column.cffi_view.dtype
        types.append(gdf_column_type_to_str(dtype))
    return cols, types


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

        for column in gdf.columns:
            dataframe_column = gdf._cols[column]
            # TODO support more column types
            numerical_column = dataframe_column._column
            data_sz = numerical_column.cffi_view.size
            dtype = numerical_column.cffi_view.dtype

            data_ipch = get_ipc_handle_for(dataframe_column)

            # TODO this valid data is fixed and is invalid
	    #felipe doesnt undertand why we need this we can send null
	    #if the bitmask is not valid
            #sample_valid_df = gen_data_frame(data_sz, 'valid', np.int8)
            #valid_ipch = get_ipc_handle_for(sample_valid_df['valid'])

            blazing_column = {
                'data': data_ipch,
                'valid': None,  # TODO we should use valid mask
                'size': data_sz,
                'dtype': dataframe_column._column.cffi_view.dtype,
                'null_count': dataframe_column._column.cffi_view.null_count,
                'dtype_info': 0
            }
            blazing_columns.append(blazing_column)

        blazing_table['columns'] = blazing_columns
        blazing_tables.append(blazing_table)

    tableGroup['tables'] = blazing_tables
    return tableGroup


def _get_client_internal():
    client = PyConnector('/tmp/orchestrator.socket')

    try:
        client.connect()
    except Error as err:
        print(err)

    return client



__blazing__global_client = _get_client_internal()

def _get_client():
    return __blazing__global_client

def _private_run_query(sql, tables):
    
    startTime = time.time()
    
    client = _get_client()

    try:
        for table, gdf in tables.items():
            _reset_table(client, table, gdf)
    except Error as err:
        print(err)
    ipchandles = []
    resultSet = None
    token = None
    interpreter_path = None
    try:
        tableGroup = _to_table_group(tables)
        token, interpreter_path, calciteTime = client.run_dml_query_token(sql, tableGroup)
        resultSet = client._get_result(token, interpreter_path)

        # TODO: this function was copied from column.py in cudf  but fixed so that it can handle a null mask. cudf has a bug there 
        def from_cffi_view(cffi_view):
            """Create a Column object from a cffi struct gdf_column*.
            """
            data_mem, mask_mem = _gdf.cffi_view_to_column_mem(cffi_view)
            data_buf = Buffer(data_mem)
 
            if mask_mem is not None:
                mask_buf = Buffer(mask_mem)
            else:
                mask_buf = None
 
            return Column(data=data_buf, mask=mask_buf)

        # TODO: this function was copied from _gdf.py in cudf  but fixed so that it can handle a null mask. cudf has a bug there 
        def columnview_from_devary(data_devary, mask_devary, dtype=None):
            return _gdf._columnview(size=data_devary.size,  data=_gdf.unwrap_devary(data_devary),
                       mask=_gdf.unwrap_mask(mask_devary)[0] if mask_devary is not None else ffi.NULL, dtype=dtype or data_devary.dtype,
                       null_count=0)

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

        gdf_columns = []

        for i, c in enumerate(resultSet.columns):
            assert len(c.data) == 64
            ipch, data_ptr = _open_ipc_array(
                c.data, shape=c.size, dtype=_gdf.gdf_to_np_dtype(c.dtype))
            ipchandles.append(ipch)
            
            valid_ptr = None
            if(c.null_count > 0):
                ipch, valid_ptr = _open_ipc_array(
                    c.valid, shape=calc_chunk_size(c.size, mask_bitsize), dtype=np.int8)
                ipchandles.append(ipch)

            # TODO: this code imitates what is in io.py from cudf in read_csv . The way it handles datetime indicates that we will need to fix this for better handling of timestemp and other datetime data types
            cffi_view = columnview_from_devary(data_ptr, valid_ptr, ffi.NULL)
            newcol = from_cffi_view(cffi_view)
            if(newcol.dtype == np.dtype('datetime64[ms]')):
                gdf_columns.append(newcol.view(DatetimeColumn, dtype='datetime64[ms]'))
            else:
                gdf_columns.append(newcol.view(NumericalColumn, dtype=newcol.dtype))

        df = DataFrame()
        for k, v in zip(resultSet.columnNames, gdf_columns):
            df[str(k)] = v

        resultSet.columns = df
        
        totalTime = (time.time() - startTime) * 1000  # in milliseconds

        # @todo close ipch, see one solution at ()
        # print(df)
        # for ipch in ipchandles:
        #     ipch.close()
    except SyntaxError as error:
        raise error
    except Error as err:
        print(err)

    return_result = ResultSetHandle(resultSet.columns, token, interpreter_path, ipchandles, client, calciteTime, resultSet.metadata.time, totalTime)
    return return_result
