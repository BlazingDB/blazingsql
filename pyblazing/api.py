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
    token = None
    interpreter_path = None # interpreter_path is a string with this format "ip:port"
    handle = None
    client = None

    def __init__(self, columns, token, interpreter_path, handle, client, calciteTime, ralTime, totalTime):
        self.columns = columns

        if hasattr(self.columns, '__iter__'):
            for col in self.columns:
                col.token = token
        else:
            self.columns.token = token

        self.token = token
        self.interpreter_path = interpreter_path
        self.handle = handle
        self.client = client
        self.calciteTime = calciteTime
        self.ralTime = ralTime
        self.totalTime = totalTime

    def __del__(self):
        # @todo
        # for ipch in self.handle:
        #    ipch.close()
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

    # connection_path is a string with this format "ip:port"
    def _send_request(self, connection_path, requestBuffer):
        ip, port = connection_path.split(":")
        connection = blazingdb.protocol.TcpSocketConnection(ip, int(port))
        client = blazingdb.protocol.Client(connection)
        return client.send(requestBuffer)

    def run_dml_load_parquet_schema(self, path):
        print('load parquet file')
        ## todo use rowGroupIndices, and columnIndices, someway??
        requestSchema = blazingdb.protocol.io.ParquetFileSchema(path=path, rowGroupIndices=[], columnIndices=[])

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.LoadParquet,
                                                                               self.accessToken, requestSchema)
        responseBuffer = self._send_request(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            if b'SqlSyntaxException' in errorResponse.errors:
                raise SyntaxError(errorResponse.errors.decode('utf-8'))
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path

    def run_dml_load_csv_schema(self, path, names, dtypes, delimiter = '|', line_terminator='\n', skip_rows=0):
        print('load csv file')
        requestSchema = blazingdb.protocol.io.CsvFileSchema(path=path, delimiter=delimiter, lineTerminator = line_terminator, skipRows=skip_rows, names=names, dtypes=dtypes)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.LoadCSV,
                                                                                self.accessToken, requestSchema)
        responseBuffer = self._send_request(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            if b'SqlSyntaxException' in errorResponse.errors:
                raise SyntaxError(errorResponse.errors.decode('utf-8'))
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path

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
        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path.decode('utf8'), dmlResponseDTO.calciteTime

    def run_dml_query_filesystem_token(self, query, tableGroup):
        dmlRequestSchema = blazingdb.protocol.io.BuildFileSystemDMLRequestSchema(query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML_FS,
                                                                               self.accessToken, dmlRequestSchema)
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


def get_ipc_handle_for(gdf, dataframe_column):
    cffiView = dataframe_column._column.cffi_view

    #@todo deep_copy
    #if hasattr(gdf, 'token'):
    #   dataframe_column._column._data = dataframe_column._column._data.copy()

    ipch = dataframe_column._column._data.mem.get_ipc_handle()
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
    cols = gdf.columns.values.tolist() ## todo las propiedades, solo una columna!

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

            data_ipch = get_ipc_handle_for(gdf, dataframe_column)

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


def _get_client_internal(orchestrator_ip, orchestrator_port):
    orchestrator_path = "%s:%s" % (orchestrator_ip, orchestrator_port)
    client = PyConnector(orchestrator_path)

    try:
        client.connect()
    except Error as err:
        print(err)

    return client

__orchestrator_ip = "127.0.0.1"
__orchestrator_port = "8890"
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

def _private_get_result(token, interpreter_path, calciteTime):
    client = _get_client()
    resultSet = client._get_result(token, interpreter_path)

    gdf_columns = []
    ipchandles = []
    for i, c in enumerate(resultSet.columns):
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

    return resultSet, ipchandles, gdf

def _private_run_query(sql, tables):

    startTime = time.time()
    client = _get_client()
    try:
        for table, gdf in tables.items():
            _reset_table(client, table, gdf)
    except Error as err:
        print(err)

    resultSet = None
    token = None
    interpreter_path = None
    try:
        tableGroup = _to_table_group(tables)
        token, interpreter_path, calciteTime = client.run_dml_query_token(sql, tableGroup)
        resultSet, ipchandles, gdf_out = _private_get_result(token, interpreter_path, calciteTime)
        resultSet.columns = gdf_out
        totalTime = (time.time() - startTime) * 1000  # in milliseconds
    except SyntaxError as error:
        raise error
    except Error as err:
        print(err)

    return_result = ResultSetHandle(resultSet.columns, token, interpreter_path, ipchandles, client, calciteTime, resultSet.metadata.time, totalTime)
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
    client = _get_client()

    token, interpreter_path = client.run_dml_load_csv_schema(**schema.kwargs)
    resultSet, ipchandles, gdf = _private_get_result(token, interpreter_path, 0)
    return_result = ResultSetHandle(resultSet.columns, token, interpreter_path, ipchandles, client, 0, resultSet.metadata.time, 0)
    return gdf


def read_parquet_table_from_filesystem(table_name, schema):
    print('create parquet table')
    client = _get_client()

    token, interpreter_path = client.run_dml_load_parquet_schema(**schema.kwargs)
    resultSet, ipchandles, gdf = _private_get_result(token, interpreter_path, 0)
    return_result = ResultSetHandle(resultSet.columns, token, interpreter_path, ipchandles, client, 0, resultSet.metadata.time, 0)
    return gdf


def register_table_schema(table_name, **kwargs):
    schema = TableSchema(**kwargs)
    return_result = None
    if schema.schema_type == SchemaFrom.CsvFile:  # csv
        gdf = read_csv_table_from_filesystem(table_name, schema)
    elif schema.schema_type == SchemaFrom.ParquetFile: # parquet
        gdf = read_parquet_table_from_filesystem(table_name, schema)
    else:
        gdf = schema.gdf
        #todo remove this option

    print('create table with ')
    print(gdf)

    schema.set_table_name(table_name)
    col_names, types = _get_table_def_from_gdf(gdf)
    schema.set_column_names(col_names)
    schema.set_column_types(types)
    schema.set_gdf(gdf)
    return schema


def register_file_system(authority, type, root, params = None):
    if params is not None:
        params = namedtuple("FileSystemConnection", params.keys())(*params.values())
    client = _get_client()
    schema = FileSystemRegisterRequestSchema(authority, root, type, params)
    request_buffer = MakeRequestBuffer(OrchestratorMessageType.RegisterFileSystem,
                                       client.accessToken,
                                       schema)
    response_buffer = client._send_request( client._orchestrator_path, request_buffer)
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
    response_buffer = client._send_request(client._orchestrator_path, request_buffer)
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

def run_query_filesystem(sql, sql_data):
    startTime = time.time()

    client = _get_client()

    for schema, files in sql_data.items():
        _reset_table(client, schema.table_name, schema.gdf)


    resultSet = None
    token = None
    interpreter_path = None
    try:
        tableGroup = _sql_data_to_table_group(sql_data)
        token, interpreter_path, calciteTime = client.run_dml_query_filesystem_token(sql, tableGroup)
        resultSet, ipchandles, gdf_out = _private_get_result(token, interpreter_path, calciteTime)
        resultSet.columns = gdf_out
        totalTime = (time.time() - startTime) * 1000  # in milliseconds
    except SyntaxError as error:
        raise error
    except Error as err:
        print(err)
        raise err

    return_result = ResultSetHandle(resultSet.columns, token, interpreter_path, ipchandles, client, calciteTime,
                                    resultSet.metadata.time, totalTime)
    return return_result
