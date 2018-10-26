import pygdf as gd

import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status

from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType
from blazingdb.protocol.gdf import gdf_columnSchema

from libgdf_cffi import ffi
from pygdf.datetime import DatetimeColumn
from pygdf.numerical import NumericalColumn

from pygdf import _gdf
from pygdf import column
from pygdf import numerical
from pygdf import DataFrame
from pygdf.dataframe import Series
from pygdf.buffer import Buffer
from pygdf import utils

from numba import cuda
import numpy as np
import pandas as pd


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
    >>> import pygdf as gd
    >>> import pyblazing
    >>> products = gd.DataFrame({'month': [2, 8, 11], 'sales': [12.1, 20.6, 13.79]})
    >>> cats = gd.DataFrame({'age': [12, 28, 19], 'weight': [5.3, 9, 7.68]})
    >>> tables = {'products': products, 'cats': cats}
    >>> result = pyblazing.run_query('select * from products, cats limit 2', tables)
    >>> type(result)
    pygdf.dataframe.DataFrame
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
    >>> result = pyblazing.run_query('select * from products, cats limit 2', tables)
    >>> type(result)
    pygdf.dataframe.DataFrame
    """

    gdf_tables = {}
    for table, df in tables.items():
        gdf = gd.DataFrame.from_pandas(df)
        gdf_tables[table] = gdf

    return run_query(sql, gdf_tables)


def _lots_of_stuff():
    pass


class PyConnector:
    def __init__(self, orchestrator_path, interpreter_path):
        self._orchestrator_path = orchestrator_path
        self._interpreter_path = interpreter_path

    def connect(self):
        print("open connection")
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
        print(responsePayload.accessToken)
        self.accessToken = responsePayload.accessToken

    def _send_request(self, unix_path, requestBuffer):
        connection = blazingdb.protocol.UnixSocketConnection(unix_path)
        client = blazingdb.protocol.Client(connection)
        return client.send(requestBuffer)

    def run_dml_query(self, query, tableGroup):
        print(query)
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
        print(dmlResponseDTO.resultToken)
        return self._get_result(dmlResponseDTO.resultToken)

    def run_ddl_create_table(self, tableName, columnNames, columnTypes, dbName):
        print('create table: ' + tableName)
        print(columnNames)
        print(columnTypes)
        print(dbName)
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLCreateTableRequestSchema(name=tableName,
                                                                                       columnNames=columnNames,
                                                                                       columnTypes=columnTypes,
                                                                                       dbName=dbName)

        print(dmlRequestSchema)

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
        print(response.status)
        return response.status

    def run_ddl_drop_table(self, tableName, dbName):
        print('drop table: ' + tableName)
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
        print(response.status)
        return response.status

    def close_connection(self):
        print("close connection")
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
        print(response.status)

    def free_result(self, result_token):

        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.FreeResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
            self._interpreter_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')
        print('free result OK!')

    def _get_result(self, result_token):

        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.GetResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
            self._interpreter_path, requestBuffer)

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
    print(cols)
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
            sample_valid_df = gen_data_frame(data_sz, 'valid', np.int8)
            valid_ipch = get_ipc_handle_for(sample_valid_df['valid'])

            blazing_column = {
                'data': data_ipch,
                'valid': valid_ipch,  # TODO we should use valid mask
                'size': data_sz,
                'dtype': dataframe_column._column.cffi_view.dtype,
                'null_count': 0,
                'dtype_info': 0
            }
            blazing_columns.append(blazing_column)

        blazing_table['columns'] = blazing_columns
        blazing_tables.append(blazing_table)

    tableGroup['tables'] = blazing_tables
    return tableGroup


def _get_client():
    client = PyConnector('/tmp/orchestrator.socket', '/tmp/ral.socket')

    try:
        client.connect()
    except Error as err:
        print(err)

    return client


def _private_run_query(sql, tables):
    client = _get_client()

    try:
        for table, gdf in tables.items():
            _reset_table(client, table, gdf)
    except Error as err:
        print(err)

    resultSet = None
    try:
        tableGroup = _to_table_group(tables)
        resultSet = client.run_dml_query(sql, tableGroup)

        print("#RESULT_SET:")

        print('GetResult Response')
        print('  metadata:')
        print('     status: %s' % resultSet.metadata.status)
        print('    message: %s' % resultSet.metadata.message)
        print('       time: %s' % resultSet.metadata.time)
        print('       rows: %s' % resultSet.metadata.rows)
        print('  columnNames: %s' % list(resultSet.columnNames))

#        def columnview_from_devary(devary_data, devary_valid, dtype=None):
#            return _gdf._columnview(size=devary_data.size, data=_gdf.unwrap_devary(devary_data),
#                                    mask=devary_valid, dtype=dtype or devary_data.dtype,
#                                    null_count=0)

#        def from_cffi_view(cffi_view):
#            data_mem, mask_mem = _gdf.cffi_view_to_column_mem(cffi_view)
#            data_buf = Buffer(data_mem)
#            mask = None
#            return column.Column(data=data_buf, mask=mask)

#        for i, c in enumerate(resultSet.columns):
#            assert len(c.data) == 64
#            with cuda.open_ipc_array(c.data, shape=c.size, dtype=_gdf.gdf_to_np_dtype(c.dtype)) as data_ptr:
#                gdf_col = columnview_from_devary(data_ptr, ffi.NULL)
#                newcol = from_cffi_view(gdf_col)

#                outcols = []
#                outcols.append(newcol.view(
#                    NumericalColumn, dtype=newcol.dtype))

#                # Build dataframe
#                df = DataFrame()
#                for k, v in zip(resultSet.columnNames, outcols):
#                    df[str(k)] = v  # todo chech concat
#                print('  dataframe:')
#                print(df)

#        print("#RESULT_SET:")

#        resultSet = client.free_result(123456)

    except Error as err:
        print(err)

    client.close_connection()

    return resultSet


import pyarrow as pa


arr = pa.RecordBatchStreamReader('/gpu.arrow').read_all()
print(arr)
df = arr.to_pandas()
df = df[['swings', 'tractions']]
gdf = gd.DataFrame.from_pandas(df)
gdf._cols["swings"]._column.data.mem.get_ipc_handle()._ipc_handle.handle
# print(gdf.columns)
# print(gdf._cols["swings"]._column.dtype)
# gdf._cols["swings"]._column.cffi_view.size

gdf = gen_data_frame(20, 'swings', np.float32)


table = 'holas'
tables = {table: gdf}

#client = _get_client()
#_reset_table(client, table, gdf)

#result = run_query('select swings, tractions from main.holas', tables)
result = run_query('select swings from main.holas', tables)

print("hi")
