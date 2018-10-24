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


class PyConnector:
    def __init__(self, orchestrator_path, interpreter_path):
        self._orchestrator_path = orchestrator_path
        self._interpreter_path = interpreter_path

    def connect(self):
        print("open connection")
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthOpen, authSchema)

        responseBuffer = self._send_request(self._orchestrator_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
            print(errorResponse.errors)
            raise Error(errorResponse.errors)
        responsePayload = blazingdb.protocol.orchestrator.AuthResponseSchema.From(response.payload)
        print(responsePayload.accessToken)
        self.accessToken = responsePayload.accessToken

    def _send_request(self, unix_path, requestBuffer):
        connection = blazingdb.protocol.UnixSocketConnection(unix_path)
        client = blazingdb.protocol.Client(connection)
        return client.send(requestBuffer)

    def run_dml_query(self, query, tableGroup):
        print(query)
        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDMLRequestSchema(query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(response.payload)
        print(dmlResponseDTO.resultToken)
        return self._get_result(dmlResponseDTO.resultToken)

    def run_ddl_create_table(self, tableName, columnNames, columnTypes, dbName):
        print('create table: ' + tableName)
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLCreateTableRequestSchema(name=tableName,
                                                                                       columnNames=columnNames,
                                                                                       columnTypes=columnTypes,
                                                                                       dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_CREATE_TABLE,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
            raise Error(errorResponse.errors)
        print(response.status)
        return response.status

    def run_ddl_drop_table(self, tableName, dbName):
        print('drop table: ' + tableName)
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLDropTableRequestSchema(name=tableName, dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_DROP_TABLE,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
            raise Error(errorResponse.errors)
        print(response.status)
        return response.status

    def close_connection(self):
        print("close connection")
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthClose, authSchema)

        responseBuffer = self._send_request(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
            print(errorResponse.errors)
        print(response.status)

    def free_result(self, result_token):

        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.FreeResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(self._interpreter_path, requestBuffer)

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

        responseBuffer = self._send_request(self._interpreter_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        queryResult = blazingdb.protocol.interpreter.GetQueryResultFrom(response.payload)
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


def main():
    client = PyConnector('/tmp/orchestrator.socket', '/tmp/ral.socket')

    try:
        client.connect()
    except Error as err:
        print(err)

    try:
        client.run_ddl_drop_table('nation', 'main')
        client.run_ddl_create_table('nation', ['id'], ['GDF_INT32'], 'main')
    except Error as err:
        print(err)

    data_sz = 25
    dtype = np.int8

    sample_data_df = gen_data_frame(data_sz, 'data', dtype)
    sample_valid_df = gen_data_frame(data_sz, 'valid', np.int8)

    data_ipch = get_ipc_handle_for(sample_data_df['data'])
    valid_ipch = get_ipc_handle_for(sample_valid_df['valid'])

    try:
        tableGroup = {
            'tables': [
                {
                    'name': 'main.nation',
                    'columns': [
                        {'data': data_ipch, 'valid': valid_ipch, 'size': data_sz, 'dtype': _gdf.np_to_gdf_dtype(dtype), 'null_count': 0,
                         'dtype_info': 0}],
                    'columnNames': ['id']
                }
            ],
            'name': 'main',
        }
        resultSet = client.run_dml_query('select id from main.nation', tableGroup)

        print("#RESULT_SET:")
        print('GetResult Response')
        print('  metadata:')
        print('     status: %s' % resultSet.metadata.status)
        print('    message: %s' % resultSet.metadata.message)
        print('       time: %s' % resultSet.metadata.time)
        print('       rows: %s' % resultSet.metadata.rows)
        print('  columnNames: %s' % list(resultSet.columnNames))

        def createDataFrameFromResult(resultResponse):
            def columnview_from_devary(devary_data, devary_valid, dtype=None):
                return _gdf._columnview(size=devary_data.size, data=_gdf.unwrap_devary(devary_data),
                                        mask=_gdf.unwrap_devary(devary_valid), dtype=dtype or devary_data.dtype,
                                        null_count=0)

            def from_cffi_view(cffi_view):
                data_mem, mask_mem = _gdf.cffi_view_to_column_mem(cffi_view)
                data_buf = Buffer(data_mem)
                mask = None
                return column.Column(data=data_buf, mask=mask)

            for i, c in enumerate(resultResponse.columns):
                with cuda.open_ipc_array(c.data, shape=c.size, dtype=_gdf.gdf_to_np_dtype(c.dtype)) as data_ptr:
                    if len(c.valid) == 0:
                        gdf_col = columnview_from_devary(data_ptr, ffi.NULL)
                        newcol = from_cffi_view(gdf_col)

                        outcols = []
                        outcols.append(newcol.view(NumericalColumn, dtype=newcol.dtype))

                        # Build dataframe
                        df = DataFrame()
                        for k, v in zip(resultResponse.columnNames, outcols):
                            df[str(k)] = v  # todo chech concat
                        print('  dataframe:')
                        print(df)
                    else:
                        with cuda.open_ipc_array(c.valid, shape=utils.calc_chunk_size(c.size, utils.mask_bitsize),
                                                 dtype=np.int8) as valid_ptr:
                            gdf_col = columnview_from_devary(data_ptr, valid_ptr)
                            newcol = column.Column.from_cffi_view(gdf_col)

                            outcols = []
                            outcols.append(newcol.view(NumericalColumn, dtype=newcol.dtype))

                            # Build dataframe
                            df = DataFrame()
                            for k, v in zip(resultResponse.columnNames, outcols):
                                df[str(k)] = v  # todo chech concat
                            print('  dataframe:')
                            print(df)

        createDataFrameFromResult(resultSet)
        print("#RESULT_SET:")

        resultSet = client.free_result(123456)

    except Error as err:
        print(err)

    client.close_connection()


if __name__ == '__main__':
    main()
