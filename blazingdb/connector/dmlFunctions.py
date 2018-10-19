import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType
import numpy as np
from numba import cuda
from pygdf import _gdf
from pygdf.buffer import Buffer
from pygdf.categorical import CategoricalColumn
from pygdf.datetime import DatetimeColumn
from pygdf.numerical import NumericalColumn
from pygdf import DataFrame
from pygdf import column

class QueryResult :
  def __init(self, cr):
    self.cr = cr;

  def __enter__(self):
    self.cr.save()
    return self.cr

  def __exit(self, type, value, traceback):
    self.cr.restore()

class dmlFunctions:

  def __init__(self, connection):
    self.connection = connection
    self._access_token = self.connection.accessToken

  def runQuery(self, query, input_dataset):
    tableGroup = self._getTableGroup(input_dataset)
    dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDMLRequestSchema(query, tableGroup)
    requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML,
                                                                           self._access_token, dmlRequestSchema)
    responseBuffer = self.connection.sendRequest(requestBuffer)
    response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)

    if response.status == Status.Error:
      errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
      raise Error(errorResponse.errors)
    dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(response.payload)

    return dmlResponseDTO.resultToken


  def getResult(self, result_token):
    def getResultRequestFrom(requestBuffer, interpreter_path):
      connection = blazingdb.protocol.UnixSocketConnection(interpreter_path)
      client = blazingdb.protocol.Client(connection)
      return client.send(requestBuffer)

    getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
      resultToken=result_token)
    requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
      InterpreterMessage.GetResult, self._access_token, getResultRequest)
    print('1 get result')
    responseBuffer = getResultRequestFrom(requestBuffer, '/tmp/ral.socket')
    print('2 get result')
    response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)

    if response.status == Status.Error:
      raise ValueError('Error status')

    resultResponse = blazingdb.protocol.interpreter.GetQueryResultFrom(response.payload)

    return resultResponse

  def _getTableGroup(self, input_dataset):
    tableGroup = {}
    tableGroup["name"] = ""
    tableGroup["tables"] = []
    for inputData in input_dataset:
      table = {}
      table["name"] = inputData._table_Name
      table["columns"] = []
      table["columnNames"] = []
      for name, series in inputData._gdfDataFrame._cols.items():
        #
        table["columnNames"].append(name)

        cffiView = series._column.cffi_view
        if series._column._mask is None:
          table["columns"].append(
            {'data': bytes(series._column._data.mem.get_ipc_handle()._ipc_handle.handle),
             'valid': b"",
             'size': cffiView.size,
             'dtype': cffiView.dtype,
             'dtype_info': 0,  # TODO dtype_info is currently not used in pygdf
             'null_count': cffiView.null_count})
        else:
          table["columns"].append(
            {'data': bytes(series._column._data.mem.get_ipc_handle()._ipc_handle.handle),
             'valid': bytes(series._column._mask.mem.get_ipc_handle()._ipc_handle.handle),
             'size': cffiView.size,
             'dtype': cffiView.dtype,
             'dtype_info': 0,  # TODO dtype_info is currently not used in pygdf
             'null_count': cffiView.null_count})

      tableGroup["tables"].append(table)
    return tableGroup


class inputData:

  def __init__(self, table_name, gdfDataFrame):
    self._table_Name = table_name
    self._gdfDataFrame = gdfDataFrame
