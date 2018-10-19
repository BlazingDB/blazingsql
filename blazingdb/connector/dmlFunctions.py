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
from pygdf.datetime import DatetimeColumn
from pygdf.numerical import NumericalColumn
from pygdf import DataFrame
from pygdf import column
from pygdf import utils

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
  
    def createDataFrameFromResult(resultResponse):
        
        def columnview_from_devary(devary_data, devary_valid, dtype=None):
          return _gdf._columnview(size=devary_data.size, data=_gdf.unwrap_devary(devary_data),
                             mask=_gdf.unwrap_devary(devary_valid), dtype=dtype or devary_data.dtype,
                             null_count=0)
        
        outcols = []
        for i, c in enumerate(resultResponse.columns):
          with cuda.open_ipc_array(c.data, shape=c.size, dtype=_gdf.gdf_to_np_dtype(c.dtype)) as data_ptr:
            with cuda.open_ipc_array(c.valid, shape=utils.calc_chunk_size(c.size, utils.mask_bitsize), dtype=np.int8) as valid_ptr:
              gdf_col = columnview_from_devary(data_ptr, valid_ptr)
              
              newcol = column.Column.from_cffi_view(gdf_col).copy() # we are creating a copy here as a temporary solution to the ipc handles going out of scope
        
              # what is it? is  it required?:  This is because how pygdf interprets datatime data. We may need to revisit this as pygdf evolves
              if newcol.dtype == np.dtype('datetime64[ms]'):
                outcols.append(newcol.view(DatetimeColumn, dtype='datetime64[ms]'))
              else:
                outcols.append(newcol.view(NumericalColumn, dtype=newcol.dtype))
        
        # Build dataframe
        df = DataFrame()
        for k, v in zip(resultResponse.columnNames, outcols):
          df[str(k)] = v #todo chech concat
        print('  dataframe:')
        print(df)
  

    getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
      resultToken=result_token)
    requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
      InterpreterMessage.GetResult, self._access_token, getResultRequest)
    
    responseBuffer = getResultRequestFrom(requestBuffer, '/tmp/ral.socket')
    response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)

    if response.status == Status.Error:
      raise ValueError('Error status')

    resultResponse = blazingdb.protocol.interpreter.GetQueryResultFrom(response.payload)
    
    return queryResult(resultResponse.metadata, createDataFrameFromResult(resultResponse))


  def _getTableGroup(self, input_dataset):
    tableGroup = {}
    tableGroup["name"] = ""
    tableGroup["tables"] = []
    for inputData in input_dataset:
      table = {}
      table["name"] = inputData._table_Name
      table["columns"] = []
      table["columnNames"] = []
      for name, series in inputData._dataFrame._cols.items():
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

  def __init__(self, table_name, dataFrame):
    self._table_Name = table_name
    self._dataFrame = dataFrame
    

class queryResult:
    
  def __init__(self, metadata, dataFrame):
    self._metadata = metadata
    self._dataFrame = dataFrame
