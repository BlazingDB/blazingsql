import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status

from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType

from blazingdb.protocol.gdf import gdf_columnSchema
import pycuda.gpuarray as gpuarray
import pycuda.driver as cuda
import numpy


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
    print(tableName)
    dmlRequestSchema = blazingdb.protocol.orchestrator.DDLCreateTableRequestSchema(name=tableName, columnNames=columnNames, columnTypes=columnTypes, dbName=dbName)
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
    print(tableName)
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


def create_sample_device_data():
  a = numpy.random.randn(1, 32) + 6
  a = a.astype(numpy.int8)
  print('orig: ', a)
  a_gpu = cuda.mem_alloc(a.size * a.dtype.itemsize)
  cuda.memcpy_htod(a_gpu, a)
  print("size: " + str(a.size))
  return a_gpu, a.size

def main():

  client = PyConnector('/tmp/orchestrator.socket', '/tmp/ral.socket')

  cuda.init()
  dev = cuda.Device(0)
  ctx_gpu = dev.make_context()

  try:
    client.connect()
  except Error as err:
    print(err)

  try:
    client.run_ddl_create_table('nation', ['id'], ['GDF_INT8'], 'main')
  except Error as err:
    print(err)

  data_gpu, data_sz = create_sample_device_data()
  data_handler = bytes(cuda.mem_get_ipc_handle(data_gpu))
  valid_gpu, data_sz = create_sample_device_data()
  valid_handler = bytes(cuda.mem_get_ipc_handle(valid_gpu))

  try:
    tableGroup = {
      'tables': [
        {
          'name': 'main.nation',
          'columns': [{'data': data_handler, 'valid': valid_handler, 'size': data_sz, 'dtype': 1, 'null_count': 0, 'dtype_info': 0}],
          'columnNames': ['id']
        }
      ],
      'name': 'main',
    }
    resultSet = client.run_dml_query('select id > 5 from main.nation', tableGroup)

    print("#RESULT_SET:")
    print('GetResult Response')
    print('  metadata:')
    print('     status: %s' % resultSet.metadata.status)
    print('    message: %s' % resultSet.metadata.message)
    print('       time: %s' % resultSet.metadata.time)
    print('       rows: %s' % resultSet.metadata.rows)
    print('  columnNames: %s' % list(resultSet.columnNames))
    for i, column in enumerate(resultSet.columns):
      x_ptr = cuda.IPCMemoryHandle(column.data)  # x_ptr: device raw pointer
      x_gpu = gpuarray.GPUArray((1, column.size), numpy.int8, gpudata=x_ptr)
      print('\tgpu:  ', x_gpu.get())
    print("#RESULT_SET:")

    resultSet = client.free_result(123456)

  except Error as err:
    print(err)

  # try:
  #   client.run_ddl_drop_table('User', 'main')
  # except Error as err:
  #   print(err)

  client.close_connection()
  ctx_gpu.pop()

if __name__ == '__main__':
  main()