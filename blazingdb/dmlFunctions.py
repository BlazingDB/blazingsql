import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from connection import Connection

from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType

import pycuda.driver as drv

class dmlFunctions:
    
    def __init__(self, orchestrator_path, interpreter_path, query, accessToken):
        self._orchestrator_path = orchestrator_path
        self._interpreter_path = interpreter_path
        self._access_token = accessToken
        
    def runQuery(self, query, input_dataset):
        
        dev = drv.Device(0)

        tableGroup["name"] = ""
        tableGroup["tables"] = []
        for inputData in input_dataset:
            table["name"] = inputData._table_Name
            table["columns"] = []
            table["columnNames"] = []
            for name, series in df._cols.items():
#             WSM TODO add if statement for valid != nullptr
                table["columnNames"].append(name)
                table["columns"].append({'data': drv.mem_get_ipc_handle(series._column.cffi_view.data),
                                          'valid': drv.mem_get_ipc_handle(series._column.cffi_view.valid),
                                           'size': series._column.cffi_view.size,
                                            'dtype': series._column.cffi_view.dtype,
                                             'dtype_info': series._column.cffi_view.dtype_info})
            
            
        
        dmlRequestSchema = blazingdb.protocol.orchestrator.DMLRequestSchema(query=query, tableGroup=tableGroup)
        
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML,
                                                                               self._access_token, dmlRequestSchema)
        responseBuffer = Connection.sendRequest(self, self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(response.payload)
                
        return dmlResponseDTO.resultToken
        
    def getResult(self, result_token):        
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
          resultToken=result_token)
    
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
          InterpreterMessage.GetResult, self._access_token, getResultRequest)
    
        responseBuffer = Connection.sendRequest(self, self._interpreter_path, requestBuffer)
    
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
          responseBuffer)
    
        if response.status == Status.Error:
          raise ValueError('Error status')
    
        getResultResponse = \
          blazingdb.protocol.interpreter.GetResultResponseSchema.From(
            response.payload)
        
#         rrr=self.interpreteResult(getResultResponse)
        
        return getResultResponse
    
#     def interpreteResult(self):
#         self.getResult(dmlResponseDTO.resultToken)


class inputData:
    
    def __init__(self, table_name, gdfDataFrame):
        self._table_Name = table_name
        self._gdfDataFrame = gdfDataFrame
        
        
