import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
import connection

from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType

class ddlFunctions:
    
    def __init__(self, orchestrator_path, database, query):
        self._orchestrator_path = orchestrator_path
        self._interpreter_path = interpreter_path
        
    def runQuery(self, query):
        dmlRequestSchema = blazingdb.protocol.orchestrator.DMLRequestSchema(query=query)
        
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = connection.sendRequest(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(response.payload)
        
        self.getResult(dmlResponseDTO.resultToken)
        
        return dmlResponseDTO.resultToken
        
    def getResult(self, result_token):        
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
          resultToken=result_token)
    
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
          InterpreterMessage.GetResult, self.accessToken, getResultRequest)
    
        responseBuffer = connection.sendRequest(self._interpreter_path, requestBuffer)
    
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
          responseBuffer)
    
        if response.status == Status.Error:
          raise ValueError('Error status')
    
        getResultResponse = \
          blazingdb.protocol.interpreter.GetResultResponseSchema.From(
            response.payload)
        
        return getResultResponse
    
    
