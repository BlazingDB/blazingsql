import blazingdb.protocol
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import OrchestratorMessageType

class Connection:
    
    def __init__(self, orchestrator_path, interpreter_path):
        self._orchestrator_path = orchestrator_path
    
    def sendRequest(self, unix_path, requestBuffer):
        connection = blazingdb.protocol.UnixSocketConnection(unix_path)
        client = blazingdb.protocol.Client(connection)
        return client.send(requestBuffer)
    
    def open(self):
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()
        
        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
          OrchestratorMessageType.AuthOpen, authSchema)
        
        responseBuffer = self.sendRequest(self._orchestrator_path, requestBuffer)
        
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          print(errorResponse.errors)
        else:
          responsePayload = blazingdb.protocol.orchestrator.AuthResponseSchema.From(response.payload)
          print(responsePayload.accessToken)
          self.accessToken = responsePayload.accessToken
     
    def close(self):
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()
    
        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
          OrchestratorMessageType.AuthClose, authSchema)
    
        responseBuffer = self.sendRequest(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          print(errorResponse.errors)
          
        self._orchestrator_path = None
        
        print(response.status)

    