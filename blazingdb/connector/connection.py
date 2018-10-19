import blazingdb.protocol
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from  blazingdb.connector.errors import ConnectionError

from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import OrchestratorMessageType

class Connection:
    def __init__(self, orchestrator_path):
        self.connection = blazingdb.protocol.UnixSocketConnection(orchestrator_path)
        self.client = blazingdb.protocol.Client(self.connection)
        self.accessToken = None

    def sendRequest(self, requestBuffer):
        return self.client.send(requestBuffer)
    
    def open(self):
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()
         
        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
          OrchestratorMessageType.AuthOpen, authSchema)
        try:
            responseBuffer = self.sendRequest(requestBuffer)
        except ConnectionError as err:
            print(err)
         
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
        else:
          responsePayload = blazingdb.protocol.orchestrator.AuthResponseSchema.From(response.payload)
          self.accessToken = responsePayload.accessToken
     
        return responsePayload.accessToken 
      
    def close(self):
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()
     
        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
          OrchestratorMessageType.AuthClose, authSchema)
     
        responseBuffer = self.sendRequest(requestBuffer)
         
        try:
            response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        except ConnectionError as err:
            print(err)
             
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
           
        print(response.status)
        self.connection = None
        self.client = None

    