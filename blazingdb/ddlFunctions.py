import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
import connection

from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import OrchestratorMessageType

class ddlFunctions:
    
    def __init__(self, orchestrator_path, query):
        self._orchestrator_path = orchestrator_path
        self.query = query
        
    def createDatabase(self,query):
        
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLRequestSchema(query=query)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = connection.sendRequest(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          raise Error(errorResponse.errors)
        print(response.status)
        return response.status
    
    def dropDatabase(self):
        
        Connector.Open(self)

        dbCreationReq = blazingdb.protocol.messages.DatabaseDropRequest(database)
    
        requestBuffer = dbCreationReq.ToBuffer()
    
        responseBuffer = client.send(requestBuffer)
    
        dbCreationResponse = blazingdb.protocol.messages.DatabaseDropResponse()

        if dbCreationResponse.hasError():
            raise BlazingDB.CreationError()
    
    def createTable(self):
        
        Connector.Open(self)

        requestBuffer = struct.pack('ss', 'type=createTbl', self.database, self.table)

        accessToken = client.send(requestBuffer)        
        
        Connector.Close(self)
        
        return Status(True, '', accessToken)
    
    def dropTable(self):

        Connector.Open(self)

        requestBuffer = struct.pack('ss', 'type=dropTbl', self.database, self.table)

        accessToken = client.send(requestBuffer)        
        
        Connector.Close(self)
        
        return Status(True, '', accessToken)
    
class CreationError(ValueError):
    pass
  
        


