import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
import blazingdb.connector.connection

from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import OrchestratorMessageType

class ddlFunctions:
    
    def __init__(self, connection):
        self.connection = connection
        self._access_token = self.connection.accessToken

    def createTable(self, tableName, columnNames, columnTypes, dbName = 'main'):
        
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLCreateTableRequestSchema(name=tableName, columnNames=columnNames, columnTypes=columnTypes, dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_CREATE_TABLE,
                                                                               self._access_token, dmlRequestSchema)
        responseBuffer = self.connection.sendRequest(requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          raise Error(errorResponse.errors)
        return response.status
    
    def dropTable(self, tableName, dbName = 'main'):
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLDropTableRequestSchema(name=tableName, dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_DROP_TABLE,
                                                                               self._access_token, dmlRequestSchema)
        responseBuffer = self.connection.sendRequest(requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          raise Error(errorResponse.errors)
        return response.status
    
class CreationError(ValueError):
    pass
  
        


