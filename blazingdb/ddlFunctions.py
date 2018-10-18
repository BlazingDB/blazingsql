import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
import connection

from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import OrchestratorMessageType

class ddlFunctions:
    
    def __init__(self, orchestrator_path, access_token):
        self._orchestrator_path = orchestrator_path
        self._access_token = access_token
        
        
#     def createDatabase(self,query):
#         
#         dmlRequestSchema = blazingdb.protocol.orchestrator.DDLRequestSchema(query=query)
#         requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL,
#                                                                                self._access_token, dmlRequestSchema)
#         responseBuffer = connection.sendRequest(self._orchestrator_path, requestBuffer)
#         response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
#         
#         if response.status == Status.Error:
#           errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
#           raise Error(errorResponse.errors)
#                 
#         return response.status
#     
#     def dropDatabase(self):
#         
#         Connector.Open(self)
# 
#         dbCreationReq = blazingdb.protocol.messages.DatabaseDropRequest(database)
#     
#         requestBuffer = dbCreationReq.ToBuffer()
#     
#         responseBuffer = client.send(requestBuffer)
#     
#         dbCreationResponse = blazingdb.protocol.messages.DatabaseDropResponse()
# 
#         if dbCreationResponse.hasError():
#             raise BlazingDB.CreationError()
    
    def createTable(self, tableName, columnNames, columnTypes, dbName = 'main'):
        
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLCreateTableRequestSchema(name=tableName, columnNames=columnNames, columnTypes=columnTypes, dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_CREATE_TABLE,
                                                                               self._access_token, dmlRequestSchema)
        responseBuffer = Connection.sendRequest(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          raise Error(errorResponse.errors)
        return response.status
    
    def dropTable(self, tableName, dbName = 'main'):

        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLDropTableRequestSchema(name=tableName, dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_DROP_TABLE,
                                                                               self._access_token, dmlRequestSchema)
        responseBuffer = Connection.sendRequest(self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)
        if response.status == Status.Error:
          errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
          raise Error(errorResponse.errors)
        return response.status
    
class CreationError(ValueError):
    pass
  
        


