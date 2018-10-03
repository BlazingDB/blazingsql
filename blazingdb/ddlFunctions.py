# coding=utf-8

from collections import namedtuple
import requests
from connection import Connector
import blazindb.protocol

Status = namedtuple(
    'Status', ['valid', 'message', 'access_token']
)

class ddlFunctions:
    def __init__(self, database='', table=''):
        self.database = database
        self.table = table
        
    def createDatabase(self):
        
        Connector.Open(self)

        dbCreationReq = blazingdb.protocol.messages.DatabaseCreationRequest(database)
    
        requestBuffer = dbCreationReq.ToBuffer()
    
        responseBuffer = client.send(requestBuffer)
    
        dbCreationResponse = blazingdb.protocol.messages.DatabaseCreationResponse()

        if dbCreationResponse.hasError():
            raise BlazingDB.CreationError()
    
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
  
        


