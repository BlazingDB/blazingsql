# coding=utf-8

from collections import namedtuple
import requests
from connection import Connector
from blazindb.protocol.interpreter import MessageType as InterpreterMessageType

class param:
    def __init__(self, database, query):
        self.database = database
        self.query = query

class ddlFunctions:
    def __init__(self, param):
        self.param = param

    def query(self):        
        
        connection = blazingdb.protocol.UnixSocketConnection('/tmp/socket')
        client = blazingdb.protocol.client(connection)
        
        self.query = 'select * from tabla'
        
        requestBuffer = blazingdb.protocol.orchestrator.MakeDMLRequest(self.query)
        
        responseBuffer = client.send(requestBuffer)
        
        response = blazingdb.protocol.orchestrator.DMLResponseFrom(responseBuffer)
        
        print response.payload.token
        
        return response.payload.token
        
    def getResult(self):
        
        connection = blazingdb.protocol.UnixSocketConnection("/tmp/socket")
        
        client = blazingdb.protocol.Client(connection)

        getResult = blazingdb.protocol.interpreter.GetResultRequestSchema(token=self.token)
        
        request = blazingdb.protocol.transport.channel.RequestSchema(header={
            'messageType': InterpreterMessageType.GetResult, 
            'accessToken': 123,
            'payloadLength':0},
            payload=getResult.ToBuffer())
        
        responseBuffer = client.send(request.ToBuffer())

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)  
        
        return reponseBuffer
    
