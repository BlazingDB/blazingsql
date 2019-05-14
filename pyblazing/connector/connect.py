from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import (AuthRequestSchema, AuthResponseSchema, OrchestratorMessageType)
from blazingdb.protocol.transport.channel import (MakeAuthRequestBuffer, ResponseSchema, ResponseErrorSchema)

def create_connect_request(self):
    # TODO find a way to print only for debug mode (add verbose arg)
    #print("open connection")
    return MakeAuthRequestBuffer(OrchestratorMessageType.AuthOpen, AuthRequestSchema())

def handle_connect_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        errorResponse = ResponseErrorSchema.From(response.payload)
        print(errorResponse.errors)
        raise Error(errorResponse.errors.decode('utf-8'))
    responsePayload = AuthResponseSchema.From(response.payload)
    # TODO find a way to print only for debug mode (add verbose arg)
    # print(responsePayload.accessToken)
    self.accessToken = responsePayload.accessToken
    return responsePayload

