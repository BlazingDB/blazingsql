from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import (AuthRequestSchema, AuthResponseSchema, OrchestratorMessageType)
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema, ResponseErrorSchema)

def create_close_connection_request(self):
    # TODO find a way to print only for debug mode (add verbose arg)
    # print("close connection")
    return MakeRequestBuffer(OrchestratorMessageType.AuthClose, self.accessToken, AuthRequestSchema())


def handle_close_connection_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        errorResponse = ResponseErrorSchema.From(response.payload)
        raise Error(errorResponse.errors.decode('utf-8'))
    # TODO find a way to print only for debug mode (add verbose arg)
    # print(response.status)

