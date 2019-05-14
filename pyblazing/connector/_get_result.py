from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import (InterpreterMessage, GetResultRequestSchema, GetQueryResultFrom)
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema)


def create__get_result_request(self, result_token):
    getResultRequest = GetResultRequestSchema(resultToken=result_token)
    return MakeRequestBuffer(InterpreterMessage.GetResult, self.accessToken, getResultRequest)


def handle__get_result_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        raise ValueError('Error status')
    queryResult = GetQueryResultFrom(response.payload)
    if queryResult.metadata.status.decode() == "Error":
        raise Error(queryResult.metadata.message.decode('utf-8'))
    return queryResult

