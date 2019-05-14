from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import (InterpreterMessage, GetResultRequestSchema)
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema)


def create_free_result_request(self, result_token):
    getResultRequest = GetResultRequestSchema(resultToken=result_token)
    return MakeRequestBuffer(InterpreterMessage.FreeResult, self.accessToken, getResultRequest)


def handle_free_result_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        raise ValueError('Error status')
    # TODO find a way to print only for debug mode (add verbose arg)
    #print('free result OK!')

