from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import (InterpreterMessage, GetResultRequestSchema)
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema)


def create_free_memory_request(self, result_token=None):
    # (ptaylor) TODO: is this correct?
    if result_token is None:
        result_token = 2433423
    return MakeRequestBuffer(InterpreterMessage.FreeMemory, self.accessToken, GetResultRequestSchema(resultToken=result_token))


def handle_free_memory_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        raise ValueError('Error status')
    # TODO find a way to print only for debug mode (add verbose arg)
    #print('free result OK!')

