from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import (BuildDMLRequestSchema, DMLResponseSchema, OrchestratorMessageType)
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema, ResponseErrorSchema)


def create_run_dml_query_token_request(self, query, tableGroup):
    dmlRequestSchema = BuildDMLRequestSchema(query, tableGroup)
    return MakeRequestBuffer(OrchestratorMessageType.DML, self.accessToken, dmlRequestSchema)


def handle_run_dml_query_token_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        errorResponse = ResponseErrorSchema.From(response.payload)
        if b'SqlSyntaxException' in errorResponse.errors:
            raise SyntaxError(errorResponse.errors.decode('utf-8'))
        elif b'SqlValidationException' in errorResponse.errors:
            raise ValueError(errorResponse.errors.decode('utf-8'))
        raise Error(errorResponse.errors.decode('utf-8'))
    dmlResponseDTO = DMLResponseSchema.From(response.payload)
    return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path.decode('utf8'), dmlResponseDTO.nodeConnection.port, dmlResponseDTO.calciteTime

