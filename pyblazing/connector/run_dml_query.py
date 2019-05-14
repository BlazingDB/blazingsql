from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import (BuildDMLRequestSchema, DMLResponseSchema, OrchestratorMessageType)
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema, ResponseErrorSchema)


def create_run_dml_query_request(self, query, tableGroup):
    # TODO find a way to print only for debug mode (add verbose arg)
    # print(query)
    dmlRequestSchema = BuildDMLRequestSchema(query, tableGroup)
    return MakeRequestBuffer(OrchestratorMessageType.DML, self.accessToken, dmlRequestSchema)


def handle_run_dml_query_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        errorResponse = ResponseErrorSchema.From(response.payload)
        raise Error(errorResponse.errors.decode('utf-8'))
    dmlResponseDTO = DMLResponseSchema.From(response.payload)
    return self._get_result(
        dmlResponseDTO.resultToken,
        dmlResponseDTO.nodeConnection.path,
        dmlResponseDTO.nodeConnection.port
    )

