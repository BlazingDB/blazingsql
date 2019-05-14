from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.io import ParquetFileSchema
from blazingdb.protocol.orchestrator import (DMLResponseSchema, OrchestratorMessageType)
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema, ResponseErrorSchema)

def create_run_dml_load_parquet_schema_request(self, path):
    print('load parquet file')
    ## todo use rowGroupIndices, and columnIndices, someway??
    requestSchema = ParquetFileSchema(path=path, rowGroupIndices=[], columnIndices=[])
    return MakeRequestBuffer(OrchestratorMessageType.LoadParquetSchema, self.accessToken, requestSchema)


def handle_run_dml_load_parquet_schema_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        errorResponse = ResponseErrorSchema.From(response.payload)
        if b'SqlSyntaxException' in errorResponse.errors:
            raise SyntaxError(errorResponse.errors.decode('utf-8'))
        elif b'SqlValidationException' in errorResponse.errors:
            raise ValueError(errorResponse.errors.decode('utf-8'))
        raise Error(errorResponse.errors.decode('utf-8'))
    dmlResponseDTO = DMLResponseSchema.From(response.payload)
    return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path, dmlResponseDTO.nodeConnection.port

