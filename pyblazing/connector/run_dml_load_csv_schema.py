from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.io import CsvFileSchema
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema, ResponseErrorSchema)

def create_run_dml_load_csv_schema_request(self, path, names, dtypes, delimiter = '|', line_terminator='\n', skip_rows=0):
    print('load csv file')
    requestSchema = CsvFileSchema(path=path, delimiter=delimiter, lineTerminator=line_terminator, skipRows=skip_rows, names=names, dtypes=dtypes)
    return MakeRequestBuffer(OrchestratorMessageType.LoadCsvSchema, self.accessToken, requestSchema)


def handle_run_dml_load_csv_schema_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        errorResponse = ResponseErrorSchema.From(response.payload)
        if b'SqlSyntaxException' in errorResponse.errors:
            raise SyntaxError(errorResponse.errors.decode('utf-8'))
        elif b'SqlValidationException' in errorResponse.errors:
            raise ValueError(errorResponse.errors.decode('utf-8'))
        raise Error(errorResponse.errors.decode('utf-8'))
    dmlResponseDTO = DMLResponseSchema.From(response.payload)
    return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path.decode('utf8'), dmlResponseDTO.nodeConnection.port

