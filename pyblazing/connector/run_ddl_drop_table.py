from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.orchestrator import (DDLDropTableRequestSchema, OrchestratorMessageType)
from blazingdb.protocol.transport.channel import (MakeRequestBuffer, ResponseSchema, ResponseErrorSchema)

def create_run_ddl_drop_table_request(self, tableName, dbName):
    # TODO find a way to print only for debug mode (add verbose arg)
    #print('drop table: ' + tableName)
    dmlRequestSchema = DDLDropTableRequestSchema(name=tableName, dbName=dbName)
    return MakeRequestBuffer(OrchestratorMessageType.DDL_DROP_TABLE, self.accessToken, dmlRequestSchema)


def handle_run_ddl_drop_table_response(self, response_buffer):
    response = ResponseSchema.From(response_buffer)
    if response.status == Status.Error:
        errorResponse = ResponseErrorSchema.From(response.payload)
        raise Error(errorResponse.errors.decode('utf-8'))
    # TODO find a way to print only for debug mode (add verbose arg)
    # print(response.status)
    return response.status

