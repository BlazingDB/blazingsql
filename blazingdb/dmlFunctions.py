import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from connection import Connection 

from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType

from pygdf import read_csv
from pygdf import _gdf


class dmlFunctions:

    def __init__(self, orchestrator_path, interpreter_path, query, accessToken):
        self._orchestrator_path = orchestrator_path
        self._interpreter_path = interpreter_path
        self._access_token = accessToken

    def runQuery(self, query, input_dataset):

        tableGroup = {}
        tableGroup["name"] = ""
        tableGroup["tables"] = []
        for inputData in input_dataset:
            table = {}
            table["name"] = inputData._table_Name
            table["columns"] = []
            table["columnNames"] = []
            for name, series in inputData._gdfDataFrame._cols.items():
                #
                table["columnNames"].append(name)

                cffiView = series._column.cffi_view
                if series._column._mask is None:
                    table["columns"].append(
                        {'data': bytes(series._column._data.mem.get_ipc_handle()._ipc_handle.handle),
                         'valid': b"",
                         'size': cffiView.size,
                         'dtype': cffiView.dtype,
                         'dtype_info': 0,  # TODO dtype_info is currently not used in pygdf
                         'null_count': cffiView.null_count})
                else:
                    table["columns"].append(
                        {'data': bytes(series._column._data.mem.get_ipc_handle()._ipc_handle.handle),
                         'valid': bytes(series._column._mask.mem.get_ipc_handle()._ipc_handle.handle),
                         'size': cffiView.size,
                         'dtype': cffiView.dtype,
                         'dtype_info': 0,  # TODO dtype_info is currently not used in pygdf
                         'null_count': cffiView.null_count})

            tableGroup["tables"].append(table)

        dmlRequestSchema = self._BuildDMLRequestSchema(query, tableGroup)
        # dmlRequestSchema = blazingdb.protocol.orchestrator.DMLRequestSchema(query=query, tableGroup=tableGroup)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML,
                                                                               self._access_token, dmlRequestSchema)
        responseBuffer = Connection.sendRequest(self, self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(responseBuffer)

        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(response.payload)
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(response.payload)

        return dmlResponseDTO.resultToken

    def _BuildDMLRequestSchema(self, query, tableGroupDto):
        tableGroupName = tableGroupDto['name']
        tables = []
        for index, t in enumerate(tableGroupDto['tables']):
            tableName = t['name']
            columnNames = t['columnNames']
            columns = []
            for i, c in enumerate(t['columns']):
                data = blazingdb.protocol.gdf.cudaIpcMemHandle_tSchema(reserved=c['data'])
                if c['valid'] is None:
                    valid = blazingdb.protocol.gdf.cudaIpcMemHandle_tSchema(reserved=b'')
                else:
                    valid = blazingdb.protocol.gdf.cudaIpcMemHandle_tSchema(reserved=c['valid'])

                dtype_info = blazingdb.protocol.gdf.gdf_dtype_extra_infoSchema(time_unit=c['dtype_info'])
                gdfColumn = blazingdb.protocol.gdf.gdf_columnSchema(data=data, valid=valid, size=c['size'],
                                                                    dtype=c['dtype'], dtype_info=dtype_info,
                                                                    null_count=c['null_count'])
                columns.append(gdfColumn)
            table = blazingdb.protocol.orchestrator.BlazingTableSchema(name=tableName, columns=columns,
                                                                       columnNames=columnNames)
            tables.append(table)
        tableGroup = blazingdb.protocol.orchestrator.TableGroupSchema(tables=tables, name=tableGroupName)
        return blazingdb.protocol.orchestrator.DMLRequestSchema(query=query, tableGroup=tableGroup)

    def getResult(self, result_token):
        
        
        def createDataFrameFromResult(resultResponse):
         
            outcols = []
             
            for i, column in enumerate(resultResponse.columns):
                data_ptr = cuda.IPCMemoryHandle(column.data)
                valid_ptr = cuda.IPCMemoryHandle(column.valid)
                 
                gdf_col = _gdf._columnview(column.size, data_ptr, valid_ptr, column.dtype, column.null_count)
                newcol = Column.from_cffi_view(gdf_col)
                if(newcol.dtype == np.dtype('datetime64[ms]')):
                    outcols.append(newcol.view(DatetimeColumn, dtype='datetime64[ms]'))
                else:
                    outcols.append(newcol.view(NumericalColumn, dtype=newcol.dtype))
             
             # Build dataframe
            df = DataFrame()
            for k, v in zip(resultResponse.columnNames, outcols):
                df[k] = v
         
            return df      
        
        
        
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.GetResult, self._access_token, getResultRequest)

        responseBuffer = Connection.sendRequest(self, self._interpreter_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        resultResponse = blazingdb.protocol.interpreter.GetQueryResultFrom(response.payload)

        result.metadata = resultResponse.metadata
        result.dataFrame = createDataFrameFromResult(resultResponse)

        return result

    
    
        
    
    
class inputData:

    def __init__(self, table_name, gdfDataFrame):
        self._table_Name = table_name
        self._gdfDataFrame = gdfDataFrame
