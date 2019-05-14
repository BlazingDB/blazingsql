from .utils import bind_request
from .connect import (create_connect_request, handle_connect_response)
from .run_dml_load_parquet_schema import (create_run_dml_load_parquet_schema_request, handle_run_dml_load_parquet_schema_response)
from .run_dml_load_csv_schema import (create_run_dml_load_csv_schema_request, handle_run_dml_load_csv_schema_response)
from .run_dml_query_token import (create_run_dml_query_token_request, handle_run_dml_query_token_response)
from .run_dml_query_filesystem_token import (create_run_dml_query_filesystem_token_request, handle_run_dml_query_filesystem_token_response)
from .run_dml_query import (create_run_dml_query_request, handle_run_dml_query_response)
from .run_ddl_create_table import (create_run_ddl_create_table_request, handle_run_ddl_create_table_response)
from .run_ddl_drop_table import (create_run_ddl_drop_table_request, handle_run_ddl_drop_table_response)
from .close_connection import (create_close_connection_request, handle_close_connection_response)
from .free_memory import (create_free_memory_request, handle_free_memory_response)
from .free_result import (create_free_result_request, handle_free_result_response)
from ._get_result import (create__get_result_request, handle__get_result_response)

class PyConnector:

    def __init__(self, orchestrator_path, orchestrator_port):
        self._path = orchestrator_path
        self._port = orchestrator_port
        self._interpreter = Interpreter(self)
        self._orchestrator = Orchestrator(self)

    def __del__(self):
        try:
            print("CLOSING CONNECTION")
            self.close_connection()
        except:
            print("Can't close connection, probably it was lost")
        del self._interpreter
        del self._orchestrator

    def connect(self):
        return self._orchestrator.connect()
    def run_dml_load_parquet_schema(self, path):
        return self._orchestrator.run_dml_load_parquet_schema(path)
    def run_dml_load_csv_schema(self, path, names, dtypes, delimiter = '|', line_terminator='\n', skip_rows=0):
        return self._orchestrator.run_dml_load_csv_schema(path, names, dtypes, delimiter = '|', line_terminator='\n', skip_rows=0)
    def run_dml_query_token(self, query, tableGroup):
        return self._orchestrator.run_dml_query_token(query, tableGroup)
    def run_dml_query_filesystem_token(self, query, tableGroup):
        return self._orchestrator.run_dml_query_filesystem_token(query, tableGroup)
    def run_dml_query(self, query, tableGroup):
        return self._orchestrator.run_dml_query(query, tableGroup)
    def run_ddl_create_table(self, tableName, columnNames, columnTypes, dbName):
        return self._orchestrator.run_ddl_create_table(tableName, columnNames, columnTypes, dbName)
    def run_ddl_drop_table(self, tableName, dbName):
        return self._orchestrator.run_ddl_drop_table(tableName, dbName)
    def close_connection(self):
        return self._orchestrator.close_connection()
    def free_memory(self, result_token, interpreter_path, interpreter_port):
        self._interpreter._path = interpreter_path
        self._interpreter._port = interpreter_port
        return self._interpreter.free_memory(result_token)
    def free_result(self, result_token, interpreter_path, interpreter_port):
        self._interpreter._path = interpreter_path
        self._interpreter._port = interpreter_port
        return self._interpreter.free_result(result_token)
    def _get_result(self, result_token, interpreter_path, interpreter_port):
        self._interpreter._path = interpreter_path
        self._interpreter._port = interpreter_port
        return self._interpreter._get_result(result_token)

class Orchestrator:
    def __init__(self, connector):
        self._connector = connector
    @property
    def _path(self):
        return self._connector._path
    @property
    def _port(self):
        return self._connector._port
    @property
    def accessToken(self):
        return self._connector.accessToken
    @accessToken.setter
    def accessToken(self, accessToken):
        self._connector.accessToken = accessToken
    def connect(self):
        pass
    def run_dml_load_parquet_schema(self, path):
        pass
    def run_dml_load_csv_schema(self, path, names, dtypes, delimiter = '|', line_terminator='\n', skip_rows=0):
        pass
    def run_dml_query_token(self, query, tableGroup):
        pass
    def run_dml_query_filesystem_token(self, query, tableGroup):
        pass
    def run_dml_query(self, query, tableGroup):
        pass
    def run_ddl_create_table(self, tableName, columnNames, columnTypes, dbName):
        pass
    def run_ddl_drop_table(self, tableName, dbName):
        pass
    def close_connection(self):
        pass

class Interpreter:
    def __init__(self, connector):
        self._connector = connector
    @property
    def accessToken(self):
        return self._connector.accessToken
    @accessToken.setter
    def accessToken(self, accessToken):
        self._connector.accessToken = accessToken
    def free_memory(self, result_token):
        pass
    def free_result(self, result_token):
        pass
    def _get_result(self, result_token):
        pass

setattr(Orchestrator, 'connect', bind_request(create_connect_request, handle_connect_response))
setattr(Orchestrator, 'run_dml_load_parquet_schema', bind_request(create_run_dml_load_parquet_schema_request, handle_run_dml_load_parquet_schema_response))
setattr(Orchestrator, 'run_dml_load_csv_schema', bind_request(create_run_dml_load_csv_schema_request, handle_run_dml_load_csv_schema_response))
setattr(Orchestrator, 'run_dml_query_token', bind_request(create_run_dml_query_token_request, handle_run_dml_query_token_response))
setattr(Orchestrator, 'run_dml_query_filesystem_token', bind_request(create_run_dml_query_filesystem_token_request, handle_run_dml_query_filesystem_token_response))
setattr(Orchestrator, 'run_dml_query', bind_request(create_run_dml_query_request, handle_run_dml_query_response))
setattr(Orchestrator, 'run_ddl_create_table', bind_request(create_run_ddl_create_table_request, handle_run_ddl_create_table_response))
setattr(Orchestrator, 'run_ddl_drop_table', bind_request(create_run_ddl_drop_table_request, handle_run_ddl_drop_table_response))
setattr(Orchestrator, 'close_connection', bind_request(create_close_connection_request, handle_close_connection_response))

setattr(Interpreter, 'free_memory', bind_request(create_free_memory_request, handle_free_memory_response))
setattr(Interpreter, 'free_result', bind_request(create_free_result_request, handle_free_result_response))
setattr(Interpreter, '_get_result', bind_request(create__get_result_request, handle__get_result_response))
