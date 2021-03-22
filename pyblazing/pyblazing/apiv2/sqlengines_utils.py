from typing import Any, AnyStr, Dict, get_type_hints, TypedDict

from pyblazing.apiv2 import DataType


SQLEngineDataTypeMap = {
    "mysql": DataType.MYSQL,
    # TODO: support for more db engines
}


class SQLEngineArgs(TypedDict):
    # TODO cris percy update the docs
    """Members:
        from_sql_engine(str): sql engine name (v.g. mysql, postgresql, sqlite.)
        database(str): database name
        username(str): username for engine authentication
        password(str): password for engine authentication
        host(str): engine host
        port(str): engine port
        database_table(str): database table name used to import data
    """
    from_sql: str
    sql_hostname: str
    sql_port: str
    sql_username: str
    sql_password: str
    sql_schema: str
    sql_table: str
    sql_table_filter: str
    sql_table_batch_size: int


def GetSQLEngineArgs(kwargs: Dict[AnyStr, Any], sql_table) -> SQLEngineArgs:
    hintsDict = get_type_hints(SQLEngineArgs)
    argsNames = hintsDict.keys()
    kwargs["sql_table"] = sql_table
    return SQLEngineArgs(**kwargs)


class SQLEngineError(Exception):
    """Base class for errors raised using tables created from sql engines
    like mysql, postgresql, sqlite, etc.
    """


class UnsupportedSQLEngineError(SQLEngineError):
    """When unrecognized sql engine is passed by user."""

    def __init__(self, engineName: str):
        self.engineName = engineName

    def __str__(self):
        return (
            f'The sql engine "{self.engineName}" has no support'
            " or is no correct. The available engines are the following:"
            f" {SQLEngineDataTypeMap.keys()}"
        )


class MissingSQLEngineArgError(SQLEngineError):
    """When an arg is not present in user kwargs. {@see SQLEngineArgs}"""

    def __init__(self, missingName: str):
        self.missingName = missingName

    def __str__(self):
        return f"Missing SQL engine argument {self.missingName}"
