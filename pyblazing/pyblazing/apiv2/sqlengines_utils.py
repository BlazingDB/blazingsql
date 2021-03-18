from typing import Any, AnyStr, Dict, get_type_hints, TypedDict

from pyblazing.apiv2 import DataType


SQLEngineDataTypeMap = {
    'mysql': DataType.MYSQL,
    # TODO: support for more db engines
}


class SQLEngineArgs(TypedDict):
    """Members:
        from_sql_engine(str): sql engine name (v.g. mysql, postgresql, sqlite.)
        database(str): database name
        username(str): username for engine authentication
        password(str): password for engine authentication
        host(str): engine host
        host(str): engine posrt
    """
    from_sql_engine: str
    database: str
    username: str
    password: str
    host: str
    port: str


def GetSQLEngineArgs(kwargs: Dict[AnyStr, Any]) -> SQLEngineArgs:
    hintsDict = get_type_hints(SQLEngineArgs)
    argsNames = hintsDict.keys()

    try:
        sqlKwArgs = {name: kwargs[name] for name in argsNames}
        # TODO: add type checking to notify possible user errors
    except KeyError as error:
      raise MissingSQLEngineArgError(str(error)) from error

    return SQLEngineArgs(**sqlKwArgs)


class SQLEngineError(Exception):
    """Base class for errors raised using tables created from sql engines
    like mysql, postgresql, sqlite, etc.
    """

class UnsupportedSQLEngineError(SQLEngineError):
    """When unrecognized sql engine is passed by user."""
    def __init__(self, engineName: str):
        self.engineName = engineName

    def __str__(self):
        return (f'The sql engine "{self.engineName}" has no support'
                ' or is no correct. The available engines are the following:'
                f' {SQLEngineDataTypeMap.keys()}')


class MissingSQLEngineArgError(SQLEngineError):
    """When an arg is not present in user kwargs. {@see SQLEngineArgs}"""

    def __init__(self, missingName: str):
      self.missingName = missingName

    def __str__(self):
      return f'Missing SQL engine argument {self.missingName}'
