from pyblazing.apiv2 import DataType


SQLEngineDataTypeMap = {
    'mysql': DataType.MYSQL,
    # TODO: support for more db engines
}


def GetSQLEngineArgs(kwargs: Dict[Str, Any]) -> SQLEngineArgs:
  return None


class SQLEngineError(Exception):
    """
    Base class for errors raised using tables created from sql engines
    like mysql, postgresql, sqlite, etc.
    """

class UnsupportedSQLEngineError(SQLEngineError):
    """When unrecognized sql engine is passed by user."""
    def __init__(self, engineName: str):
        self.engineName = engineName

    def __str__(self):
        return (f'The sql engine "{self.engineName}" has no support'
                ' or is no correct. The available engines are the following:'
                ' {SQLEngineDataTypeMap.keys()}')
