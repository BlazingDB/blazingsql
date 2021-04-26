import typing

from pyblazing.apiv2 import DataType

SQLEngineDataTypeMap = {
    "mysql": DataType.MYSQL,
    "sqlite": DataType.SQLITE,
    "postgresql": DataType.POSTGRESQL,
    "snowflake": DataType.SNOWFLAKE,
    # TODO percy c.gonzales support for more db engines
}


def GetSQLEngineArgs(kwargs: typing.Dict[str, typing.Any], sql_table):
    kwargs["sql_table"] = sql_table
    return kwargs


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
    """When an arg is not present in user kwargs."""

    def __init__(self, missingName: str):
        self.missingName = missingName

    def __str__(self):
        return f"Missing SQL engine argument {self.missingName}"
