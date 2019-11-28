import bsql_engine
import pytest

def test_Initialize():
    with pytest.raises(bsql_engine.InitializeError):
        bsql_engine.initializeCaller(1, -1, b'', b'', 0, False)
