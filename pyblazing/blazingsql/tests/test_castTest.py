from blazingsql.tests.BlazingSQLTest.DataBase import createSchema as cs
from blazingsql.tests.BlazingSQLTest.Configuration import Settings as Settings
from blazingsql.tests.BlazingSQLTest.Runner import runTest
from blazingsql.tests.BlazingSQLTest.Utils import Execution
from blazingsql import BlazingContext
from blazingsql.tests.BlazingSQLTest.Utils import gpuMemory, the_test_name
from pynvml import *
from blazingsql import DataType
from blazingsql.tests.BlazingSQLTest.Utils import dquery, skip_test, init_context

from blazingsql.tests.BlazingSQLTest.EndToEndTests.castTest import main


def test_all():
    Execution.getArgs()
    
    nvmlInit()

    # Create Context For BlazingSQL
    bc, dask_client = init_context()

    nRals = Settings.data['RunSettings']['nRals']
    main(dask_client, None, None, Settings.data['TestSettings']['dataDirectory'], bc, nRals)

    runTest.save_log()

    gpuMemory.print_log_gpu_memory()
