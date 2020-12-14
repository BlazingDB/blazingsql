from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Full outer join"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation"]

        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
            DataType.JSON
        ]

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            # Run Query ------------------------------------------------------
            # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select n1.n_nationkey as n1key,
                    n2.n_nationkey as n2key, n1.n_nationkey + n2.n_nationkey
                    from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_02"
            query = """select n1.n_nationkey as n1key,
                        n2.n_nationkey as n2key,
                        n1.n_nationkey + n2.n_nationkey
                    from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    where n1.n_nationkey < 10"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_03"
            query = """select n1.n_nationkey as n1key,
                        n2.n_nationkey as n2key,
                        n1.n_nationkey + n2.n_nationkey
                    from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    where n1.n_nationkey < 10 and n1.n_nationkey > 5"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_04"
            query = """select n1.n_nationkey as n1key,
                        n2.n_nationkey as n2key,
                        n1.n_nationkey + n2.n_nationkey
                    from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    and n1.n_nationkey + 1 = n2.n_nationkey + 7
                    and n1.n_nationkey + 2 = n2.n_nationkey + 8"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break

    executionTest()

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    drill = "drill"  # None

    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if ((Settings.execution_mode == ExecutionMode.FULL and
         compareResults == "true") or
            Settings.execution_mode == ExecutionMode.GENERATOR):
        # Create Table Drill ------------------------------------------------
        print("starting drill")
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill,
         Settings.data["TestSettings"]["dataDirectory"], bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
