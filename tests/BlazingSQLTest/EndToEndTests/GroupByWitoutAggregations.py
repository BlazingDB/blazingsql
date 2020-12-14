from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Group by without aggregations"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation", "region", "customer"]
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

            # Run Query -----------------------------------------------------
            # Parameter to indicate if its necessary to order the
            # resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select n_nationkey, n_regionkey from nation
                    group by n_regionkey, n_nationkey"""
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
            query = """select c_custkey, c_nationkey from customer
                    where c_acctbal < 1000 group by c_nationkey, c_custkey"""
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
            query = """select c.c_custkey, r.r_regionkey,
                    c.c_custkey + r.r_regionkey as addy from customer as c
                    inner join region as r
                    on c.c_nationkey = r.r_regionkey
                    group by r.r_regionkey, c.c_custkey"""
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
            query = """select c_nationkey, c_custkey from customer
                    where c_acctbal < 10000 group by c_nationkey,
                    c_custkey order by c_nationkey desc, c_custkey asc"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_05"
            query = """select c.c_custkey, r.r_regionkey,
                    c.c_custkey + r.r_regionkey as addy from customer as c
                    inner join region as r on c.c_nationkey = r.r_regionkey
                    where c.c_acctbal < 1000 group by r.r_regionkey,
                    c.c_custkey order by c.c_custkey desc"""
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
        # Create Table Drill -----------------------------------------
        print("starting drill")
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
