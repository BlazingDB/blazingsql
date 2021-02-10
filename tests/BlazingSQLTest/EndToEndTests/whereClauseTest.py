from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Where clause"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():

        tables = ["customer", "orders", "nation"]
        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
        ]  # TODO json

        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            print(fileSchemaType)
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            # Run Query ------------------------------------------------------
            # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select c_custkey, c_nationkey, c_acctbal
                    from customer where c_custkey < 15"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_02"
            query = """select c_custkey, c_nationkey, c_acctbal
                    from customer where c_custkey < 150
                    and c_nationkey = 5"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_03"
            query = """select c_custkey, c_nationkey as nkey
                    from customer where c_custkey < 0"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_04"
            query = """select c_custkey, c_nationkey as nkey
                    from customer where c_custkey < 0
                    and c_nationkey >= 30"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_05"
            query = """select c_custkey, c_nationkey as nkey
                    from customer where c_custkey < 0
                    or c_nationkey >= 24"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_06"
            query = """select c_custkey, c_nationkey as nkey
                    from customer where c_custkey < 0
                    and c_nationkey >= 3"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_07"
            query = """select c_custkey, c_nationkey, c_acctbal
                    from customer
                    where c_custkey < 150 and c_nationkey = 5
                    and c_acctbal > 600"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_08"
            query = """select c_custkey, c_nationkey as nkey
                    from customer where c_custkey > 0
                    or c_nationkey >= 24 or c_acctbal > 700"""
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

            queryId = "TEST_09"
            query = """select c_custkey, c_nationkey, c_acctbal
                    from customer where c_custkey < 150 and c_nationkey = 5
                    or c_custkey = 200 or c_nationkey >= 10
                    or c_acctbal <= 500"""
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

            queryId = "TEST_10"
            query = """select c.c_custkey, c.c_nationkey, n.n_regionkey
                    from customer as c
                    inner join nation as n on c.c_nationkey = n.n_nationkey
                    where n.n_regionkey = 1
                    and (c.c_custkey > 10 and c.c_custkey < 50)"""
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

            queryId = "TEST_11"
            query = """select * from nation
                    where n_regionkey > 2 and n_regionkey < 5"""
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

            # Gives [RunExecuteGraph Error] std::exception and crashes with ORC
            # Related PR: https://github.com/BlazingDB/blazingsql/issues/1324
            # queryId = "TEST_12"
            # query = """select c_custkey, c_nationkey as nkey from customer
            #         where -c_nationkey + c_acctbal > 750.3"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "c_custkey",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # Gives [RunExecuteGraph Error] std::exception and crashes with ORC
            # Related PR: https://github.com/BlazingDB/blazingsql/issues/1324
            # queryId = "TEST_13"
            # query = """select c_custkey, c_nationkey as nkey from customer
            #    where -c_nationkey + c_acctbal > 750"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "c_custkey",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

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

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
