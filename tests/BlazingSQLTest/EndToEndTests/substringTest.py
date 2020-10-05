from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test


def main(dask_client, drill, spark, dir_data_file, bc, nRals):
    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Substring"

    def executionTest():

        tables = ["partsupp", "customer", "nation"]
        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
        ]  # TODO json

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
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select SUBSTRING(CAST(ps_partkey as VARCHAR),1,1),
                        ps_availqty
                    from partsupp
                    where ps_availqty > 7000 and ps_supplycost > 700
                    order by ps_partkey, ps_availqty limit 50"""
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
            query = """select c_custkey, c_name from customer
                    where SUBSTRING(c_name,1,17) = 'Customer#00000000'"""
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
            query = """select c_custkey, SUBSTRING(c_name, 1, 8) from customer
                    where c_name = 'Customer#000000009'"""
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
            query = """select * from nation
                    where SUBSTRING(n_name,1,1) = 'I'"""
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

            queryId = "TEST_05"
            query = """select c_custkey, c_name, SUBSTRING(c_name,1,1),
                        SUBSTRING(c_name,2,1), SUBSTRING(c_name,1,2),
                        SUBSTRING(c_name,2,2)
                    from customer where c_custkey < 20"""
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

            queryId = "TEST_06"
            query = """select c.c_custkey, SUBSTRING(c.c_name, 10, 18),
                        CAST(SUBSTRING(c.c_name, 10, 18) as INT),
                        CAST(SUBSTRING(c.c_name, 10, 18) as INT) + 1
                    from customer c
                    where c.c_custkey < 50"""
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

            queryId = "TEST_07"
            query = """select c.c_custkey, SUBSTRING(c.c_name, 1, 8),
                        SUBSTRING(c.c_name, 10, 18) || '**'
                    from customer c where c.c_custkey < 0"""
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

            queryId = "TEST_08"
            query = """select * from (
                            select c.c_custkey, SUBSTRING(c.c_name, 1, 8) as n1, SUBSTRING(c.c_name, 10, 18) || '**' as n2
                            from customer c where c.c_custkey < 50
                     ) as n where SUBSTRING(n.n1, 1,7) = 'Customer'"""
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
            query = """select substring(c_comment, 5 , CHAR_LENGTH(c_comment) - 5 ),
                        substring(c_comment, CHAR_LENGTH(c_comment) - 3 , CHAR_LENGTH(c_comment) - 1 ),
                        substring(substring(c_comment, 3 , CHAR_LENGTH(c_comment) - 1 ), 1, 1 )
                        from customer where c_custkey < 100"""
            runTest.run_query(
                bc,
                spark,
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
            query = """select c_custkey, substring(c_name, 1),
                         substring(c_name, 10),
                        substring(c_name, cast(c_nationkey as bigint)),
                        substring(c_name, c_nationkey + 3)
                    from customer
                    where (c_nationkey between 1 and 10) and c_custkey < 100"""
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
                print_result=True,
            )

            queryId = "TEST_11"
            query = """select c_custkey, substring(c_name, 1, 5),
                        substring(c_name, 10, 7),
                        substring(c_name, 3, cast(c_nationkey as bigint)),
                        substring(c_name, c_nationkey, 4),
                        substring(c_name, cast(c_nationkey as bigint),
                            c_nationkey + 0),
                        substring(c_name, c_nationkey + 3, c_nationkey)
                    from customer
                    where (c_nationkey between 1 and 10) and c_custkey < 100"""
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
                print_result=True,
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
    spark = "spark"

    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if (
        Settings.execution_mode == ExecutionMode.FULL and compareResults == "true"
    ) or Settings.execution_mode == ExecutionMode.GENERATOR:
        # Create Table Drill ------------------------------------------------
        print("starting drill")

        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill, Settings.data["TestSettings"]["dataDirectory"])

        # Create Table Spark ------------------------------------------------------------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark, Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(
        dask_client,
        drill,
        spark,
        Settings.data["TestSettings"]["dataDirectory"],
        bc,
        nRals,
    )

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
