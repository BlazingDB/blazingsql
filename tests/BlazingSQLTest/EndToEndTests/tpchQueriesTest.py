from DataBase import createSchema as cs
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution
from Utils import gpuMemory, skip_test, init_context
from pynvml import nvmlInit
from blazingsql import DataType
from Configuration import ExecutionMode
from EndToEndTests import tpchQueries as tpch


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "TPCH Queries"

    def executionTest(queryType):

        tables = [
            "nation",
            "region",
            "customer",
            "lineitem",
            "orders",
            "supplier",
            "part",
            "partsupp",
        ]
        data_types = [           
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.PARQUET
        ]  # TODO orc, json

        # Create Tables ------------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            # Run Query ------------------------------------------------------
            worder = 1  # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.001

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"

            query = tpch.get_tpch_query(queryId)

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

            query = tpch.get_tpch_query(queryId)

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

            query = tpch.get_tpch_query(queryId)

            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                0.1,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_04"

            query = tpch.get_tpch_query(queryId)

            if fileSchemaType == DataType.ORC:
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
            else:
                runTest.run_query(
                    bc,
                    drill,
                    query,
                    queryId,
                    queryType,
                    worder,
                    "",
                    acceptable_difference,
                    True,
                    fileSchemaType,
                )

            queryId = "TEST_05"

            query = tpch.get_tpch_query(queryId)

            if fileSchemaType == DataType.ORC:
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
            else:
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

            query = tpch.get_tpch_query(queryId)

            runTest.run_query(
                    bc,
                    spark,
                    query,
                    queryId,
                    queryType,
                    worder,
                    "",
                    acceptable_difference,
                    True,
                    fileSchemaType,
                )

            queryId = "TEST_07"

            query = tpch.get_tpch_query(queryId)

            if fileSchemaType == DataType.ORC:
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
            else:
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

            query = tpch.get_tpch_query(queryId)

            if fileSchemaType == DataType.ORC:
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
            else:
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

            query = tpch.get_tpch_query(queryId)

            if fileSchemaType == DataType.ORC:
                runTest.run_query(
                    bc,
                    spark,
                    query,
                    queryId,
                    queryType,
                    worder,
                    "",
                    acceptable_difference,
                    True,
                    fileSchemaType,
                )
            else:
                runTest.run_query(
                    bc,
                    drill,
                    query,
                    queryId,
                    queryType,
                    worder,
                    "",
                    acceptable_difference,
                    True,
                    fileSchemaType,
                )

            queryId = "TEST_10"

            query = tpch.get_tpch_query(queryId)

            # TODO: Failed test with nulls
            testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
            if testsWithNulls != "true":
                if fileSchemaType == DataType.ORC:
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
                else:
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

            query = tpch.get_tpch_query(queryId)

            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            queryId = "TEST_12"

            query = tpch.get_tpch_query(queryId)

            if fileSchemaType == DataType.ORC:
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
            else:
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

            queryId = "TEST_13"

            query = tpch.get_tpch_query(queryId)

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

            queryId = "TEST_14"

            query = tpch.get_tpch_query(queryId)

            if fileSchemaType == DataType.ORC:
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
            else:
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

            queryId = "TEST_15"

            query = tpch.get_tpch_query(queryId)

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
                fileSchemaType
            )

            queryId = "TEST_16"

            query = tpch.get_tpch_query(queryId)

            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                '',
                acceptable_difference,
                use_percentage,
                fileSchemaType
            )

            queryId = "TEST_17"

            query = tpch.get_tpch_query(queryId)

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

            queryId = "TEST_18"

            query = tpch.get_tpch_query(queryId)

            if fileSchemaType == DataType.ORC:
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
            else:
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

            queryId = "TEST_19"

            query = tpch.get_tpch_query(queryId)

            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                True,
                fileSchemaType,
            )

            # queryId = "TEST_20"

            # query = tpch.get_tpch_query(queryId)

            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     True,
            #     fileSchemaType,
            # )

            # queryId = "TEST_21"

            # query = tpch.get_tpch_query(queryId)

            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            queryId = "TEST_22"

            query = tpch.get_tpch_query(queryId)

            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

    executionTest(queryType)

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
        Settings.execution_mode == ExecutionMode.FULL and
            compareResults == "true"
    ) or Settings.execution_mode == ExecutionMode.GENERATOR:
        # Create Table Drill -----------------------------------------
        print("starting drill")
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(
                            drill,
                            Settings.data["TestSettings"]["dataDirectory"]
        )

        # Create Table Spark -------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(
                            spark,
                            Settings.data["TestSettings"]["dataDirectory"]
        )

    # Create Context For BlazingSQL

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
