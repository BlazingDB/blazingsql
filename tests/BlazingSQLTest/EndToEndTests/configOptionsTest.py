from DataBase import createSchema as cs
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution
from Utils import gpuMemory, skip_test, init_context
from pynvml import nvmlInit
from blazingsql import DataType
from Configuration import ExecutionMode
from EndToEndTests import tpchQueries as tpch
import gc
import time


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    if dask_client is not None:
        dask_client.close()
        dask_client.shutdown()
        del dask_client
    del bc

    # conf_opt_1
    conf_opt_1 = {}
    conf_opt_1["JOIN_PARTITION_SIZE_THRESHOLD"] = 10
    conf_opt_1["MAX_DATA_LOAD_CONCAT_CACHE_BYTE_SIZE"] = 10
    conf_opt_1["MAX_KERNEL_RUN_THREADS"] = 5

    # conf_opt_2
    conf_opt_2 = {}
    conf_opt_2["JOIN_PARTITION_SIZE_THRESHOLD"] = 10
    conf_opt_2["MAX_DATA_LOAD_CONCAT_CACHE_BYTE_SIZE"] = 10
    conf_opt_2["MAX_KERNEL_RUN_THREADS"] = 1

    # conf_opt_3
    conf_opt_3 = {}
    conf_opt_3["MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE"] = 1
    conf_opt_3["NUM_BYTES_PER_ORDER_BY_PARTITION"] = 10

    # conf_opt_4
    conf_opt_4 = {}
    conf_opt_4["MAX_ORDER_BY_SAMPLES_PER_NODE"] = 5
    conf_opt_4["MAX_SEND_MESSAGE_THREADS"] = 1
    conf_opt_4["TRANSPORT_BUFFER_BYTE_SIZE"] = 10000
    conf_opt_4["TRANSPORT_POOL_NUM_BUFFERS"] = 10000

    # conf_opt_5
    conf_opt_5 = {}
    conf_opt_5["MAX_ORDER_BY_SAMPLES_PER_NODE"] = 5000000
    conf_opt_5["MAX_SEND_MESSAGE_THREADS"] = 200
    conf_opt_5["TRANSPORT_BUFFER_BYTE_SIZE"] = 10000
    conf_opt_5["TRANSPORT_POOL_NUM_BUFFERS"] = 10

    # conf_opt_6
    conf_opt_6 = {}
    conf_opt_6["MAX_ORDER_BY_SAMPLES_PER_NODE"] = 5
    conf_opt_6["MAX_SEND_MESSAGE_THREADS"] = 200
    conf_opt_6["TRANSPORT_BUFFER_BYTE_SIZE"] = 100000000
    conf_opt_6["TRANSPORT_POOL_NUM_BUFFERS"] = 10

    # conf_opt_7
    conf_opt_7 = {}
    conf_opt_7["MAX_ORDER_BY_SAMPLES_PER_NODE"] = 5000000
    conf_opt_7["MAX_SEND_MESSAGE_THREADS"] = 1
    conf_opt_7["TRANSPORT_BUFFER_BYTE_SIZE"] = 100000000
    conf_opt_7["TRANSPORT_POOL_NUM_BUFFERS"] = 100

    # conf_opt_8
    conf_opt_8 = {}
    conf_opt_8["BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD"] = 0.0001
    conf_opt_8["MEMORY_MONITOR_PERIOD"] = 5000000

    # conf_opt_9
    conf_opt_9 = {}
    conf_opt_9["ENABLE_GENERAL_ENGINE_LOGS"] = True
    conf_opt_9["ENABLE_COMMS_LOGS"] = True
    conf_opt_9["ENABLE_CACHES_LOGS"] = True
    conf_opt_9["ENABLE_OTHER_ENGINE_LOGS"] = True

    # conf_opt_10
    conf_opt_10 = {}
    conf_opt_10["ENABLE_GENERAL_ENGINE_LOGS"] = False
    conf_opt_10["ENABLE_COMMS_LOGS"] = False
    conf_opt_10["ENABLE_CACHES_LOGS"] = False
    conf_opt_10["ENABLE_OTHER_ENGINE_LOGS"] = False

    # all sets
    all_set_list = [
        conf_opt_1,
        conf_opt_2,
        conf_opt_3,
        conf_opt_4,
        conf_opt_5,
        conf_opt_6,
        conf_opt_7,
        conf_opt_8,
        conf_opt_9,
        conf_opt_10,
    ]

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Config Options"

    def executionTest(queryType, setInd, config_options):

        bc, dask_client = init_context(config_options)

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
            DataType.PARQUET,
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
            print("Test set: " + str(setInd + 1) + " Options: " + str(config_options))
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

            if fileSchemaType == DataType.PARQUET:
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
                fileSchemaType,
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
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
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

            queryId = "TEST_20"

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

            queryId = "TEST_21"

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

        if dask_client is not None:
            dask_client.run(gc.collect)
            dask_client.run_on_scheduler(gc.collect)

            dask_client.close()
            dask_client.shutdown()
            del dask_client
        del bc

    for setInd, config_options in enumerate(all_set_list):
        executionTest(queryType, setInd, config_options)
        gc.collect()

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
        # Create Table Drill -----------------------------------------
        print("starting drill")
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill, Settings.data["TestSettings"]["dataDirectory"])

        # Create Table Spark -------------------------------------------------
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
