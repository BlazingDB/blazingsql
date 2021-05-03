from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Interval"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation", "region", "customer", "orders", "lineitem"]
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

            # TODO: remove all the print_results=True (for now it's useful for experiments)
            # All the test are being casting to VARCHAR in order to save the results not as interval type (issue drill ?)

            # ======================== INTERVAL: SECONDS ===========================
            queryId = "TEST_01"
            query = """select CAST(INTERVAL '4' SECOND AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_02"
            query = """select CAST(INTERVAL '1' SECOND + INTERVAL '3' SECOND AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_03"
            query = """select CASt(CAST(INTERVAL '1' DAY as INTERVAL SECOND) as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_04"
            query = """select CAST(INTERVAL '15:30' MINUTE TO SECOND as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_05"
            query = """select CAST(INTERVAL '01:10:02' HOUR TO SECOND as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_06"
            query = """select CAST(INTERVAL '2 00:00:00' DAY TO SECOND as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_07"
            query = """select CAST(INTERVAL '2 01:03:10' DAY TO SECOND as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_08"
            query = """select CAST(900 * INTERVAL '1' SECOND  as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_09"
            query = """select CAST(date '1990-05-02' + INTERVAL '45' SECOND as date) from region"""
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
                print_result=True
            )

            queryId = "TEST_10"
            query = """select timestamp '1990-05-02 05:10:12' + INTERVAL '1' SECOND from region"""
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
                print_result=True
            )

            # # TODO: this fails due to ms values
            # queryId = "TEST_11"
            # query = """select cast(INTERVAL '03:04:11.332' HOUR TO SECOND as VARCHAR) from region"""
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
            #     print_result=True
            # )

            # ======================== INTERVAL: MINUTES ===========================

            queryId = "TEST_11"
            query = """select cast(INTERVAL '4' MINUTE as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_12"
            query = """select CAST(INTERVAL '1' MINUTE + INTERVAL '3' MINUTE AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_13"
            query = """select CAST(CAST(INTERVAL '1' DAY as INTERVAL MINUTE) AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_14"
            query = """select CAST(INTERVAL '23:15' HOUR(2) TO MINUTE AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_15"
            query = """select CAST(INTERVAL '123:15' HOUR(3) TO MINUTE AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_16"
            query = """select CAST(INTERVAL '2 10:40' DAY(1) TO MINUTE AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_17"
            query = """select CAST(150 * INTERVAL '1' MINUTE AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_18"
            query = """select CAST(date '1990-05-02' + INTERVAL '45' MINUTE AS DATE) from region"""
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
                print_result=True
            )

            queryId = "TEST_19"
            query = """select timestamp '1990-05-02 05:10:12' + INTERVAL '1' MINUTE from region"""
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
                print_result=True
            )

            # ======================== INTERVAL: HOURS ===========================

            queryId = "TEST_20"
            query = """select CAST(INTERVAL '4' HOUR AS VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_21"
            query = """select cast(INTERVAL '1' HOUR + INTERVAL '3' HOUR as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_22"
            query = """select cast(INTERVAL '2 10' DAY(1) TO HOUR as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_23"
            query = """select cast(INTERVAL '125 10' DAY(3) TO HOUR as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_24"
            query = """select cast(150 * INTERVAL '1' HOUR as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_25"
            query = """select cast(date '1990-05-02' + INTERVAL '12' HOUR as date) from region"""
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
                print_result=True
            )

            queryId = "TEST_26"
            query = """select cast(date '1990-05-02' + INTERVAL '25' HOUR as date) from region"""
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
                print_result=True
            )

            queryId = "TEST_27"
            query = """select timestamp '1990-05-02 05:10:12' + INTERVAL '1' HOUR from region"""
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
                print_result=True
            )

            # ======================== INTERVAL: DAYS ===========================

            queryId = "TEST_28"
            query = """select CAST(INTERVAL '4' DAY as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_29"
            query = """select CAST(INTERVAL '1' DAY + INTERVAL '3' DAY as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_30"
            query = """select CAST(150 * INTERVAL '1' DAY as VARCHAR) from region"""
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
                print_result=True
            )

            queryId = "TEST_31"
            query = """select cast(date '1990-05-02' + INTERVAL '1' DAY as date) from region"""
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
                print_result=True
            )

            queryId = "TEST_32"
            query = """select timestamp '1990-05-02 05:10:12' + INTERVAL '2' DAY from region"""
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
                print_result=True
            )

            # TODO: for now not support for INTERVAL on months and years

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

    if ((Settings.execution_mode == ExecutionMode.FULL and
         compareResults == "true") or
            Settings.execution_mode == ExecutionMode.GENERATOR):
        # Create Table Drill ------------------------------------------------
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

        # Create Table Spark -------------------------------------------------
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark,
                             Settings.data["TestSettings"]["dataDirectory"])

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
