from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Window"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["orders", "nation", "lineitem"]
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
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)

            print("==============================")

            # ------------------- ORDER BY ------------------------
            
            # queryId = "TEST_01"
            # query = """select min(n_nationkey) over 
            #                 (
            #                     order by n_regionkey
            #                 ) min_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation order by n_name"""
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

            # queryId = "TEST_02"
            # query = """select min(n_nationkey) over 
            #                 (
            #                     order by n_regionkey,
            #                     n_name desc
            #                 ) min_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation order by n_name"""
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

            # queryId = "TEST_03"
            # query = """select max(n_nationkey) over 
            #                 (
            #                     order by n_regionkey,
            #                     n_name desc
            #                 ) max_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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
            
            # queryId = "TEST_04"
            # query = """select count(n_nationkey) over 
            #                 (
            #                     order by n_regionkey,
            #                     n_name
            #                 ) count_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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

            # queryId = "TEST_05"
            # query = """select row_number() over 
            #                 (
            #                     order by n_regionkey desc,
            #                     n_name
            #                 ) row_num,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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

            # queryId = "TEST_06"
            # query = """select sum(n_nationkey) over 
            #                 (
            #                     order by n_nationkey desc
            #                 ) sum_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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

            # queryId = "TEST_07"
            # query = """select avg(cast(n_nationkey as double)) over 
            #                 (
            #                     order by n_regionkey,
            #                     n_name desc
            #                 ) avg_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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

            # queryId = "TEST_08"
            # query = """select first_value(n_nationkey) over
            #                 (
            #                     order by n_regionkey desc,
            #                     n_name
            #                 ) first_val,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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

            # queryId = "TEST_09"
            # query = """select last_value(n_nationkey) over
            #                 (
            #                     order by n_regionkey desc,
            #                     n_name
            #                 ) last_val,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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

            # ----------------- PARTITION BY ----------------------

            queryId = "TEST_11"
            query = """select min(n_nationkey) over
                            (
                                partition by n_regionkey
                            ) min_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_name"""
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

            queryId = "TEST_12"
            query = """select max(n_nationkey) over
                            (
                                partition by n_regionkey
                            ) max_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_nationkey"""
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
            query = """select count(n_nationkey) over
                            (
                                partition by n_regionkey
                            ) count_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_regionkey, count_keys"""
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
            query = """select sum(n_nationkey) over 
                            (
                                partition by n_regionkey
                            ) sum_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_regionkey"""
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
            query = """select avg(cast(n_nationkey as double)) over 
                            (
                                partition by n_regionkey
                            ) avg_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_nationkey, avg_keys"""
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

            # TODO: LAG() and LEAD(): Calcite issue when get optimized plan

            # TODO: RANK() and DENSE_RANK(): cudf aggs no supported currently

            # ------------ PARTITION BY + ORDER BY ----------------

            queryId = "TEST_21"
            query = """select min(n_nationkey) over 
                            (
                                partition by n_regionkey
                                order by n_name
                            ) min_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_name"""
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

            queryId = "TEST_22"
            query = """select max(n_nationkey) over
                            (
                                partition by n_regionkey
                                order by n_name desc
                            ) max_keys,
                            n_nationkey, n_regionkey
                        from nation order by n_nationkey"""
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

            queryId = "TEST_23"
            query = """select count(n_nationkey) over
                            (
                                partition by n_regionkey
                                order by n_name desc
                            ) count_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_regionkey, count_keys"""
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

            queryId = "TEST_24"
            query = """select sum(n_nationkey) over 
                            (
                                partition by n_regionkey
                                order by n_nationkey desc
                            ) sum_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation"""
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
            
            queryId = "TEST_25"
            query = """select avg(cast(n_nationkey as double)) over 
                            (
                                partition by n_regionkey
                                order by n_name
                            ) avg_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by avg_keys"""
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

            queryId = "TEST_26"
            query = """select row_number() over 
                            (
                                partition by n_regionkey
                                order by n_name desc
                            ) row_num,
                            n_regionkey, n_name
                        from nation order by n_nationkey"""
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

            # TODO: first_value and last_value fails
            queryId = "TEST_27"
            query = """select first_value(n_nationkey) over
                            (
                                partition by n_regionkey
                                order by n_name
                            ) last_val,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_name"""
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

    main(dask_client,
        drill,
        spark,
        Settings.data["TestSettings"]["dataDirectory"],
        bc,
        nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
