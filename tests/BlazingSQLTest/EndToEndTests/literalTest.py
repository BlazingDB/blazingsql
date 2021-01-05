from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Literal"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["orders", "customer"]
        data_types = [DataType.ORC]  # TODO gdf csv parquet json

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
            query = """select 2, o_orderdate
                    from orders
                    order by o_orderdate asc
                    limit 5"""
            query_spark = """select 2, o_orderdate
                    from orders
                    order by o_orderdate nulls last
                    limit 5"""
            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                worder,
                "o_orderdate",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                query_spark=query_spark,
            )

            queryId = "TEST_02"
            query = """select 'Rommel',c_name from customer
                    order by c_name limit 5"""
            query_spark = """select 'Rommel',c_name from customer
                    order by c_name nulls last limit 5"""
            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                worder,
                "c_name",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                query_spark=query_spark,
            )

            queryId = "TEST_03"
            query = """select '1990-01-01', c_custkey from customer
                    order by c_custkey limit 5"""
            query_spark = """select '1990-01-01', c_custkey from customer
                    order by c_custkey nulls last limit 5"""
            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                query_spark=query_spark,
            )

            queryId = "TEST_04"
            query = """select timestamp '1990-01-01 00:00:00', c_custkey
                    from customer order by c_custkey limit 5"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                True,
                fileSchemaType,
            )  # TODO: Change sum/count for avg KC

            queryId = "TEST_05"
            query = """select 80000 as constant, c_custkey
                    from customer order by c_custkey limit 5"""
            query_spark = """select 80000 as constant, c_custkey
                    from customer order by c_custkey nulls last limit 5"""
            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                query_spark=query_spark,
            )

            queryId = "TEST_06"
            query = """select 2+2, o_orderdate from orders
                    order by o_orderkey limit 5"""
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
