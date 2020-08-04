from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from pyspark.sql import SparkSession
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Bindable Alias"

    def executionTest(queryType):
        tables = ["nation", "region", "lineitem", "orders"]
        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
        ]  # TODO ORC gdf parquet json

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            # Run Query ------------------------------------------------------
            # Parameter to indicate if its necessary to order the
            # resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select n_regionkey as rkey, n_nationkey from nation
            where n_regionkey < 3"""
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

            queryId = "TEST_02"
            query = """select n_nationkey, n_regionkey as nr from nation
                    where n_regionkey < 3"""
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
            query = """select n_regionkey as rkey, n_nationkey from nation
                    where n_regionkey < 3 and n_nationkey > 5"""
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
            query = """select n_nationkey as nkey, n_comment from nation
                    where n_nationkey > 5"""
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
            query = "select n_regionkey as rkey, n_nationkey from nation"
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
            query = """select n_regionkey as rkey, n_nationkey as nkey
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

            queryId = "TEST_07"
            query = """select n_nationkey as nkey, n_regionkey as rkey
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

            queryId = "TEST_08"
            query = """(select r_name as name, r_regionkey as key from region )
                    union all
                    (select n_name as name, n_nationkey as key from nation )"""
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
            query = """select o_orderkey as okey, o_custkey as ckey,
                    o_orderdate as odate from orders
                    where o_orderpriority = '1-URGENT'
                    order by okey, ckey, odate"""
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

            queryId = "TEST_10"
            query = """select (l_quantity + 2) as new_quantity from lineitem
                    group by l_quantity order by l_quantity asc limit 30"""
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
            query = """select count(o_orderkey) as count_ok from orders
                    group by o_orderstatus"""
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
            query = "select n_nationkey from nation where n_regionkey < 3"
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

            # if Settings.execution_mode == ExecutionMode.GENERATOR:
            #     print("==============================")
            #     break

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

    if ((Settings.execution_mode == ExecutionMode.FULL and
         compareResults == "true") or
            Settings.execution_mode == ExecutionMode.GENERATOR):
        # Create Table Drill ------------------------------------------------
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

        # Create Table Spark -------------------------------------------------
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
