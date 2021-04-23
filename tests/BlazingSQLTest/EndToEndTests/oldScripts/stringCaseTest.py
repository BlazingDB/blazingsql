from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test


def main(dask_client, drill, spark, dir_data_file, bc, nRals):
    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "String case"

    def executionTest():

        tables = ["customer", "nation", "orders"]
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
            query = """select c_custkey, UPPER(c_name) from customer
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

            queryId = "TEST_02"
            query = """select c_custkey, upper(c_comment)
                    from customer where c_mktsegment = 'household'"""
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
            query = """select LOWER(c_name), UPPER(c_address)
                    from customer where c_custkey < 42"""
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

            queryId = "TEST_04"
            query = """select c.c_custkey, UPPER(SUBSTRING(c.c_name, 1, 8)),
                        LOWER(SUBSTRING(c.c_name, 10, 18)) || '**'
                    from customer c where c.c_custkey < 20"""
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
            query = """select o_orderkey, upper(o_comment), lower(o_orderstatus)
                    from orders where o_custkey < 120"""
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
            query = """select lower(o_orderpriority), lower(o_orderstatus)
                    from orders group by o_orderpriority, o_orderstatus"""
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
            query = """select count(o_orderkey), sum(o_orderkey), lower(o_clerk)
                    from orders where o_custkey < 1000
                    group by o_clerk, o_orderstatus"""
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
            query = """select count(o_orderkey), sum(o_orderkey), upper(o_clerk)
                    from orders where o_custkey < 1000
                    group by o_clerk, o_orderstatus"""
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

            queryId = "TEST_09"
            query = """select LOWER(n_name), UPPER(n_comment) from nation
                    where n_regionkey = 4"""
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
            query = """select upper(n_comment), lower(upper(n_comment)) from nation
                    where n_nationkey between 5 and 15"""
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

            # NOTE: SUBSTRING() drill API, `start` parameter must be at least 1
            # If 0 was provided then will return an empty string ''
            # finally if you want to use `start` value to 0, please use `spark` engine
            queryId = "TEST_11"
            query = """select INITCAP(LOWER(o_clerk)) as init_a, 
                    INITCAP(SUBSTRING(o_comment, 0, 10)) || INITCAP(o_orderstatus) as init_b
                    from orders where MOD(o_custkey, 2) = 0 
                    order by init_a nulls last, init_b nulls last
                    limit 180"""
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

            queryId = "TEST_12"
            query = """select o_orderkey, 
                    INITCAP(SUBSTRING(o_comment, 1, 5)) || SUBSTRING(INITCAP(o_comment), 2, 5)
                    from orders where o_orderkey < 10000"""
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
            query = """SELECT INITCAP(LOWER(c_mktsegment)) as init_mkt, 
                    INITCAP(SUBSTRING(c_comment, 0, 7)) as init_comment,
                    INITCAP(c_name) as init_name
                    from customer
                    where c_mktsegment in ('AUTOMOBILE', 'HOUSEHOLD')
                    and 50.0 <= c_acctbal
                    order by c_custkey nulls last, init_comment nulls last
                    limit 200"""
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
