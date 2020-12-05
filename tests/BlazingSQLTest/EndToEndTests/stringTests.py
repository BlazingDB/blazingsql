from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Simple String"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):
    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["customer", "orders", "nation", "region"]
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
            query = """select o_orderkey, sum(o_totalprice)/count(o_orderstatus)
                    from orders where o_custkey < 100
                    group by o_orderstatus, o_orderkey"""
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
            query = """select o_orderkey, o_orderstatus
                from orders where o_custkey < 10
                and o_orderstatus <> 'O'
                order by o_orderkey, o_orderstatus limit 50"""
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
            query = """select count(o_orderstatus)
                    from orders where o_orderstatus <> 'O'"""
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
            query = """select count(o_orderkey), sum(o_orderkey), o_clerk
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

            queryId = "TEST_05"
            query = """select avg(CAST(o_orderkey AS DOUBLE))
                    from orders group by o_orderstatus"""
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
            query = """select count(o_shippriority), sum(o_totalprice)
                    from orders group by o_shippriority"""
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
            query = """with regionTemp as (
                        select r_regionkey, r_name
                        from region where r_regionkey > 2
                    ), nationTemp as (
                        select n_nationkey, n_regionkey as fkey, n_name
                        from nation where n_nationkey > 3
                        order by n_nationkey
                    )
                    select regionTemp.r_name, nationTemp.n_name
                    from regionTemp inner join nationTemp
                    on regionTemp.r_regionkey = nationTemp.fkey"""
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
            query = """select c_custkey, CHAR_LENGTH(c_comment)
                    from customer where MOD(CHAR_LENGTH(c_comment), 7) = 0"""
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
            query = "select sum(CHAR_LENGTH(c_comment)) from customer"
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
            query = """SELECT
                REPLACE(c_comment, 'in', '') as empty_replace,
                REPLACE(c_comment, 'the', '&&') as and_replace,
                REPLACE(c_comment, 'a', '$e*u') as a_replace
                FROM customer
                """
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

            queryId = "TEST_11"
            query = """SELECT
                REPLACE(SUBSTRING(c_comment, 2, 10), 'e', '&&') as rep_sub,
                CAST(REPLACE(c_comment, 'a', 'e') LIKE '%the%' AS INT) as rep_like,
                SUBSTRING(REPLACE(c_comment, 'e', '&#'), 2, 30) as sub_rep
                FROM customer
                """
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
            query = """SELECT
                TRIM(c_comment) as both_space_trim,
                TRIM(LEADING 'Cu' FROM c_name) as leading_char_trim,
                TRIM(TRAILING 'E' FROM c_mktsegment) as trailing_char_trim,
                LTRIM(c_comment) as left_trim,
                RTRIM(c_comment) as right_trim
                FROM customer
                """
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

            queryId = "TEST_13"
            query = """SELECT
                TRIM(TRAILING 'er' FROM SUBSTRING(c_name, 0, 7)) as trim_subs,
                TRIM(LEADING 'CU' FROM UPPER(c_name)) as trim_upper,
                LOWER(TRIM('AE' FROM c_mktsegment)) as lower_trim,
                LTRIM(REPLACE(c_name, 'Customer', '   Test')) as ltrim_replace,
                CAST(RTRIM(c_comment) LIKE '%the%' AS INT) as rtrim_like
                FROM customer
                """
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

            queryId = "TEST_14"
            query = """SELECT
                REVERSE(c_comment) as reversed_c,
                SUBSTRING(REVERSE(c_comment), 2, 10) as sub_reversed_c,
                CAST(SUBSTRING(REVERSE(c_comment), 2, 10) LIKE '%or%' AS INT) as cast_sub_reversed_c
                FROM customer
                """
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

            queryId = "TEST_15"
            query = """SELECT
                COUNT(SUBSTRING(REVERSE(c_comment), 2, 10)) as reverse_subbed_count
                FROM customer
                GROUP BY SUBSTRING(REVERSE(c_comment), 2, 10)
                """
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

        # Create Table Spark ------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark, Settings.data["TestSettings"]["dataDirectory"])

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
