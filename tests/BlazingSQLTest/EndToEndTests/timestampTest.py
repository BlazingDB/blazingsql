from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Timestamp"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():

        tables = ["lineitem", "orders", "nation", "region", "customer"]
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
            query = """select * from orders
                    where o_orderdate > TIMESTAMP '1997-09-05 19:00:00'
                    order by o_orderkey limit 10"""

            # TODO: Failed test with nulls
            testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
            if testsWithNulls != "true":
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

            queryId = "TEST_02"
            query = """select l_suppkey, l_shipdate from lineitem
                where l_shipdate < TIMESTAMP '1993-01-01 10:12:48'
                and l_suppkey < 100 order by l_orderkey"""
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

            queryId = "TEST_03"
            query = """select o_orderkey, o_orderdate from orders
                    where o_orderdate >= TIMESTAMP '1997-09-05 19:00:00'
                    order by o_orderkey limit 20"""

            # TODO: Failed test with nulls
            testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
            if testsWithNulls != "true":
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
            query = """select orders.o_orderkey, orders.o_orderdate,
                        orders.o_orderstatus, lineitem.l_receiptdate
                    from orders inner join lineitem
                    on lineitem.l_orderkey = orders.o_orderkey
                    where orders.o_orderkey < 70 and lineitem.l_orderkey < 120
                    and orders.o_orderdate < TIMESTAMP '1992-01-01 10:12:48'
                    order by orders.o_orderkey, lineitem.l_linenumber,
                    orders.o_custkey, lineitem.l_orderkey"""
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

            queryId = "TEST_05"
            query = """select customer.c_nationkey, customer.c_name,
                        orders.o_orderdate
                    from customer left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    inner join lineitem
                    on lineitem.l_orderkey = orders.o_orderkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 100
                    and orders.o_orderdate < '1990-01-01 20:00:00'
                    order by orders.o_orderkey, lineitem.l_linenumber"""
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

            queryId = "TEST_06"
            query = """select orders.o_orderkey, orders.o_orderdate,
                    orders.o_orderstatus
                    from orders inner join lineitem
                    on lineitem.l_orderkey = orders.o_orderkey
                    where orders.o_orderkey < 20
                    and lineitem.l_orderkey < 16
                    order by orders.o_orderkey"""
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

            queryId = "TEST_07"
            query = """select cast(o_orderdate as timestamp)
                    from orders order by o_orderkey limit 5"""
            query_spark = """select cast(o_orderdate as timestamp)
                    from orders order by o_orderkey nulls last limit 5"""
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
                query_spark=query_spark,
            )

            queryId = "TEST_08"
            query = """select o_orderkey, cast(o_orderdate as timestamp)
                    from orders where o_orderdate = date '1996-12-01'
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

            queryId = "TEST_09"
            query = """with dateTemp as (
                    select o_orderdate from orders
                    where o_orderdate > TIMESTAMP '1960-05-05 12:34:55'
                    order by o_orderkey
                    ) select count(*) from dateTemp"""
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
            query = """with dateTemp as (
                    select o_orderdate from orders
                    where o_orderdate <= TIMESTAMP '1996-12-01 00:00:00'
                    order by o_orderdate, o_orderkey limit 5
                    ) select o_orderdate as myDate from dateTemp
                    order by o_orderdate desc"""
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
            query = """with dateTemp as (
                    select o_orderdate from orders
                    where o_orderdate = TIMESTAMP '1996-12-01 19:00:00'
                    order by o_orderkey) select count(*) from dateTemp"""
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
            query = """select o_orderkey, o_orderdate from orders
                    where o_orderdate = date '1996-12-01'
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
