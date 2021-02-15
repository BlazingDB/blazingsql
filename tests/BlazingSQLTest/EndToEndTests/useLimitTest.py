from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution,  gpuMemory, init_context, skip_test

queryType = "Limit"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["orders", "customer", "partsupp", "lineitem"]
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

            queryId = "TEST_01"
            query = "select o_orderkey from orders order by 1 limit 10"
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
            query = """select o_orderdate, o_orderkey, o_clerk from orders
                    order by o_orderdate, o_orderkey, o_custkey,
                        o_orderstatus, o_clerk
                    limit 1000"""
            query_spark = """select o_orderdate, o_orderkey, o_clerk from orders
                    order by o_orderdate nulls last, o_orderkey nulls last,
                    o_custkey nulls last, o_orderstatus nulls last,
                    o_clerk nulls last limit 1000"""
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
                    query_spark=query_spark,
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

            queryId = "TEST_03"
            query = """select o_orderkey from orders
                    where o_custkey < 300 and o_orderdate >= '1990-08-01'
                    order by o_orderkey limit 50"""
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
            query = """select ps_partkey, ps_availqty from partsupp
                    where ps_availqty < 3 and ps_availqty >= 1
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

            queryId = 'TEST_05'
            query = """select o_orderkey, o_orderstatus from orders
                    where o_custkey < 10 and o_orderstatus = 'O'
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

            queryId = "TEST_06"
            query = """select orders.o_totalprice, customer.c_name from orders
                      inner join customer
                      on orders.o_custkey = customer.c_custkey
                      order by customer.c_name, orders.o_orderkey limit 10"""
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
            query = """(select l_shipdate, l_orderkey, l_linestatus
                    from lineitem where l_linenumber = 1
                    order by 1, 2, 3, l_linenumber limit 10)
                    union all
                    (select l_shipdate, l_orderkey, l_linestatus
                    from lineitem where l_linenumber = 1
                    order by 1 desc, 2, 3, l_linenumber limit 10)"""
            query_spark = """(select l_shipdate, l_orderkey, l_linestatus
                    from lineitem where l_linenumber = 1
                    order by 1 nulls last, 2 nulls last, 3 nulls last,
                    l_linenumber nulls last limit 10)
                    union all
                    (select l_shipdate, l_orderkey, l_linestatus
                    from lineitem where l_linenumber = 1
                    order by 1 desc nulls first, 2 nulls last, 3 nulls last,
                    l_linenumber nulls last limit 10)"""

            if fileSchemaType == DataType.ORC:
                runTest.run_query(
                    bc,
                    spark,
                    query,
                    queryId,
                    queryType,
                    1,
                    "",
                    acceptable_difference,
                    use_percentage,
                    fileSchemaType,
                    query_spark=query_spark,
                )
            else:
                runTest.run_query(
                    bc,
                    drill,
                    query,
                    queryId,
                    queryType,
                    1,
                    "",
                    acceptable_difference,
                    use_percentage,
                    fileSchemaType,
                )

            queryId = "TEST_08"
            query = """select c_custkey from customer
                    where c_custkey < 0 order by c_custkey limit 40"""
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
            query = """select c_custkey, c_name from customer
                    where c_custkey < 10 order by 1 limit 30"""
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
            query = """select c_custkey, c_name from customer
                    where c_custkey < 10 limit 30"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                1,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_11"
            query = """select avg(CAST(c_custkey AS DOUBLE)), min(c_custkey)
                    from customer limit 5"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                1,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_12"
            query = """select o_orderdate, o_orderkey, o_clerk
                    from orders
                    order by o_orderdate, o_orderkey, o_custkey
                    limit 1000"""
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

            # if Settings.execution_mode == ExecutionMode.GENERATOR:
            #     print("==============================")
            #     break

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
