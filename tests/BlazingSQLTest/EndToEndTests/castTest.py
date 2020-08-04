from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from pyspark.sql import SparkSession
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Cast"


def main(dask_client, drill, spark, dir_data_lc, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = [
            "nation",
            "region",
            "supplier",
            "customer",
            "lineitem",
            "orders",
            "part",
        ]
        data_types = [
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
        ]  # TODO parquet json

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_lc, fileSchemaType, tables=tables)

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
            query = """select p_partkey, p_retailprice,
                    cast(cast(p_retailprice as VARCHAR) as DOUBLE)
                    from part order by p_partkey limit 10"""
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
            query = """select CAST(c_custkey as BIGINT), c_acctbal
                    from customer order by c_acctbal desc, c_custkey"""
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
            query = """select SUM(c_custkey), CAST(c_custkey as VARCHAR)
                    from customer where c_custkey between 123 and 125
                    group by c_custkey"""
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
            query = """select cast(o_totalprice AS DOUBLE) * o_orderkey
                    from orders where o_orderkey between 990 and 1010"""
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
            query = """select o_custkey, o_orderkey,
                    cast(o_custkey AS FLOAT) * o_orderkey from orders
                    where o_custkey between 998 and 1000
                    order by o_custkey, o_orderkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                acceptable_difference,
                True,
                fileSchemaType,
            )

            queryId = "TEST_06"
            query = """select cast(c_nationkey AS INTEGER) from customer
                where c_custkey < 100 and c_nationkey in (19, 20)"""
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
            query = """select cast(o_orderkey AS FLOAT) * o_totalprice from
                    orders where o_orderkey < 10 order by o_orderkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                acceptable_difference,
                True,
                fileSchemaType,
            )

            queryId = "TEST_08"
            query = """select cast(o_orderkey AS TINYINT) from orders
                    where o_orderkey < 120"""
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
            query = """select cast(o_orderkey AS SMALLINT) from orders
                    where o_orderkey < 32000"""
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
            query = """select cast(o_totalprice AS INTEGER) * o_orderkey
                    from orders where o_orderkey < 10"""
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
            query = """select cast(o_orderdate AS TIMESTAMP)
                    from orders where o_orderkey < 10"""
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

            # TODO: FIx cast(o_orderdate AS DATE) when fileSchemaType is ORC
            queryId = "TEST_12"
            query = """select cast(o_orderdate AS TIMESTAMP) from orders
                    where cast(o_orderdate as TIMESTAMP)
                    between '1995-01-01' and '1995-01-05'"""
            if fileSchemaType != DataType.ORC:
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

            queryId = 'TEST_13'
            query = """select cast(o_totalprice AS INTEGER), 4 as scan_type, 4.0 as scan_type2, 
                        5 as scan_type3, o_comment, o_comment as comento, 'hello' as greeting 
                        from orders where o_orderkey < 10"""
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
