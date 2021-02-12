from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Case"


def main(dask_client, drill, spark, dir_data_lc, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = [
            "orders",
            "region",
            "lineitem",
            "part",
            "supplier",
            "customer",
            "nation",
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
            query = """select case when o_custkey > 20 then o_orderkey
                    else o_custkey - 20 end
                    from orders where o_orderkey <= 50"""
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
            query = """SELECT l.l_shipmode, sum(case
                        when o.o_orderpriority = '1-URGENT'
                        OR o.o_orderpriority = '2-HIGH' then 1
                        else 0 end) as high_line_count
                    FROM orders o
                    inner join lineitem l on o.o_orderkey = l.l_orderkey
                    WHERE l.l_commitdate < l.l_receiptdate
                    AND l.l_shipdate < l.l_commitdate
                    AND l.l_receiptdate >= date '1994-01-01'
                    GROUP BY l.l_shipmode
                    ORDER BY l.l_shipmode"""
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

            queryId = "TEST_04"
            query = """select r_name, (case when r_name = 'AFRICA' then
                    r_name  else 'AFROBEAT' end) as n_rname
                    from region
                    where ( case when r_name = 'AFRICA' then
                    r_name  else 'AFROBEAT' end ) = 'AFROBEAT'"""
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
            query = """select case when o_custkey > 20 then o_orderkey
                    when o_custkey > 10 then o_custkey - 20 else
                    o_custkey - 10 end from orders where o_orderkey <= 50"""
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
            query = """SELECT l.l_shipmode, sum(case when
                    o.o_orderpriority = '1-URGENT'
                    OR o.o_orderpriority = '2-HIGH'then 1 else 0 end)
                    as high_line_count,
                    sum(case when o.o_orderpriority <> '1-URGENT'
                    AND o.o_orderpriority <> '2-HIGH' then 1 else 0 end)
                    AS low_line_count
                    FROM orders o
                    INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
                    WHERE l.l_commitdate < l.l_receiptdate
                    AND l.l_shipdate < l.l_commitdate
                    AND l.l_receiptdate < date '1994-01-01'
                    GROUP BY l.l_shipmode
                    ORDER BY l.l_shipmode"""
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
            query = """select 100.00 * sum(case when p.p_type = 'PROMO'
                        then l.l_extendedprice*(1-l.l_discount) else 0 end)
                        / sum(l.l_extendedprice * (1 - l.l_discount))
                        as promo_revenue from lineitem l
                        inner join part p on l.l_partkey = p.p_partkey"""
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
            query = """select o_orderkey, o_custkey, case when
                    o_custkey > 10000 then o_orderkey else NULL end
                    from orders where o_orderkey <= 50"""
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
            query = """select o_orderkey, o_custkey,
                    case when o_custkey > 20000 then o_orderkey else null
                    end from orders where o_orderkey <= 30"""
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
            query = """select o_totalprice, o_custkey,
                    case when o_totalprice > 100000.2 then o_totalprice
                    else null end
                    from orders where o_orderkey < 20"""
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
            query = """select n_nationkey, n_regionkey,
                    case when n_nationkey > 10 then n_regionkey else NULL end
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

            queryId = "TEST_13"
            query = """select CASE WHEN mod(l_linenumber,  2) <> 1 THEN 0 ELSE
                    l_quantity END as s, l_linenumber, l_quantity
                    from lineitem
                    order by s, l_linenumber, l_quantity limit 100"""
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
