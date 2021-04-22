from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Union"


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

            # ------------------ UNION ALL ---------------------

            queryId = "TEST_01"
            query = """(select o_orderkey, o_custkey from orders
                        where o_orderkey < 100
                    )
                    union all
                    (
                        select o_orderkey, o_custkey from orders
                        where o_orderkey < 300
                        and o_orderkey >= 200
                    )"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_02"
            query = """(select o_orderkey, o_custkey from orders
                        where o_orderkey < 100
                    )
                    union all
                    (
                        select o_orderkey, o_custkey from orders
                        where o_orderkey < 300
                        and o_orderkey >= 200
                    )
                    order by 2"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_03"
            query = """(select o_orderkey, o_totalprice as key
                        from orders where o_orderkey < 100
                    )
                    union all
                    (
                        select o_orderkey, o_custkey as key from orders
                        where o_orderkey < 300 and o_orderkey >= 200
                    )"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_04"
            query = """(select o_orderkey, null as keyy, o_totalprice,
                        cast(null as int) as o_totalprice2, null as field5,
                        null as field6 from orders
                        where o_orderkey < 100
                    )
                    union all
                    (
                        select o_orderkey + 100.1 as o_orderkey,
                        o_custkey as keyy, null as o_totalprice,
                        o_totalprice as o_totalprice2, null as field5,
                        cast(null as double) as field6
                        from orders where o_orderkey < 300
                        and o_orderkey >= 200
                    )"""
            query_spark = """(select
                            o_orderkey,
                            cast(null as int) as keyy,
                            o_totalprice,
                            cast(null as double) as o_totalprice2,
                            cast(null as int) as field5,
                            cast(null as double) as field6
                            from orders where o_orderkey < 100
                        )
                        union all
                        (
                            select
                            o_orderkey + 100.1 as o_orderkey,
                            o_custkey as keyy,
                            cast(null as double) as o_totalprice,
                            o_totalprice as o_totalprice2,
                            cast(null as int) as field5,
                            cast(null as double) as field6
                            from orders where o_orderkey < 300
                            and o_orderkey >= 200
                        )"""
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

            queryId = "TEST_05"
            query = """(select o_orderkey, 100.1, o_totalprice,
                        cast(100 as float), 100, 1.1
                        from orders where o_orderkey < 100
                    )
                    union all
                    (
                        select o_orderkey + 100.1 as o_orderkey,
                        o_custkey as keyy, 10000, o_totalprice, 101.1,100
                        from orders where o_orderkey < 300
                        and o_orderkey >= 200
                    )"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_06"
            query = """(select o_orderkey, o_orderstatus, o_orderstatus
                        from orders where o_orderkey < 100
                    )
                    union all
                    (
                        select o_orderkey + 100.1 as o_orderkey,
                        SUBSTRING(o_orderstatus, 2, 4), 'hello work'
                        from orders where o_orderkey < 300
                        and o_orderkey >= 200
                    )"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_07"
            query = """(select o_orderkey, o_custkey from orders
                        where o_orderkey < 100
                    )
                    union all
                    (select o_orderkey, o_custkey from orders
                        where o_orderkey < 300
                        and o_orderkey >= 200) order by 2"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            # ------------------ UNION ---------------------

            queryId = "TEST_08"
            query = """(select o_orderkey, o_custkey from orders
                        where o_orderkey < 100
                    )
                    union
                    (
                        select o_orderkey, o_custkey from orders
                        where o_orderkey < 200 and o_orderkey >= 10
                    )"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_09"
            query = """(select o_orderkey, o_custkey from orders
                        where o_orderkey < 60
                    )
                    union
                    (
                        select o_orderkey, o_custkey from orders
                        where o_orderkey < 200 and o_orderkey >= 10
                    )
                    order by 2"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_10"
            query = """(select o_orderkey, o_orderstatus, o_orderstatus
                        from orders where o_orderkey < 100
                    )
                    union
                    (
                        select o_orderkey + 100.1 as o_orderkey,
                        SUBSTRING(o_orderstatus, 2, 4), 'hello work'
                        from orders where o_orderkey < 300
                        and o_orderkey >= 5
                    )"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_11"
            query = """(select nat1.n_nationkey, nat1.n_name from nation as nat1
                        inner join lineitem on nat1.n_nationkey = mod(l_suppkey, 1010)
                        where nat1.n_name like 'INDIA'
                    ) union
			        ( select nat2.n_nationkey, nat2.n_name from nation as nat2
                        inner join orders on nat2.n_nationkey = mod(o_orderkey, 1010)
                        where nat2.n_name like 'INDIA'
                    )"""
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
            query = """select l_returnflag, l_shipdate, l_linestatus
                        from lineitem
                        where l_orderkey < 100 and l_linenumber < 2
                    union all
                        select l_returnflag, l_shipdate, l_linestatus
                        from lineitem where l_partkey < 1
                        and l_orderkey < 2 and l_linenumber < 2"""
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

            queryId = "TEST_13"
            query = """select o_orderpriority as l_returnflag,
                        o_orderdate as l_shipdate, o_orderstatus as l_linestatus
                    from orders where o_orderkey < 100
                    union all
                    select l_returnflag, l_shipdate, l_linestatus
                    from lineitem where l_orderkey = 3"""
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

            queryId = "TEST_14"
            query = """select o_orderdate as d1, o_orderpriority as s1,
                        o_orderstatus as s2, o_orderkey as l1
                    from orders where o_orderkey < 100
                    union all
                    select o_orderdate as d1, o_orderpriority as s1,
                        o_orderstatus as s2, o_orderkey as l1
                    from orders where o_custkey < 100
                    union all
                    select o_orderdate as d1, o_orderpriority as s1,
                        o_orderstatus as s2, o_orderkey as l1
                    from orders where o_orderstatus = 'O'
                    union all
                    select o_orderdate as d1, o_orderpriority as s1,
                        o_orderstatus as s2, o_orderkey as l1
                    from orders where o_totalprice < 350"""
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
