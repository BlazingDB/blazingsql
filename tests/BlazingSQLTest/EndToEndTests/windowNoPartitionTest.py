from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Window Functions With No Partition"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["orders", "nation", "lineitem", "customer", "region"]
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

          
            # ------------ ROWS bounding ----------------

            queryId = "TEST_01"
            query = """select min(n_nationkey) over
                            (
                                order by n_name
                                ROWS BETWEEN 1 PRECEDING
                                AND 1 FOLLOWING
                            ) min_val,
 							n_nationkey, n_regionkey, n_name
                        from nation"""
            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                0,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType
            )

            queryId = "TEST_02"
            query = """select min(o_orderkey) over
                            (
                                order by o_totalprice
                                ROWS BETWEEN 2 PRECEDING
                                AND 1 FOLLOWING
                            ) min_keys, 
                            max(o_orderkey) over
                            (
                                order by o_totalprice
                                ROWS BETWEEN 2 PRECEDING
                                AND 1 FOLLOWING
                            ) max_keys, o_orderkey, o_orderpriority
                        from orders
                        where o_orderpriority <> '2-HIGH'
                        and o_clerk = 'Clerk#000000880'
                        and o_orderstatus is not null
                        and o_totalprice is not null
                        order by o_orderstatus, o_totalprice
                        limit 50"""
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
                fileSchemaType
            )

            queryId = "TEST_03"
            query = """with new_nation as (
                    select n.n_nationkey as n_natio1,
                        n.n_name as n_nam1,
                        n.n_regionkey as n_region1
                    from nation as n
                    inner join region as r
                    on n.n_nationkey = r.r_regionkey
                )
                select avg(cast(nn.n_natio1 as double)) over 
                    (
                        order by nn.n_nam1
                        ROWS BETWEEN 3 PRECEDING
                        AND 2 FOLLOWING
                    ) avg_keys,
                    nn.n_natio1, nn.n_nam1, nn.n_region1
                from new_nation nn
                order by nn.n_natio1, avg_keys"""
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
                fileSchemaType
            )

            queryId = "TEST_04"
            query = """select max(l_partkey) over
                            (
                                order by l_extendedprice desc, l_orderkey, l_quantity
                                ROWS BETWEEN 6 PRECEDING
                                AND 2 FOLLOWING
                            ) max_keys,
                            l_linestatus, l_extendedprice
                        from lineitem
                        where l_shipmode not in ('MAIL', 'SHIP', 'AIR')
                        and l_linestatus = 'F'
                        and l_extendedprice is not null
                        order by l_extendedprice, l_orderkey, max_keys
                        limit 50"""

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
            query = """select  l.l_orderkey, l.l_linenumber, l.l_suppkey, l.l_partkey, c.c_custkey,
                            max(l.l_partkey) over
                            (
                                order by l.l_orderkey, l.l_linenumber, l.l_suppkey
                                ROWS BETWEEN 6 PRECEDING
                                AND 2 FOLLOWING
                            ) max_pkeys,
                            max(c.c_custkey) over
                            (
                                order by  l.l_orderkey, l.l_linenumber, l.l_suppkey
                                ROWS BETWEEN 6 PRECEDING
                                AND 2 FOLLOWING
                            ) max_cust
                            
                        from lineitem as l
                        inner join orders as o on o.o_orderkey = l.l_orderkey
                        inner join customer as c on c.c_custkey = o.o_custkey
                        where l.l_quantity = 1 and c.c_custkey < 10000
                        order by l.l_orderkey, l.l_linenumber, l.l_partkey, c.c_custkey                        
                        """

            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                0,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType                
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
