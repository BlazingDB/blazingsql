from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Non-EquiJoin Queries"

    def executionTest(queryType):

        tables = [
            "lineitem",
            "orders",
            "part",
            "partsupp",
            "customer",
            "nation",
            "supplier",
        ]
        data_types = [
            # DataType.DASK_CUDF,
            # DataType.CUDF,
            # DataType.CSV,
            # DataType.ORC,
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
            acceptable_difference = 0.001

            print("==============================")
            print(queryType)
            print("==============================")

            # queryId = "TEST_01"
            # query = """ select l.l_orderkey, l.l_linenumber from lineitem as l
            #         inner join orders as o on l.l_orderkey = o.o_orderkey
            #         and l.l_commitdate < o.o_orderdate
            #         and l.l_receiptdate > o.o_orderdate"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_02"
            # query = """ select l.l_orderkey, l.l_linenumber from lineitem as l
            #         inner join orders as o
            #         on l.l_commitdate > o.o_orderdate
            #         and l.l_orderkey = o.o_orderkey """
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_03"
            # query = """ select l.l_orderkey, l.l_linenumber from lineitem as l
            #             inner join orders as o on l.l_orderkey = o.o_orderkey
            #             where o.o_orderkey < 10000 and
            #             (o.o_custkey > 1000 or l.l_partkey < 10000)"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_04"
            # query = """ select l.l_orderkey, l.l_linenumber from lineitem as l
            #             inner join orders as o
            #             on l.l_receiptdate > o.o_orderdate
            #             and l.l_orderkey = o.o_orderkey
            #             where o.o_orderkey < 100000
            #             and (o.o_custkey > 1000 or l.l_partkey < 10000)"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_05"
            # query = """ select p.p_brand, p.p_type, p.p_size,
            #                 count(ps.ps_suppkey) as supplier_cnt
            #             from partsupp ps
            #             inner join part p on p.p_partkey = ps.ps_partkey
            #             where
            #                 p.p_brand <> 'Brand#45'
            #                 and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
            #                 and ps.ps_supplycost < p.p_retailprice
            #             group by
            #                 p.p_brand, p.p_type, p.p_size
            #             order by
            #                 supplier_cnt desc, p.p_brand, p.p_type, p.p_size"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_06"
            # query = """ select sum(l.l_extendedprice) / 12.0 as avg_yearly
            #             from lineitem l
            #             inner join part p on p.p_partkey = l.l_partkey
            #             inner join partsupp ps on ps.ps_partkey = l.l_partkey
            #             where
            #             (
            #                 p.p_brand = 'Brand#23'
            #                 or p.p_container = 'MED BOX'
            #             )
            #             or l.l_quantity < ps.ps_supplycost """
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_07"
            # query = """ select sum(c.c_custkey) / count(c.c_custkey),
            #                 sum(c.c_nationkey) / count(c.c_nationkey),
            #                 n.n_regionkey
            #             from customer as c
            #             inner join nation as n
            #             on c.c_nationkey = n.n_nationkey
            #             inner join supplier as s
            #             on c.c_nationkey = s.s_nationkey
            #             where c.c_acctbal <= s.s_acctbal
            #             and (
            #                 s.s_suppkey >= c.c_nationkey
            #                 OR s.s_suppkey < 1289
            #             )
            #             group by n.n_regionkey"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_08"
            # query = """
            #     SELECT SUM(c.c_acctbal)
            #     FROM
            #         orders o,
            #         customer c
            #     WHERE c.c_custkey = o.o_custkey
            #     AND o.o_orderstatus = 'F'
            #     AND
            #     (
            #         (
            #             o.o_orderpriority = '2-HIGH'
            #             AND 100.0 <= c.c_acctbal
            #         )
            #         OR
            #         (
            #             o.o_orderpriority = '2-HIGH'
            #             AND 50.0 <= c.c_acctbal
            #         )
            #         OR
            #         (
            #             o.o_orderpriority = '2-HIGH'
            #             AND 150.0 <= c.c_acctbal
            #         )
            #     )
            #     """
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     0.01,
            #     use_percentage,
            #     fileSchemaType,
            # )
            
            # queryId = "TEST_09"
            # query = """ select o1.o_orderkey as okey1, o2.o_orderkey as okey2 from orders as o1
            #         left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey
            #         and o1.o_orderkey < 10000"""
            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # # fail
            # queryId = "TEST_10"
            # query = """ select o1.o_orderkey as okey1, o2.o_orderkey as okey2 from orders as o1
            #         left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey
            #         and o2.o_orderkey < 10000"""
            # print(bc.explain(query))
            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # # fail
            # queryId = "TEST_11"
            # query = """select o1.o_orderkey as okey1, o2.o_orderkey as okey2, o2.o_custkey from orders as o1
            #         left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey
            #         and o1.o_orderkey < o2.o_custkey
            #         """
            # print(bc.explain(query))
            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_12"
            # query = """select o1.o_orderkey as okey1, o2.o_orderkey as okey2, o2.o_custkey from orders as o1
            #         left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey + 6
            #         and o1.o_clerk < o2.o_clerk
            #         """
            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # smallest fail case
            queryId = "TEST_13"
            query = """select o1.o_orderkey as okey1, o1.o_custkey as ocust1, o1.o_orderkey - o1.o_custkey as diffy, 
                    o2.o_orderkey as okey2, o2.o_custkey as ocust2, o2.o_orderkey + o2.o_custkey as summy from orders as o1
                    left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey + o2.o_custkey
                    and o1.o_orderkey - o1.o_custkey < o2.o_orderkey 
                    where o1.o_orderkey between 3000 and 3050
                    """
            print(bc.explain(query))
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
                print_result=True
            )

            # queryId = "TEST_13"
            # query = """select o1.o_orderkey as okey1, o2.o_orderkey as okey2, o2.o_custkey from orders as o1
            #         left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey + o2.o_custkey
            #         and o1.o_orderkey - o1.o_custkey < o2.o_orderkey 
            #         where o1.o_orderkey between 3050 and 3100
            #         """
            # print(bc.explain(query))
            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_13"
            # query = """select o1.o_orderkey as okey1, o2.o_orderkey as okey2, o2.o_custkey from orders as o1
            #         left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey + o2.o_custkey
            #         and o1.o_orderkey - o1.o_custkey < o2.o_orderkey 
            #         where o1.o_orderkey between 3100 and 3150
            #         """
            # print(bc.explain(query))
            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_13"
            # query = """select o1.o_orderkey as okey1, o2.o_orderkey as okey2, o2.o_custkey from orders as o1
            #         left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey + o2.o_custkey
            #         and o1.o_orderkey - o1.o_custkey < o2.o_orderkey 
            #         where o1.o_orderkey between 3150 and 3200
            #         """
            # print(bc.explain(query))
            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            # queryId = "TEST_13"
            # query = """select o1.o_orderkey as okey1, o2.o_orderkey as okey2, o2.o_custkey from orders as o1
            #         left outer join orders as o2 on o1.o_orderkey = o2.o_orderkey + o2.o_custkey
            #         and o1.o_orderkey - o1.o_custkey < o2.o_orderkey 
            #         where o1.o_orderkey between 3200 and 3250
            #         """
            # print(bc.explain(query))
            # runTest.run_query(
            #     bc,
            #     spark,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )


            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break

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
        print("starting drill")
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

    main(dask_client, drill, spark, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
