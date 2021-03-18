from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Dir"


# TODO percy use new cs.create_tables adapt to send wildcard or dir
def main(dask_client, drill, spark, dir_data_lc, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = [
            "nation",
            "region",
            "supplier",
            "customer",
            "part",
            "lineitem",
            "orders",
            "partsupp",
        ]
        data_types = [DataType.CUDF]  # TODO csv orc parquet json

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

            queryId = "TEST_00"
            query = """select count(p_partkey), sum(p_partkey),
                    max(p_partkey), min(p_partkey) from part"""
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

            queryId = "TEST_01"
            query = """select count(c_custkey) as c1, count(c_acctbal) as c2
                    from customer"""
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
            query = """select count(n_nationkey), count(n_regionkey)
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

            queryId = "TEST_03"
            query = """select count(s_suppkey), count(s_nationkey)
                    from supplier"""
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
            query = """select count(c_custkey), sum(c_acctbal),
                    sum(c_acctbal)/count(c_acctbal), min(c_custkey),
                    max(c_nationkey), (max(c_nationkey) + min(c_nationkey))/2
                    c_nationkey from customer
                    where c_custkey < 100 group by c_nationkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                True,
                fileSchemaType,
            )

            queryId = "TEST_05"
            query = """select c.c_custkey, c.c_nationkey, n.n_regionkey
                    from customer as c inner join nation as n
                    on c.c_nationkey = n.n_nationkey
                    where n.n_regionkey = 1 and c.c_custkey < 50"""
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
            query = """select c_custkey, c_nationkey, c_acctbal from customer
                    order by c_nationkey, c_custkey, c_acctbal"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_07"
            query = """select c_custkey + c_nationkey, c_acctbal
                    from customer order by 1, 2"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_08"
            query = """select n1.n_nationkey as supp_nation,
                    n2.n_nationkey as cust_nation,
                    l.l_extendedprice * l.l_discount from supplier as s
                    inner join lineitem as l on s.s_suppkey = l.l_suppkey
                    inner join orders as o on o.o_orderkey = l.l_orderkey
                    inner join customer as c on c.c_custkey = o.o_custkey
                    inner join nation as n1 on s.s_nationkey = n1.n_nationkey
                    inner join nation as n2 on c.c_nationkey = n2.n_nationkey
                    where n1.n_nationkey = 1 and n2.n_nationkey = 2
                    and o.o_orderkey < 10000"""
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
            query = """select c_custkey, c_nationkey as nkey from customer
                    where c_custkey < 0 and c_nationkey >=30"""
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
            query = """select sin(c_acctbal), cos(c_acctbal), asin(c_acctbal),
                    acos(c_acctbal), ln(c_acctbal), tan(c_acctbal),
                    atan(c_acctbal), floor(c_acctbal), c_acctbal
                    from customer"""
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
            query = """select n1.n_nationkey as n1key, n2.n_nationkey as n2key,
                    n1.n_nationkey + n2.n_nationkey from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    where n1.n_nationkey < 10 and n1.n_nationkey > 5"""
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
            query = """select count(n1.n_nationkey) as n1key,
                    count(n2.n_nationkey) as n2key,
                    count(*) as cstar
                    from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6"""
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
            query = """select o_orderkey, o_custkey from orders
                    where o_orderkey < 10 and o_orderkey >= 1"""
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
            query = """select 100168549 - avg(o_orderkey),
                    56410984 / sum(o_totalprice), (123 - 945/max(o_orderkey)) /
                    (avg(81619.0/o_orderkey))
                    from orders"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                True,
                fileSchemaType,
            )

            queryId = "TEST_15"
            query = """select o_orderkey, sum(o_totalprice) /
                    count(o_orderstatus) from orders
                    where o_custkey < 100
                    group by o_orderstatus, o_orderkey"""
            runTest.run_query(
                bc,
                spark, #because Drill outputs some inf's instead of NaN
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_16"
            query = """select o_orderkey, o_orderstatus from orders
                    where o_custkey < 10 and o_orderstatus <> 'O'
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

            queryId = "TEST_17"
            query = """select count(o_orderstatus) from orders
                    where o_orderstatus <> 'O'"""
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

            queryId = "TEST_18"
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
                True,
                fileSchemaType,
            )

            queryId = "TEST_19"
            query = """select sum(o_orderkey)/count(o_orderkey)
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
                True,
                fileSchemaType,
            )

            queryId = "TEST_20"
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

            queryId = "TEST_21"
            query = """with regionTemp as (
                        select r_regionkey, r_name
                        from region where r_regionkey > 2
                    ), nationTemp as (
                        select n_nationkey, n_regionkey as fkey, n_name
                        from nation where n_nationkey > 3 order by n_nationkey
                    )
                    select regionTemp.r_name, nationTemp.n_name from regionTemp
                    inner join nationTemp
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

            queryId = "TEST_22"
            # such change without casting generates an issue: Different values
            # #BLZ:
            #    partKey  avgSize
            # 0      1.0     25.0
            # 1      1.0     25.0
            # 2      1.0     25.0
            # 3      1.0     25.0
            # #DRILL:
            #    partKey   avgSize
            # 0      1.0  25.36675
            # 1      1.0  25.36675
            # 2      1.0  25.36675
            # 3      1.0  25.36675

            query = """select partSuppTemp.partKey, partAnalysis.avgSize
                    from (
                        select min(p_partkey) as partKey,
                        avg(cast(p_size as float)) as avgSize,
                        max(p_retailprice) as maxPrice,
                        min(p_retailprice) as minPrice from part
                    ) as partAnalysis
                    inner join (
                        select ps_partkey as partKey,
                        ps_suppkey as suppKey from partsupp
                        where ps_availqty > 2
                    ) as partSuppTemp
                    on partAnalysis.partKey = partSuppTemp.partKey
                    inner join (
                        select s_suppkey as suppKey from supplier
                    ) as supplierTemp on
                    supplierTemp.suppKey = partSuppTemp.suppKey"""
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

        # Create Table Spark ------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark, Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill, spark,
         Settings.data["TestSettings"]["dataDirectory"], bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
