from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Json tests"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["customer", "part", "region", "nation", "orders", "supplier", "partsupp"]
        data_types = [DataType.JSON]

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            # Run Query -----------------------------------------------------
            # Parameter to indicate if its necessary to order the
            # resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select MIN(n.n_nationkey), MAX(r.r_regionkey),
                    AVG(n.n_nationkey + r.r_regionkey) from nation as n
                    left outer join region as r
                    on n.n_nationkey = r.r_regionkey"""
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
            query = """select SUM(n1.n_nationkey) as n1key,
                    AVG(n2.n_nationkey +  n1.n_nationkey ) as n2key
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 10"""
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

            queryId = "TEST_04"
            query = """select cast(o_orderdate AS TIMESTAMP) from orders
                    where cast(o_orderdate as TIMESTAMP)
                    between '1995-01-01' and '1995-01-05'"""
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
            query = """ WITH
                t1_l AS ( SELECT * FROM orders ),
                t1_r AS ( SELECT * FROM customer ),
                main_lr AS(
                    SELECT
                        COALESCE(o.o_comment, c.c_comment) AS info
                    FROM
                        t1_l o FULL JOIN t1_r c
                        ON  o.o_custkey = c.c_custkey
                        AND o.o_orderkey = c.c_nationkey
                ) SELECT * FROM main_lr
                """
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
            query = """select o.o_orderkey, c.c_name ||
                    cast(c.c_custkey as VARCHAR), c.c_name || '-' ||
                    cast(c.c_custkey as VARCHAR), o.o_orderstatus
                    from orders o
                    inner join customer c on o.o_custkey = c.c_custkey
                    where c.c_custkey < 10"""
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
            query = """select n1.n_regionkey, n2.n_nationkey,
                    MIN(n1.n_regionkey), MAX(n1.n_regionkey),
                    AVG(n2.n_nationkey)
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    GROUP BY n1.n_regionkey, n2.n_nationkey"""
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
            query = """SELECT c_custkey, count(c_nationkey),
                    min(c_nationkey), sum(c_nationkey)
                    from customer group by c_custkey"""
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
            query = """select o_orderkey as okey, o_custkey as ckey,
                    (EXTRACT(YEAR FROM o_orderdate) - 5) from orders
                    where o_orderstatus = 'O' order by okey"""
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
            query = """select n1.n_nationkey as n1key,
                        n2.n_nationkey as n2key,
                        n1.n_nationkey + n2.n_nationkey
                    from nation as n1
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

            queryId = "TEST_11"
            query = """select n1.n_nationkey as n1key,
                        n2.n_nationkey as n2key,
                        n1.n_nationkey + n2.n_nationkey
                    from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    and n1.n_nationkey + 1 = n2.n_nationkey + 7
                    and n1.n_nationkey + 2 = n2.n_nationkey + 8"""
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
            query = """select count(c_custkey) + sum(c_acctbal) +
                        avg(c_acctbal), min(c_custkey) - max(c_nationkey),
                        c_nationkey * 2 as key
                    from customer where  c_nationkey * 2 < 40
                    group by  c_nationkey * 2"""
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
            query = """select c.c_custkey, r.r_regionkey,
                    c.c_custkey + r.r_regionkey as addy from customer as c
                    inner join region as r on c.c_nationkey = r.r_regionkey
                    where c.c_acctbal < 1000 group by r.r_regionkey,
                    c.c_custkey order by c.c_custkey desc"""
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
            query = """SELECT n.n_nationkey + 1, n.n_regionkey from nation
                    AS n INNER JOIN region AS r
                    ON n.n_regionkey = r.r_regionkey
                    and n.n_nationkey = 5"""
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

            queryId = "TEST_15"
            query = """select n.n_nationkey, r.r_regionkey
                    from nation as n left outer join region as r
                    on n.n_regionkey = r.r_regionkey
                    where n.n_nationkey < 10
                    and n.n_nationkey > 5"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "n_nationkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_16"
            query = """select partSuppTemp.partKey, partAnalysis.avgSize
                    from
                    (
                        select min(p_partkey) as partKey,
                        avg(CAST(p_size AS DOUBLE)) as avgSize,
                        max(p_retailprice) as maxPrice,
                        min(p_retailprice) as minPrice from part
                    ) as partAnalysis
                    inner join
                    (
                        select ps_partkey as partKey, ps_suppkey as suppKey
                        from partsupp where ps_availqty > 2
                    ) as partSuppTemp
                    on partAnalysis.partKey = partSuppTemp.partKey
                    inner join
                    (
                        select s_suppkey as suppKey from supplier
                    ) as supplierTemp
                    on supplierTemp.suppKey = partSuppTemp.suppKey"""
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
            query = """ select p.p_brand, p.p_type, p.p_size,
                            count(ps.ps_suppkey) as supplier_cnt
                        from partsupp ps
                        inner join part p on p.p_partkey = ps.ps_partkey
                        where
                            p.p_brand <> 'Brand#45'
                            and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
                            and ps.ps_supplycost < p.p_retailprice
                        group by
                            p.p_brand, p.p_type, p.p_size
                        order by
                            supplier_cnt desc, p.p_brand, p.p_type, p.p_size"""
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
            query = """select c_custkey + c_nationkey, c_acctbal
                    from customer order by 1 desc, 2"""
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

            queryId = "TEST_19"
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

            queryId = "TEST_20"
            query = """select avg(CAST(c_custkey AS DOUBLE)), min(c_custkey)
                    from customer limit 5"""
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

    if (
        Settings.execution_mode == ExecutionMode.FULL and compareResults == "true"
    ) or Settings.execution_mode == ExecutionMode.GENERATOR:
        # Create Table Drill -----------------------------------------
        print("starting drill")
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill, Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"], bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
