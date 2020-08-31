from blazingsql import DataType
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test


def main(dask_client, drill, dir_data_file, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Not passing Test"

    def executionTest(queryType):
        tables = cs.tpchTables
        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
        ]  # TODO json

        bc, dask_client = init_context()

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables)

            # Run Query ------------------------------------------------------
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            queryType = "Aggregations without group by Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_07"
            query = """select COUNT(n1.n_nationkey) as n1key,
                    COUNT(DISTINCT(n2.n_nationkey +  n1.n_nationkey)) as n2key
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 10"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Coalesce Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_02"
            query = """select COALESCE(orders.o_orderkey, 100),
                         COALESCE(orders.o_totalprice, 0.01)
                    from customer left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    where  customer.c_nationkey = 3
                    and customer.c_custkey < 500"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_03"
            query = """select COALESCE(orders.o_orderkey, customer.c_custkey),
                         COALESCE(orders.o_totalprice, customer.c_acctbal)
                    from customer left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 500"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_05"
            query = """select COUNT(DISTINCT(COALESCE(n1.n_regionkey,32))),
                         AVG(COALESCE(n1.n_regionkey,32))
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_06"
            query = """select SUM(COALESCE(n2.n_nationkey, 100)),
                        COUNT(DISTINCT(COALESCE(n1.n_nationkey,32))),
                         n2.n_regionkey as n1key
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    GROUP BY n2.n_regionkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_07"
            query = """select MIN(COALESCE(n.n_nationkey, r.r_regionkey)),
                    MAX(COALESCE(n.n_nationkey, 8))
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_08"
            query = """select AVG(COALESCE(n.n_nationkey, r.r_regionkey)),
                         MAX(COALESCE(n.n_nationkey, 8)),
                          COUNT(COALESCE(n.n_nationkey, 12)), n.n_nationkey
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey GROUP BY n.n_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_09"
            query = """select SUM(COALESCE(n2.n_nationkey, 100)),
                         COUNT(DISTINCT(COALESCE(n1.n_nationkey,32))),
                          COALESCE(n2.n_regionkey, 100) as n1key
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    GROUP BY COALESCE(n2.n_regionkey, 100)"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Commom Table Expressions Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_04"
            query = """with ordersTemp as (
                        select min(orders.o_orderkey) as priorityKey,
                        o_custkey
                        from orders group by o_custkey
                    ), ordersjoin as(
                        select orders.o_orderkey,
                         orders.o_custkey/(orders.o_custkey + 1) as o_custkey,
                         (ordersTemp.priorityKey + 1) as priorityKey
                        from orders inner join ordersTemp on
                        (ordersTemp.priorityKey = orders.o_orderkey)
                    )
                    select (customer.c_custkey + 1)/(customer.c_custkey -
                        customer.c_custkey + 1) from customer
                    inner join ordersjoin
                    on ordersjoin.o_custkey =  customer.c_custkey
                    where (customer.c_custkey > 1
                    or customer.c_custkey < 100)"""
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
            )  # WSM NEED TO REVISIT THIS

            queryType = "Count Distinc Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_07"
            # count(distinct(o_orderdate)),
            query = """select count(distinct(o_custkey)),
                        count(distinct(o_totalprice)), sum(o_orderkey)
                    from orders group by o_custkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_08"
            query = """select COUNT(DISTINCT(n.n_nationkey)), AVG(r.r_regionkey)
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', 0.01, use_percentage, fileSchemaType)

            queryId = "TEST_09"
            query = """select MIN(n.n_nationkey), MAX(r.r_regionkey),
                        COUNT(DISTINCT(n.n_nationkey + r.r_regionkey))
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_10"
            query = """select COUNT(DISTINCT(n1.n_nationkey)) as n1key,
                    COUNT(DISTINCT(n2.n_nationkey)) as n2key
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_regionkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_11"
            query = """select r.r_regionkey, n.n_nationkey,
                     COUNT(n.n_nationkey), COUNT(DISTINCT(r.r_regionkey)),
                      SUM(DISTINCT(n.n_nationkey + r.r_regionkey))
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    GROUP BY r.r_regionkey, n.n_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_12"
            query = """select n1.n_regionkey, n2.n_nationkey,
                         MIN(n1.n_regionkey), MAX(n1.n_regionkey),
                         AVG(n2.n_nationkey)
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    GROUP BY n1.n_regionkey, n2.n_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Count without group by Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = "select count(*), count(n_nationkey) from nation"
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_02"
            query = """select count(n_nationkey), count(*)
                    from nation group by n_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Predicates with nulls"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_06"
            query = """select COUNT(n.n_nationkey), AVG(r.r_regionkey)
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE n.n_regionkey IS NULL"""
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
                print_result=True,
            )

            queryId = "TEST_07"
            query = """select n.n_nationkey, n.n_name, r.r_regionkey,
                     r.r_name
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE r.r_name IS NULL"""
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

            queryId = "TEST_08"  # Core dump al iniciar el query
            query = """select n.n_nationkey, n.n_name, r.r_regionkey,
                     r.r_name
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE n.n_name IS NOT NULL"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Single Node From Local Test"

            queryId = "TEST_01"
            query = "select count(*), count(n_nationkey) from nation"
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_02"
            query = """select count(n_nationkey), count(*)
                    from nation group by n_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Predicates with nulls"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_06"
            query = """select COUNT(n.n_nationkey), AVG(r.r_regionkey)
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE n.n_regionkey IS NULL"""
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
                print_result=True,
            )

            queryId = "TEST_07"
            query = """select n.n_nationkey, n.n_name, r.r_regionkey, r.r_name
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE r.r_name IS NULL"""
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

            queryId = "TEST_08"  # Core dump al iniciar el query
            query = """select n.n_nationkey, n.n_name, r.r_regionkey, r.r_name
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE n.n_name IS NOT NULL"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Single Node From Local Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_04"
            query = """select count(c_custkey), sum(c_acctbal), avg(c_acctbal),
                         min(c_custkey), max(c_nationkey),
                         (max(c_nationkey) + min(c_nationkey))/2  c_nationkey
                    from customer where c_custkey < 100 group by c_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Tables From Pandas Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_04"
            query = """select count(c_custkey), sum(c_acctbal),
                        avg(c_acctbal), min(c_custkey), max(c_nationkey),
                         (max(c_nationkey) + min(c_nationkey))/2 c_nationkey
                    from customer where c_custkey < 100 group by c_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', 0.01, use_percentage, fileSchemaType)

            queryType = "Union Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_03"
            query = """(select o_orderkey, o_totalprice as key
                    from orders where o_orderkey < 100)
                    union all
                    (select o_orderkey, o_custkey as key
                    from orders where o_orderkey < 300
                    and o_orderkey >= 200)"""
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

            queryType = "Where clause Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_10"
            query = """select c_custkey, c_nationkey as nkey
                    from customer where -c_nationkey + c_acctbal > 750.3"""
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
            query = """select c_custkey, c_nationkey as nkey
                    from customer where -c_nationkey + c_acctbal > 750"""
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
                         o_orderdate as odate
                        from orders
                    where o_orderstatus = 'O'
                    and o_orderpriority = '1-URGENT' order by okey"""
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
            query = """select max(o_totalprice) as max_price,
                        min(o_orderdate) as min_orderdate from orders
                       where o_orderdate = '1998-08-01'"""
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
            query = """select max(o_totalprice) as max_price,
                         min(o_orderdate) as min_orderdate
                        from orders
                    where o_orderdate > '1998-08-01'"""
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
                print_result=True,
            )

            queryType = "New Queries"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_12"
            query = """select count(n1.n_nationkey) as n1key,
                         count(n2.n_nationkey) as n2key, count(*) as cstar
                    from nation as n1 full outer join nation as n2
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

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_04"
            query = """select count(c_custkey), sum(c_acctbal), avg(c_acctbal),
                     min(c_custkey), max(c_nationkey),
                     (max(c_nationkey) + min(c_nationkey))/2 c_nationkey
                    from customer where c_custkey < 100 group by c_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryType = "Tables From Pandas Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_04"
            query = """select count(c_custkey), sum(c_acctbal), avg(c_acctbal),
                        min(c_custkey), max(c_nationkey),
                        (max(c_nationkey) + min(c_nationkey))/2 c_nationkey
                    from customer where c_custkey < 100 group by c_nationkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', 0.01, use_percentage, fileSchemaType)

            queryType = "Union Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_03"
            query = """(select o_orderkey, o_totalprice as key
                    from orders where o_orderkey < 100)
                    union all
                    (select o_orderkey, o_custkey as key
                    from orders where o_orderkey < 300
                    and o_orderkey >= 200)"""
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

            queryType = "Where clause Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_10"
            query = """select c_custkey, c_nationkey as nkey
                    from customer where -c_nationkey + c_acctbal > 750.3"""
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
            query = """select c_custkey, c_nationkey as nkey
                    from customer where -c_nationkey + c_acctbal > 750"""
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

            queryType = "New Queries"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_12"
            query = """select count(n1.n_nationkey) as n1key,
                        count(n2.n_nationkey) as n2key, count(*) as cstar
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

            queryType = "Concat Test"

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_09"
            query = """select o.o_orderkey, c.c_name || '-' ||
                        (c.c_custkey + 1), o.o_orderstatus from orders o
                    inner join customer c on o.o_custkey = c.c_custkey
                    where c.c_custkey < 20"""

            queryId = "TEST_04"
            query = """select c_custkey, SUBSTRING(c_name, 1, 8)
                    from customer
                    where c_name between 'Customer#000000009'
                    and 'Customer#0000000011'"""
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

    executionTest(queryType)

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    # Create Table Drill ------------------------------------------------
    from pydrill.client import PyDrill

    drill = PyDrill(host="localhost", port=8047)
    cs.init_drill_schema(drill,
                         Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL
    bc, dask_client = init_context()
    nRals = Settings.data["RunSettings"]["nRals"]
    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         nRals)

    runTest.save_log()

    gpuMemory.print_log_gpu_memory()
