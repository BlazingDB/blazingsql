from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Simple Distribution From Local"


def main(dask_client, drill, spark, dir_data_lc, bc, nRals):
    tables = ["nation", "region", "supplier", "customer",
              "lineitem", "orders"]

    data_types = [
        DataType.DASK_CUDF,
        DataType.CUDF,
        DataType.CSV,
        DataType.ORC,
        DataType.PARQUET,
        DataType.JSON
    ]

    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue
        cs.create_tables(bc, dir_data_lc, fileSchemaType, tables=tables)

        # Run Query -----------------------------------------------------
        # Parameter to indicate if its necessary to order
        # the resulsets before compare them
        worder = 1
        use_percentage = False
        acceptable_difference = 0.01
        # queryType = 'Simple Distribution Test'

        print("==============================")
        print(queryType)
        print("==============================")

        # distributed sort
        queryId = "TEST_00"
        query = """select l_linenumber, l_orderkey
                from lineitem where l_orderkey < 50000
                order by l_linenumber desc, l_suppkey asc,
                    l_partkey desc, l_orderkey"""
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

        queryId = "TEST_01"
        query = """select c_custkey, c_nationkey
                from customer order by c_nationkey, c_custkey"""
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

        queryId = "TEST_02"
        query = """select c_custkey, c_acctbal
                from customer
                order by c_custkey desc, c_acctbal"""
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

        queryId = "TEST_03"
        query = """select o_orderkey, o_custkey, o_clerk
                from orders where o_orderkey < 10000
                order by o_clerk desc, o_custkey desc, o_orderkey"""
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

        # distributed join
        queryId = "TEST_04"
        query = """select count(c.c_custkey), sum(c.c_nationkey), n.n_regionkey
                from customer as c
                inner join nation as n on c.c_nationkey = n.n_nationkey
                group by n.n_regionkey"""
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
        query = """select c.c_custkey, c.c_nationkey, n.n_regionkey
                from customer as c
                inner join nation as n on c.c_nationkey = n.n_nationkey
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

        queryId = "TEST_06"  # parallel_for failed: invalid argument
        query = """select c.c_custkey, c.c_nationkey, n.o_orderkey
                from customer as c
                inner join orders as n on c.c_custkey = n.o_custkey
                where n.o_orderkey < 100"""
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

        # distributed group_by
        queryId = "TEST_07"
        query = """select count(c_custkey), sum(c_acctbal),
                    sum(c_acctbal)/count(c_acctbal), min(c_custkey),
                    max(c_nationkey), c_nationkey
                from customer
                group by c_nationkey"""
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
        query = """select count(c_custkey), sum(c_acctbal),
                    sum(c_acctbal)/count(c_acctbal),
                    min(c_custkey), max(c_custkey), c_nationkey
                from customer
                where c_custkey < 50
                group by c_nationkey"""
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
        query = """select c_nationkey, count(c_acctbal)
                from customer group by c_nationkey, c_custkey"""
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

        # all queries
        queryId = "TEST_10"
        query = """select c.c_custkey, c.c_nationkey, o.o_orderkey
                from customer as c
                inner join orders as o on c.c_custkey = o.o_custkey
                inner join nation as n on c.c_nationkey = n.n_nationkey
                order by c_custkey, o.o_orderkey"""
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

        # ========================== STRING TEST ============================

        queryId = "TEST_11"
        query = """select c_custkey, c_nationkey, c_name
                from customer where c_custkey > 300 and c_custkey < 600
                order by c_nationkey, c_custkey"""
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

        queryId = "TEST_12"
        query = """select c_acctbal, c_name
                from customer where c_custkey < 300
                order by c_acctbal, c_custkey"""
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

        queryId = "TEST_13"
        query = """select c_name, c_nationkey
                from customer
                order by c_custkey, c_nationkey limit 200"""
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

        # ============================ Order By ============================

        queryId = "TEST_14"
        query = """select o_totalprice, o_custkey + o_custkey
                from orders where o_orderkey < 100
                order by o_custkey, o_totalprice"""
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

        queryId = "TEST_15"
        query = """select o_orderkey, o_custkey, o_orderstatus
                from orders where o_orderkey < 100
                order by  o_custkey, o_custkey, o_comment"""
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

        queryId = "TEST_16"
        query = """select o_orderkey, o_custkey, o_totalprice, o_orderstatus
                from orders where o_orderkey < 100
                order by o_orderkey, o_custkey, o_totalprice """
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

        queryId = "TEST_17"
        query = """select o_orderkey, o_custkey, o_totalprice, o_orderstatus
                from orders where o_orderkey < 100
                order by o_custkey, o_orderstatus,
                o_shippriority, o_comment"""
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

        queryId = "TEST_18"
        query = """select c_custkey + c_nationkey, c_acctbal
                from customer order by 1 desc, 2"""
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

        # ============================ Group By ============================

        queryId = "TEST_19"
        query = """select count(c_custkey), sum(c_acctbal),
                     min(c_custkey), max(c_nationkey), c_nationkey
                from customer
                where c_custkey < 5000
                group by c_nationkey"""
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

        queryId = "TEST_20"
        query = """select count(c_custkey), sum(c_acctbal),
                     min(c_custkey), max(c_custkey), c_nationkey
                from customer
                where c_custkey < 5000
                group by c_nationkey"""
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
        query = """select l_orderkey
                from lineitem where l_linenumber > 5
                group by l_orderkey"""
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
        query = """select l_orderkey, l_extendedprice, l_shipdate
                from lineitem where l_orderkey < 100
                group by l_orderkey, l_extendedprice,
                l_shipdate, l_linestatus"""
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

        # =========================== Aggregations ===========================

        queryId = "TEST_23"
        query = """select min(l_orderkey)
                from lineitem where l_orderkey < 100"""
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

        queryId = "TEST_24"
        query = """select sum(o_custkey), min(o_custkey),
                max(o_orderkey), count(o_orderkey)
                from orders where o_orderkey < 100"""
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

        queryId = "TEST_25"
        query = """select max(l_partkey) from lineitem
                where l_orderkey < 100
                group by l_orderkey"""
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

        queryId = "TEST_26"
        query = """select max(l_partkey), min(l_orderkey), sum(l_orderkey)
                from lineitem
                where l_orderkey < 100 group by l_orderkey"""
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

        queryId = "TEST_27"
        query = """select max(l_partkey)
                from lineitem where l_orderkey < 100
                group by l_orderkey, l_suppkey, l_linenumber"""
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

        queryId = "TEST_28"
        query = """select max(l_partkey), min(l_orderkey), sum(l_orderkey)
                from lineitem
                where l_orderkey < 100
                group by l_orderkey, l_suppkey, l_linenumber"""
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

        # =========================== Where Clause ===========================

        queryId = "TEST_29"
        query = """select c_custkey, c_name, c_acctbal
                from customer
                where c_custkey <> 10 and c_custkey <> 11
                and c_custkey<>100 and c_custkey<>1000
                and c_custkey < 1001 """
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

        #         queryId = 'TEST_30'
        #         query = """select l_orderkey, l_partkey, l_suppkey,
        #  l_returnflag from lineitem where l_returnflag<>'g packages.'"""
        #         runTest.run_query(bc, drill, query, queryId, queryType,
        #  worder, '', acceptable_difference, use_percentage, fileSchemaType)

        queryId = "TEST_30"
        query = """select o_orderkey, o_custkey, o_orderstatus,
                o_totalprice, o_orderpriority
                from orders
                where o_custkey = 88910"""
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

        queryId = "TEST_31"
        query = """select l_orderkey, l_partkey, l_suppkey, l_returnflag
                from lineitem
                where l_returnflag='N' and l_linenumber < 3
                and l_orderkey < 50"""
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

        queryId = "TEST_32"
        query = """select o_orderkey, o_custkey, o_totalprice
                from orders where o_orderkey=100"""
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

        #     queryId = 'TEST_31'
        #     query = """select l_orderkey, l_partkey, l_suppkey, l_comment
        #            from lineitem where l_comment='dolites wake'"""
        #     runTest.run_query(bc, drill, query, queryId, queryType, worder,
        #    '', acceptable_difference, use_percentage, fileSchemaType)

        queryId = "TEST_33"
        query = """select c_custkey, c_nationkey, c_acctbal
                from customer
                where c_custkey < 15000
                and c_nationkey = 5 or c_custkey = c_nationkey * c_nationkey
                or c_nationkey >= 10 or c_acctbal <= 500"""
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

        queryId = "TEST_34"
        query = """select o_orderkey, o_orderstatus, o_totalprice, o_comment
                from orders
                where o_orderstatus = 'O' and o_orderkey<5"""
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

        # ========================== Nested Queries ==========================

        queryId = "TEST_35"
        query = """select custOrders.avgPrice, custOrders.numOrders
                from customer inner join
                (
                    select o_custkey as o_custkey,
                    SUM(o_totalprice)/COUNT(o_totalprice) as avgPrice,
                    count(o_totalprice) as numOrders
                    from orders
                    where o_custkey <= 100
                    group by o_custkey
                ) as custOrders
                on custOrders.o_custkey = customer.c_custkey
                where customer.c_nationkey <= 5"""
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
        )  # TODO: Change sum/count for avg KC

        # ============================ Inner Join ============================

        queryId = "TEST_36"
        query = """select o.o_orderkey, o.o_totalprice, l.l_partkey
                from orders as o
                inner join lineitem as l on o.o_orderkey = l.l_orderkey
                and l.l_linenumber > 5"""
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

        queryId = "TEST_37"
        query = """select c.c_name, o.o_orderkey, o.o_totalprice,
                    l.l_partkey, l.l_returnflag
                from lineitem as l
                inner join orders as o on o.o_orderkey = l.l_orderkey
                inner join customer as c on c.c_custkey = o.o_custkey
                and l.l_linenumber < 3 and c.c_custkey < 30"""
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

        queryId = "TEST_38"
        query = """select cust.custkey, cust.cname from customer inner join
                (select c_custkey as custkey, c_name as cname from customer
                where c_custkey <= 50 group by c_custkey, c_name) as cust
                on cust.cname = customer.c_name"""
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

        queryId = "TEST_39"
        query = """select o.o_orderkey, o.o_totalprice, l.l_partkey
                from lineitem as l
                inner join orders as o on o.o_orderkey = l.l_orderkey
                inner join customer as c on c.c_custkey = o.o_custkey
                and l.l_linenumber > 5"""
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

        queryId = "TEST_40"
        query = """select o.o_orderkey, o.o_totalprice, l.l_partkey
                from lineitem as l
                inner join orders as o on o.o_orderkey = l.l_orderkey * 2
                inner join customer as c on c.c_nationkey = o.o_custkey"""
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

        queryId = "TEST_41"
        query = """select c.c_name, c.c_custkey, o.o_orderkey
                from customer as c
                inner join orders as o on c.c_custkey = o.o_custkey
                where c.c_custkey < 1000"""
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

        # ============================ Union ============================

        #         queryId = 'TEST_42'
        #         query = """select o_orderdate from orders
        #            order by o_orderkey limit 5"""
        #         runTest.run_query(bc, drill, query, queryId,
        #            queryType, worder, '', acceptable_difference,
        #               use_percentage, fileSchemaType)

        queryId = "TEST_42"
        query = """select l_returnflag, l_shipdate, l_linestatus
                from lineitem
                where l_orderkey < 100 and l_linenumber < 2
                union all
                select l_returnflag, l_shipdate, l_linestatus
                from lineitem where l_partkey < 1
                and l_orderkey < 2 and l_linenumber < 2"""
        # runTest.run_query(bc, drill, query, queryId, queryType, worder,
        #  '', acceptable_difference, use_percentage, fileSchemaType)

        queryId = "TEST_43"
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

        queryId = "TEST_44"
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
        # runTest.run_query(bc, drill, query, queryId, queryType, worder,
        #  '', acceptable_difference, use_percentage, fileSchemaType)

        queryId = "TEST_45"
        query = """select count(mo.o_totalprice), c.c_name
                from (
                    select o.o_orderkey, o.o_custkey,
                    o.o_totalprice, o.o_orderstatus
                    from orders as o
                    inner join lineitem as l on o.o_orderkey = l.l_orderkey
                    where l.l_linenumber = 5
                ) as mo
                inner join customer as c on c.c_custkey = mo.o_custkey
                where mo.o_orderstatus = 'O' group by c.c_name"""
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

        # ============================== Limit ==============================

        queryId = "TEST_46"
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

        queryId = "TEST_47"
        query = """select o_orderkey from orders
                where o_custkey < 300
                and o_orderdate >= '1990-08-01'
                order by o_orderkey, o_orderkey
                limit 50"""
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

        queryId = "TEST_48"
        query = """select orders.o_totalprice, customer.c_name
                from orders
                inner join customer on orders.o_custkey = customer.c_custkey
                order by customer.c_name, orders.o_orderkey,
                customer.c_custkey
                limit 10"""
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

        queryId = "TEST_49"
        query = """(select l_shipdate, l_orderkey, l_linestatus
                    from lineitem
                    where l_linenumber = 1 order by 1,2, 3
                    limit 10
                )
                union all
                (
                    select l_shipdate, l_orderkey, l_linestatus
                    from lineitem where l_linenumber = 1
                    order by 1, 3 desc, 2
                    limit 10
                )"""
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

        queryId = "TEST_50"
        query = """select c_custkey from customer
                where c_custkey < 0
                order by c_custkey limit 40"""
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

        queryId = "TEST_51"
        query = """select c_custkey, c_name
                from customer where c_custkey < 10
                order by c_custkey, 1 limit 30"""
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

        queryId = "TEST_52"
        query = """select c_custkey, c_name
                from customer where c_custkey < 10 limit 30"""
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

        queryId = "TEST_53"
        query = """select sum(c_custkey)/count(c_custkey), min(c_custkey)
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
            True,
            fileSchemaType,
        )  # TODO: Change sum/count for avg KC

        # if Settings.execution_mode == ExecutionMode.GENERATOR:
        #         print("==============================")
        #         break


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
