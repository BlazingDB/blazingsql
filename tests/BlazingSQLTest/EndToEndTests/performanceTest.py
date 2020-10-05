from blazingsql import BlazingContext
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from Runner import runTest
from Utils import Execution


def main(drill, dir_data_file, nRals):

    table1 = "customer"
    table2 = "orders"
    table3 = "lineitem"

    # Read Data TPCH------------------------------------------------------
    customer_gdf = cs.read_data(table1, dir_data_file)
    orders_gdf = cs.read_data(table2, dir_data_file)
    lineitem_gdf = cs.read_data(table3, dir_data_file)

    # Create Tables -----------------------------------------------------
    bc = BlazingContext()

    bc.create_table("customer", customer_gdf)
    bc.create_table("orders", orders_gdf)
    bc.create_table("lineitem", lineitem_gdf)

    # Run Query ------------------------------------------------------
    # Parameter to indicate if its necessary to order
    # the resulsets before compare them
    worder = 1
    use_percentage = False
    acceptable_difference = 0.01
    queryType = "Performance Test"

    print("==============================")
    print(queryType)
    print("==============================")

    queryId = "TEST_01"
    query = "select min(l_orderkey) from lineitem"
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_02"
    query = """select sum(o_custkey), avg(o_totalprice),
            min(o_custkey), max(o_orderkey), count(o_orderkey)
            from orders"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_03"
    query = "select max(l_partkey) from lineitem group by l_orderkey"
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_04"
    query = """select max(l_partkey), min(l_orderkey),
            sum(l_orderkey), avg(l_orderkey)
            from lineitem group by l_orderkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_05"
    query = """select max(l_partkey)
            from lineitem
            group by l_orderkey, l_suppkey, l_linenumber"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_06"
    query = """select max(l_partkey), min(l_orderkey),
                sum(l_orderkey), avg(l_orderkey)
            from lineitem
            group by l_orderkey, l_suppkey, l_linenumber"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_07"
    query = "select l_orderkey from lineitem group by l_orderkey"
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_08"
    query = """select l_orderkey, l_extendedprice, l_shipdate
            from lineitem
            group by l_orderkey, l_extendedprice, l_shipdate, l_linestatus"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_09"
    query = """select o.o_orderkey, o.o_totalprice,
                l.l_partkey, l.l_returnflag
            from orders as o
            inner join lineitem as l on o.o_orderkey = l.l_orderkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_10"
    query = """select o.o_orderkey, o.o_totalprice,
                l.l_partkey, l.l_returnflag
            from lineitem as l
            inner join orders as o on o.o_orderkey = l.l_orderkey
            inner join customer as c on c.c_custkey = o.o_custkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_11"
    query = """select o.o_orderkey, o.o_totalprice, l.l_partkey, l.l_returnflag
            from lineitem as l
            inner join orders as o on o.o_orderkey = l.l_orderkey
            inner join customer as c on c.c_nationkey = o.o_custkey
            and l.l_linenumber = c.c_custkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_12"
    query = """select o.o_totalprice, l.l_partkey
            from orders as o
            left outer join lineitem as l on o.o_custkey = l.l_linenumber
            and l.l_suppkey = o.o_orderkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_13"
    query = """select c.c_name, o.o_custkey, l.l_linenumber
            from customer as c
            left outer join orders as o on c.c_custkey = o.o_custkey
            left outer join lineitem as l on l.l_orderkey = o.o_orderkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_14"
    query = """select o.o_orderkey, o.o_totalprice,
            l.l_partkey, l.l_linestatus
            from orders as o
            full outer join lineitem as l
            on l.l_orderkey = o.o_orderkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_15"
    query = """select c.c_name, o.o_custkey, l.l_linenumber from customer as c
        full outer join orders as o on c.c_custkey = o.o_custkey
        full outer join lineitem as l on l.l_orderkey = o.o_orderkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_16"
    query = """select l_returnflag, l_shipdate, l_linestatus
            from lineitem where l_orderkey < 10000
            union all
            select l_returnflag, l_shipdate, l_linestatus
            from lineitem where l_partkey > 100"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_17"
    query = """select o_orderdate as d1, o_orderpriority as s1,
                o_orderstatus as s2, o_orderkey as l1
            from orders where o_orderkey < 10000
            union all
            select o_orderdate as d1, o_orderpriority as s1,
                o_orderstatus as s2, o_orderkey as l1
            from orders where o_custkey < 100000
            union all
            select o_orderdate as d1, o_orderpriority as s1,
                o_orderstatus as s2, o_orderkey as l1
            from orders where o_orderstatus = 'O'
            union all
            select o_orderdate as d1, o_orderpriority as s1,
                o_orderstatus as s2, o_orderkey as l1
            from orders where o_totalprice < 350"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_18"
    query = """select o_orderkey, o_custkey,
                o_orderstatus, o_totalprice, o_orderpriority
            from orders
            where o_custkey <> 4318470"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_19"
    query = """select l_orderkey, l_partkey, l_suppkey, l_returnflag
            from lineitem where l_returnflag <> 'g packages.'"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_20"
    query = """select o_orderkey, o_custkey, o_orderstatus,
                 o_totalprice, o_orderpriority
            from orders where o_custkey = 88910"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_21"
    query = """select l_orderkey, l_partkey, l_suppkey, l_returnflag
            from lineitem where l_returnflag='N'"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_22"
    query = """select o_orderkey, o_custkey, o_orderstatus, o_totalprice
            from orders
            where o_orderkey=1000"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_23"
    query = """select l_orderkey, l_partkey, l_suppkey, l_comment
            from lineitem
            where l_comment='dolites wake'"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_24"
    query = """select c_custkey, c_nationkey, c_acctbal
            from customer
            where c_custkey < 15000
            and c_nationkey = 5
            or c_custkey = c_nationkey * c_nationkey
            or c_nationkey >= 10
            or c_acctbal <= 500"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    #     queryId = 'TEST_25'
    #     query = "select o_orderkey, o_orderstatus, o_totalprice, o_comment
    #            from orders where o_orderstatus = 'O'"
    #     runTest.run_query_performance(bc, drill, query, queryId, queryType,
    #     worder, '', acceptable_difference, use_percentage, # fileSchemaType)

    queryId = "TEST_26"
    query = """select ((l_orderkey + l_partkey)/2) * l_quantity -
                 l_suppkey * (l_discount - 1000.999)
            from lineitem"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_27"
    query = """select (l_orderkey + l_partkey*l_quantity)/2,
                (l_orderkey + l_quantity - l_partkey) * (l_orderkey + 3),
                l_orderkey/l_partkey - l_quantity,
                l_quantity * l_linenumber / l_partkey*l_tax
            from lineitem"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_28"
    query = """select EXTRACT(YEAR FROM l_receiptdate) -
             EXTRACT(YEAR FROM l_shipdate) as years_late,
            EXTRACT(MONTH FROM l_receiptdate) -
             EXTRACT(MONTH FROM l_shipdate) as months_late,
            EXTRACT(DAY FROM l_receiptdate) -
             EXTRACT(DAY FROM l_shipdate) as days_late
            from lineitem"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_29"
    query = """select l_quantity, sin(l_quantity), cos(l_quantity),
                asin(l_quantity), acos(l_quantity), ln(l_quantity),
                tan(l_quantity), atan(l_quantity), floor(l_quantity),
                ceil(l_quantity)
            from lineitem"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_30"
    query = """select o_totalprice, o_custkey + o_custkey
            from orders order by o_totalprice"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_31"
    query = """select o_orderkey, o_custkey, o_orderstatus
            from orders order by o_comment"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_32"
    query = """select o_orderkey, o_custkey, o_totalprice, o_orderstatus
            from orders
            order by o_orderkey, o_custkey, o_totalprice"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_33"
    query = """select o_orderkey, o_custkey, o_totalprice, o_orderstatus
            from orders
            order by o_orderstatus, o_shippriority, o_comment"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_34"
    query = """select c_custkey + c_nationkey, c_acctbal
            from customer order by 1 desc, 2"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_35"
    query = """select custOrders.avgPrice, custOrders.numOrders
            from customer
            inner join
            (
                select o_custkey as o_custkey, avg(o_totalprice) as avgPrice,
                count(o_totalprice) as numOrders
                from orders
                where o_custkey <= 100 group by o_custkey
            ) as custOrders
            on custOrders.o_custkey = customer.c_custkey
            where customer.c_nationkey <= 5"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )

    queryId = "TEST_36"
    query = """with ordersTemp as (
            select min(o_orderkey) as priorityKey, o_custkey
            from orders group by o_custkey
        ), ordersjoin as(
            select orders.o_custkey
            from orders
            inner join ordersTemp
            on ordersTemp.priorityKey = orders.o_orderkey
        )
        select customer.c_custkey, customer.c_nationkey
        from customer inner join ordersjoin
        on ordersjoin.o_custkey =  customer.c_custkey"""
    runTest.run_query_performance(
        bc,
        drill,
        query,
        queryId,
        queryType,
        worder,
        "",
        acceptable_difference,
        use_percentage,
        # fileSchemaType,
    )


if __name__ == "__main__":

    Execution.getArgs()

    drill = "drill"

    if ((Settings.execution_mode == ExecutionMode.FULL and
         compareResults == "true") or
            Settings.execution_mode == ExecutionMode.GENERATOR):

        # Create Table Drill ------------------------------------------------
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                            Settings.data["TestSettings"]["dataDirectory"])
    nRals = Settings.data["RunSettings"]["nRals"]
    main(drill, Settings.data["TestSettings"]["dataDirectory"], nRals)

    runTest.save_log()
