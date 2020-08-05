from pydrill.client import PyDrill
from DataBase import createSchema as cs
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution
from Utils import gpuMemory, skip_test, init_context
from pynvml import nvmlInit
from blazingsql import DataType
from Configuration import ExecutionMode
from pyspark.sql import SparkSession


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "TPCH Queries"

    def executionTest(queryType):

        tables = [
            "nation",
            "region",
            "customer",
            "lineitem",
            "orders",
            "supplier",
            "part",
            "partsupp",
        ]
        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
        ]  # TODO json

        # Create Tables ------------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            # Run Query ------------------------------------------------------
            worder = 1  # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.001

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"

            query = """
                select
                    l_returnflag,
                    l_linestatus,
                    sum(l_quantity) as sum_qty,
                    sum(l_extendedprice) as sum_base_price,
                    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
                    sum(l_extendedprice*(1-l_discount)*(1+l_tax))
                        as sum_charge,
                    avg(l_quantity) as avg_qty,
                    avg(l_extendedprice) as avg_price,
                    avg(l_discount) as avg_disc,
                    count(*) as count_order
                from
                    lineitem
                where
                    l_shipdate <= date '1998-12-01' - interval '90' day
                group by
                    l_returnflag,
                    l_linestatus
                order by
                    l_returnflag,
                    l_linestatus
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

            queryId = "TEST_02"

            # Edited:
            # - implicit joins generated some condition=[true] on Blazingsql
            # - added table aliases to avoid ambiguity on Drill

            query = """
                select
                    s.s_acctbal,
                    s.s_name,
                    n.n_name,
                    p.p_partkey,
                    p.p_mfgr,
                    s.s_address,
                    s.s_phone,
                    s.s_comment
                from
                    supplier as s
                    inner join nation as n on s.s_nationkey = n.n_nationkey
                    inner join partsupp as ps on s.s_suppkey = ps.ps_suppkey
                    inner join part as p on p.p_partkey = ps.ps_partkey
                    inner join region as r on r.r_regionkey = n.n_regionkey
                where
                    p.p_size = 15
                    and p.p_type like '%BRASS'
                    and r.r_name = 'EUROPE'
                    and ps.ps_supplycost = (
                        select
                            min(psq.ps_supplycost)
                        from
                            partsupp as psq
                            inner join supplier sq on
                                sq.s_suppkey = psq.ps_suppkey
                            inner join nation as nq on
                                sq.s_nationkey = nq.n_nationkey
                            inner join region as rq on
                                nq.n_regionkey = rq.r_regionkey
                        where
                            p.p_partkey = psq.ps_partkey
                            and rq.r_name = 'EUROPE'
                        )
                order by
                    s.s_acctbal desc,
                    n.n_name,
                    s.s_name,
                    p.p_partkey
                limit 100
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

            queryId = "TEST_03"

            # Edited:
            # - implicit joins without table aliases causes
            #   parsing errors on Drill
            # - added table aliases to avoid ambiguity on Drill
            # - There is an issue with validation on gpuci

            query = """
                select
                    l.l_orderkey,
                    sum(l.l_extendedprice*(1-l.l_discount)) as revenue,
                    o.o_orderdate,
                    o.o_shippriority
                from
                    customer as c,
                    orders as o,
                    lineitem as l
                where
                    c.c_mktsegment = 'BUILDING'
                    and c.c_custkey = o.o_custkey
                    and l.l_orderkey = o.o_orderkey
                    and o.o_orderdate < date '1995-03-15'
                    and l.l_shipdate > date '1995-03-15'
                group by
                    l.l_orderkey,
                    o.o_orderdate,
                    o.o_shippriority
                order by
                    revenue desc,
                    o.o_orderdate
                limit 10
            """
            # if fileSchemaType == DataType.ORC:
            #     runTest.run_query(
            #         bc,
            #         spark,
            #         query,
            #         queryId,
            #         queryType,
            #         worder,
            #         "",
            #         acceptable_difference,
            #         use_percentage,
            #         fileSchemaType,
            #     )
            # else:
            #     runTest.run_query(
            #         bc,
            #         drill,
            #         query,
            #         queryId,
            #         queryType,
            #         worder,
            #         "",
            #         acceptable_difference,
            #         use_percentage,
            #         fileSchemaType,
            #     )

            queryId = "TEST_04"

            # WARNING:
            # - Fails with Drill, passes only with ORC files on PySpark
            # - Passes with BigQuery
            # - Blazingsql is returning different results
            #   for parquet, psv, and gdf

            query = """
                select
                    o_orderpriority,
                    count(*) as order_count
                from
                    orders
                where
                    o_orderdate >= date '1993-07-01'
                    and o_orderdate < date '1993-07-01' + interval '3' month
                    and exists (
                        select
                            *
                        from
                            lineitem
                        where
                            l_orderkey = o_orderkey
                            and l_commitdate < l_receiptdate
                        )
                group by
                    o_orderpriority
                order by
                    o_orderpriority
            """
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

            queryId = "TEST_05"

            # Edited:
            # - implicit joins without table aliases causes
            #   parsing errors on Drill

            query = """
                select
                    n.n_name,
                    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
                from
                    customer as c,
                    orders as o,
                    lineitem as l,
                    supplier as s,
                    nation as n,
                    region as r
                where
                    c.c_custkey = o.o_custkey
                    and l.l_orderkey = o.o_orderkey
                    and l.l_suppkey = s.s_suppkey
                    and c.c_nationkey = s.s_nationkey
                    and s.s_nationkey = n.n_nationkey
                    and n.n_regionkey = r.r_regionkey
                    and r.r_name = 'ASIA'
                    and o.o_orderdate >= date '1994-01-01'
                    and o.o_orderdate < date '1994-01-01' + interval '1' year
                group by
                    n.n_name
                order by
                    revenue desc
            """
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

            queryId = "TEST_06"

            # Edited:
            # - Even that there is a difference with evaluations on Calcite,
            # the query passes on PySpark and BigQuery but fails on Drill
            # >=($6, -(0.06:DECIMAL(3, 2), 0.01:DECIMAL(3, 2))),
            #   <=($6, +(0.06:DECIMAL(3, 2), 0.01:DECIMAL(3, 2))) became
            # >=($2, 0.05:DECIMAL(4, 2)), <=($2, 0.07:DECIMAL(4, 2))

            query = """
                select
                    sum(l_extendedprice*l_discount) as revenue
                from
                    lineitem
                where
                    l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1994-01-01' + interval '1' year
                    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                    and l_quantity < 24
            """
            
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

            queryId = "TEST_07"

            # Edited:
            # - implicit joins without table aliases causes
            #   parsing errors on Drill

            query = """
                select
                    supp_nation,
                    cust_nation,
                    l_year, sum(volume) as revenue
                from (
                    select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        extract(year from l.l_shipdate) as l_year,
                        l.l_extendedprice * (1 - l.l_discount) as volume
                    from
                        supplier as s,
                        lineitem as l,
                        orders as o,
                        customer as c,
                        nation as n1,
                        nation as n2
                    where
                        s.s_suppkey = l.l_suppkey
                        and o.o_orderkey = l.l_orderkey
                        and c.c_custkey = o.o_custkey
                        and s.s_nationkey = n1.n_nationkey
                        and c.c_nationkey = n2.n_nationkey
                        and (
                            (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and l.l_shipdate between date '1995-01-01' and
                                                 date '1996-12-31'
                    ) as shipping
                group by
                    supp_nation,
                    cust_nation,
                    l_year
                order by
                    supp_nation,
                    cust_nation,
                    l_year
            """
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

            # Edited:
            # - implicit joins generated some condition=[true] on Blazingsql
            # - 'nation' colum name was renamed to nationl because it produces
            #   a parse error on Drill
            # - added table aliases to avoid ambiguity on Drill

            query = """
                select
                    o_year,
                    sum(case
                        when nationl = 'BRAZIL'
                        then volume
                        else 0
                    end) / sum(volume) as mkt_share
                from (
                    select
                        extract(year from o.o_orderdate) as o_year,
                        l.l_extendedprice * (1-l.l_discount) as volume,
                        n2.n_name as nationl
                    from
                        part as p
                        inner join lineitem as l on p.p_partkey = l.l_partkey
                        inner join supplier as s on s.s_suppkey = l.l_suppkey
                        inner join orders as o on o.o_orderkey = l.l_orderkey
                        inner join customer as c on c.c_custkey = o.o_custkey
                        inner join nation as n1 on
                            n1.n_nationkey = c.c_nationkey
                        inner join nation as n2 on
                            n2.n_nationkey = s.s_nationkey
                        inner join region as r on
                            r.r_regionkey = n1.n_regionkey
                    where
                        r.r_name = 'AMERICA'
                        and o.o_orderdate between date '1995-01-01' and
                                                  date '1996-12-31'
                        and p.p_type = 'ECONOMY ANODIZED STEEL'
                    ) as all_nations
                group by
                    o_year
                    order by
                    o_year
            """
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

            queryId = "TEST_09"

            # Edited:
            # - implicit joins generated some condition=[true] on Blazingsql
            # - 'nation' colum name was renamed to nationl because it
            #   produces a parse error on Drill
            # - implicit joins without table aliases causes parsing
            #   errors on Drill

            query = """
                select
                    nationl,
                    o_year,
                    sum(amount) as sum_profit
                from (
                    select
                        n.n_name as nationl,
                        extract(year from o.o_orderdate) as o_year,
                        l.l_extendedprice * (1 - l.l_discount) -
                            ps.ps_supplycost * l.l_quantity as amount
                    from
                        lineitem as l
                        inner join orders as o
                            on o.o_orderkey = l.l_orderkey
                        inner join partsupp as ps
                            on ps.ps_suppkey = l.l_suppkey
                        inner join part as p
                            on p.p_partkey = l.l_partkey
                        inner join supplier as s
                            on s.s_suppkey = l.l_suppkey
                        inner join nation as n
                            on n.n_nationkey = s.s_nationkey
                    where
                        l.l_partkey = ps.ps_partkey
                        and p.p_name like '%green%'
                    ) as profit
                group by
                    nationl,
                    o_year
                order by
                    nationl,
                    o_year desc
            """
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
                    True,
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
                    True,
                    fileSchemaType,
                )

            queryId = "TEST_10"

            # Edited:
            # - implicit joins without table aliases causes parsing
            #   errors on Drill
            # - no needed to converting to explicit joins, added
            #   only table aliases

            query = """
                select
                    c.c_custkey,
                    c.c_name,
                    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
                    c.c_acctbal,
                    n.n_name,
                    c.c_address,
                    c.c_phone,
                    c.c_comment
                from
                    customer c,
                    orders o,
                    lineitem l,
                    nation n
                where
                    c.c_custkey = o.o_custkey
                    and l.l_orderkey = o.o_orderkey
                    and o.o_orderdate >= date '1993-10-01'
                    and o.o_orderdate < date '1993-10-01' + interval '3' month
                    and l.l_returnflag = 'R'
                    and c.c_nationkey = n.n_nationkey
                group by
                    c.c_custkey,
                    c.c_name,
                    c.c_acctbal,
                    c.c_phone,
                    n.n_name,
                    c.c_address,
                    c.c_comment
                order by
                    revenue desc
                limit 20
            """
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

            queryId = "TEST_11"

            # Edited:
            # - 'value' colum name was renamed to valuep because it produces
            #   a parse error on Drill
            # WARNING: Join condition is currently not supported

            query = """
                select
                    ps_partkey,
                    sum(ps_supplycost * ps_availqty) as valuep
                from
                    partsupp,
                    supplier,
                    nation
                where
                    ps_suppkey = s_suppkey
                    and s_nationkey = n_nationkey
                    and n_name = 'GERMANY'
                group by
                    ps_partkey having
                        sum(ps_supplycost * ps_availqty) > (
                            select
                                sum(ps_supplycost * ps_availqty) * 0.0001
                            from
                                partsupp,
                                supplier,
                                nation
                            where
                                ps_suppkey = s_suppkey
                                and s_nationkey = n_nationkey
                                and n_name = 'GERMANY'
                        )
                order by
                    valuep desc
            """
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

            queryId = "TEST_12"

            # Edited:
            # - implicit joins without table aliases causes parsing
            #   errors on Drill
            # - no needed to converting to explicit joins, added
            #   only table aliases

            query = """
                select
                    l.l_shipmode,
                    sum(case
                        when o.o_orderpriority ='1-URGENT'
                            or o.o_orderpriority ='2-HIGH'
                        then 1
                        else 0
                    end) as high_line_count,
                    sum(case
                        when o.o_orderpriority <> '1-URGENT'
                            and o.o_orderpriority <> '2-HIGH'
                        then 1
                        else 0
                    end) as low_line_count
                from
                    orders o,
                    lineitem l
                where
                    o.o_orderkey = l.l_orderkey
                    and l.l_shipmode in ('MAIL', 'SHIP')
                    and l.l_commitdate < l.l_receiptdate
                    and l.l_shipdate < l.l_commitdate
                    and l.l_receiptdate >= date '1994-01-01'
                    and l.l_receiptdate < date '1994-01-01' +
                                                interval '1' year
                group by
                    l.l_shipmode
                order by
                    l.l_shipmode
            """
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

            # Edited:
            # - added table aliases to avoid ambiguity on Drill

            query = """
                select
                    c_count, count(*) as custdist
                from (
                    select
                        c.c_custkey,
                        count(o.o_orderkey)
                    from
                        customer c left outer join orders o on
                        c.c_custkey = o.o_custkey
                        and o.o_comment not like '%special%requests%'
                    group by
                        c.c_custkey
                    )as c_orders (c_custkey, c_count)
                group by
                    c_count
                order by
                    custdist desc,
                    c_count desc
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

            queryId = "TEST_14"

            # Edited:
            # - implicit joins without table aliases causes parsing
            #   errors on Drill
            # - no needed to converting to explicit joins, added
            #   only table aliases

            query = """
                select
                    100.00 * sum(case
                        when p.p_type like 'PROMO%'
                        then l.l_extendedprice*(1-l.l_discount)
                        else 0
                    end) / sum(l.l_extendedprice * (1 - l.l_discount))
                            as promo_revenue
                from
                    lineitem l,
                    part p
                where
                    l.l_partkey = p.p_partkey
                    and l.l_shipdate >= date '1995-09-01'
                    and l.l_shipdate < date '1995-09-01' + interval '1' month
            """
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

            queryId = "TEST_15"

            # Edited:
            # - Replacing 'create view' by 'with' clause
            # - Pyspark doest not support this syntax as is
            # WARNING: Drill presents undeterministic results

            query = """
                with revenue (suplier_no, total_revenue) as (
                    select
                        l_suppkey,
                        sum(l_extendedprice * (1-l_discount))
                    from
                        lineitem
                    where
                        l_shipdate >= date '1996-01-01'
                        and l_shipdate < date '1996-01-01' + interval '3' month
                    group by
                        l_suppkey
                )
                select
                    s_suppkey,
                    s_name,
                    s_address,
                    s_phone,
                    total_revenue
                from
                    supplier, revenue
                where
                    s_suppkey = suplier_no
                    and total_revenue = (
                        select
                            max(total_revenue)
                        from
                            revenue
                    )
                order by
                    s_suppkey
            """
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
            #     fileSchemaType
            # )

            queryId = "TEST_16"

            # Edited:
            # - implicit joins generated some condition=[true] on Blazingsql
            # - added table aliases to avoid ambiguity on Drill

            query = """
                select
                    p.p_brand,
                    p.p_type,
                    p.p_size,
                    count(distinct ps.ps_suppkey) as supplier_cnt
                from
                    partsupp ps
                    inner join part p on p.p_partkey = ps.ps_partkey
                where
                    p.p_brand <> 'Brand#45'
                    and p.p_type not like 'MEDIUM POLISHED%'
                    and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
                    and ps.ps_suppkey not in (
                        select
                            s_suppkey
                        from
                            supplier
                        where
                            s_comment like '%Customer%Complaints%'
                    )
                group by
                    p.p_brand,
                    p.p_type,
                    p.p_size
                order by
                    supplier_cnt desc,
                    p.p_brand,
                    p.p_type,
                    p.p_size
            """
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                '',
                acceptable_difference,
                use_percentage,
                fileSchemaType
            )

            queryId = "TEST_17"

            # Edited:
            # - implicit joins without table aliases causes parsing
            #   errors on Drill
            # - no needed to converting to explicit joins, added
            #   only table aliases
            # - this query fails on Drill with all format files,
            #   but passes on PySpark

            query = """
                select
                    sum(l.l_extendedprice) / 7.0 as avg_yearly
                from
                    lineitem l,
                    part p
                where
                    p.p_partkey = l.l_partkey
                    and p.p_brand = 'Brand#23'
                    and p.p_container = 'MED BOX'
                    and l.l_quantity < (
                        select
                            0.2 * avg(l_quantity)
                        from
                            lineitem
                        where
                            l_partkey = p_partkey)
            """
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

            queryId = "TEST_18"

            # Edited:
            # - implicit joins without table aliases causes parsing
            #   errors on Drill
            # - no needed to converting to explicit joins, added only
            #   table aliases

            query = """
                select
                    c.c_name,
                    c.c_custkey,
                    o.o_orderkey,
                    o.o_orderdate,
                    o.o_totalprice,
                    sum(l.l_quantity)
                from
                    customer c,
                    orders o,
                    lineitem l
                where
                    o.o_orderkey in (
                        select
                            l_orderkey
                        from
                            lineitem
                        group by
                            l_orderkey having
                            sum(l_quantity) > 300
                    )
                    and c.c_custkey = o.o_custkey
                    and o.o_orderkey = l.l_orderkey
                group by
                    c.c_name,
                    c.c_custkey,
                    o.o_orderkey,
                    o.o_orderdate,
                    o.o_totalprice
                order by
                    o.o_totalprice desc,
                    o.o_orderdate
                limit 100
            """
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

            queryId = "TEST_19"

            # Edited:
            # - implicit joins on Blazingsql generates 'Join condition is
            #   currently not supported' with
            #   LogicalJoin(condition=[OR(
            #       AND(=($10, $0), $11, $12, $2, $3, $13, $14, $4, $5),
            #       AND(=($10, $0), $15, $16, $6, $7, $13, $17, $4, $5),
            #       AND(=($10, $0), $18, $19, $8, $9, $13, $20, $4, $5))],
            #       joinType=[inner])
            #   also parsing errors on Drill
            # - added table aliases to avoid ambiguity on Drill

            query = """
                select
                    sum(l.l_extendedprice * (1 - l.l_discount) ) as revenue
                from
                    lineitem l
                inner join part p ON l.l_partkey = p.p_partkey
                where
                    (
                        p.p_brand = 'Brand#12'
                        and p.p_container in ('SM CASE', 'SM BOX',
                                              'SM PACK', 'SM PKG')
                        and l.l_quantity >= 1 and l.l_quantity <= 1 + 10
                        and p.p_size between 1 and 5
                        and l.l_shipmode in ('AIR', 'AIR REG')
                        and l.l_shipinstruct = 'DELIVER IN PERSON'
                    )
                    or
                    (
                        p.p_brand = 'Brand#23'
                        and p.p_container in ('MED BAG', 'MED BOX',
                                              'MED PKG', 'MED PACK')
                        and l.l_quantity >= 10 and l.l_quantity <= 10 + 10
                        and p.p_size between 1 and 10
                        and l.l_shipmode in ('AIR', 'AIR REG')
                        and l.l_shipinstruct = 'DELIVER IN PERSON'
                    )
                    or
                    (
                        p.p_brand = 'Brand#34'
                        and p.p_container in ('LG CASE', 'LG BOX',
                                              'LG PACK', 'LG PKG')
                        and l.l_quantity >= 20 and l.l_quantity <= 20 + 10
                        and p.p_size between 1 and 15
                        and l.l_shipmode in ('AIR', 'AIR REG')
                        and l.l_shipinstruct = 'DELIVER IN PERSON'
                    )
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

            queryId = "TEST_20"

            # Edited:
            # - implicit joins without table aliases causes parsing errors
            #   on Drill no needed to converting to explicit joins, added
            #   only table aliases
            # - this query fails on Drill with all format files, but passes
            #   on PySpark

            query = """
                select
                    s.s_name,
                    s.s_address
                from
                    supplier s, nation n
                where
                    s.s_suppkey in (
                        select
                            ps_suppkey
                        from
                            partsupp
                        where
                            ps_partkey in (
                                select
                                    p_partkey
                                from
                                    part
                                where
                                    p_name like 'forest%'
                            )
                        and ps_availqty > (
                            select
                                0.5 * sum(l_quantity)
                            from
                                lineitem
                            where
                                l_partkey = ps_partkey
                                and l_suppkey = ps_suppkey
                                and l_shipdate >= date '1994-01-01'
                                and l_shipdate <
                                    date '1994-01-01' + interval '1' year
                        )
                    )
                    and s.s_nationkey = n.n_nationkey
                    and n.n_name = 'CANADA'
                order by
                    s.s_name
            """
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

            queryId = "TEST_21"

            # Edited:
            # - implicit joins generated some condition=[true] on Blazingsql
            # - by comparing with Pyspark there is no needed
            #   of adding more table aliases

            query = """
                select
                    s_name,
                    count(*) as numwait
                from
                    supplier
                    inner join lineitem l1 on s_suppkey = l1.l_suppkey
                    inner join orders on o_orderkey = l1.l_orderkey
                    inner join nation on n_nationkey = s_nationkey
                where
                    o_orderstatus = 'F'
                    and l1.l_receiptdate > l1.l_commitdate
                    and exists (
                        select
                            *
                        from
                            lineitem l2
                        where
                            l2.l_orderkey = l1.l_orderkey
                            and l2.l_suppkey <> l1.l_suppkey
                    )
                    and not exists (
                        select
                            *
                        from
                            lineitem l3
                        where
                            l3.l_orderkey = l1.l_orderkey
                            and l3.l_suppkey <> l1.l_suppkey
                            and l3.l_receiptdate > l3.l_commitdate
                    )
                    and n_name = 'SAUDI ARABIA'
                group by
                    s_name
                order by
                    numwait desc,
                    s_name
                limit 100
            """
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

            queryId = "TEST_22"

            # WARNING: Join condition is currently not supported

            query = """
                select
                    cntrycode,
                    count(*) as numcust,
                    sum(c_acctbal) as totacctbal
                from (
                    select
                        substring(c_phone from 1 for 2) as cntrycode,
                        c_acctbal
                    from
                        customer
                    where
                        substring(c_phone from 1 for 2) in
                            ('13','31','23','29','30','18','17')
                        and c_acctbal > (
                            select
                                avg(c_acctbal)
                            from
                                customer
                            where
                                c_acctbal > 0.00
                                and substring (c_phone from 1 for 2) in
                                ('13','31','23','29','30','18','17')
                        )
                        and not exists (
                            select
                                *
                            from
                                orders
                            where
                                o_custkey = c_custkey
                        )
                    ) as custsale
                group by
                    cntrycode
                order by
                    cntrycode
            """
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

    if (
        Settings.execution_mode == ExecutionMode.FULL and
            compareResults == "true"
    ) or Settings.execution_mode == ExecutionMode.GENERATOR:
        # Create Table Drill -----------------------------------------
        print("starting drill")

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(
                            drill,
                            Settings.data["TestSettings"]["dataDirectory"]
        )

        # Create Table Spark -------------------------------------------------
        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(
                            spark,
                            Settings.data["TestSettings"]["dataDirectory"]
        )

    # Create Context For BlazingSQL

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
