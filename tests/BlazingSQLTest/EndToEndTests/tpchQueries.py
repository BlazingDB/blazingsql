query_templates = {
    "TEST_01": """
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
                {lineitem}
            where
                l_shipdate <= date '1998-12-01' - interval '90' day
            group by
                l_returnflag,
                l_linestatus
            order by
                l_returnflag,
                l_linestatus
        """,

    # Edited:
        # - implicit joins generated some condition=[true] on Blazingsql
        # - added table aliases to avoid ambiguity on Drill

    "TEST_02": """
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
                {supplier} as s
                inner join {nation} as n on s.s_nationkey = n.n_nationkey
                inner join {partsupp} as ps on s.s_suppkey = ps.ps_suppkey
                inner join {part} as p on p.p_partkey = ps.ps_partkey
                inner join {region} as r on r.r_regionkey = n.n_regionkey
            where
                p.p_size = 15
                and p.p_type like '%BRASS'
                and r.r_name = 'EUROPE'
                and ps.ps_supplycost = (
                    select
                        min(psq.ps_supplycost)
                    from
                        {partsupp} as psq
                        inner join {supplier} sq on
                            sq.s_suppkey = psq.ps_suppkey
                        inner join {nation} as nq on
                            sq.s_nationkey = nq.n_nationkey
                        inner join {region} as rq on
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
        """,

    # Edited:
        # - implicit joins without table aliases causes
        #   parsing errors on Drill
        # - added table aliases to avoid ambiguity on Drill
        # - There is an issue with validation on gpuci

    "TEST_03": """
            select
                l.l_orderkey,
                sum(l.l_extendedprice*(1-l.l_discount)) as revenue,
                o.o_orderdate,
                o.o_shippriority
            from
                {customer} c
                inner join {orders} o 
                on c.c_custkey = o.o_custkey
                inner join {lineitem} l
                on l.l_orderkey = o.o_orderkey
            where
                c.c_mktsegment = 'BUILDING'
                and o.o_orderdate < date '1995-03-15'
                and l.l_shipdate > date '1995-03-15'
            group by
                l.l_orderkey,
                o.o_orderdate,
                o.o_shippriority
            order by
                l.l_orderkey,
                revenue desc,
                o.o_orderdate
            limit 10
        """,

    # WARNING:
        # - Became implicit joins into explicit joins
        # - Fails with Drill, passes only with ORC files on PySpark
        # - Passes with BigQuery
        # - Blazingsql is returning different results
        #   for parquet, psv, and gdf

    "TEST_04": """
            select
                o.o_orderpriority,
                count(*) as order_count
            from
                {orders} o
            where
                o.o_orderdate >= date '1993-07-01'
                and o.o_orderdate < date '1993-07-01' + interval '3' month
                and exists (
                    select
                        *
                    from
                        {lineitem} l
                    where
                        l.l_orderkey = o.o_orderkey
                        and l.l_commitdate < l.l_receiptdate
                    )
            group by
                o.o_orderpriority
            order by
                o.o_orderpriority
        """,

    # Edited:
        # - implicit joins without table aliases causes
        #   parsing errors on Drill

    "TEST_05": """
            select
                n.n_name,
                sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
            from
                {customer} as c
                inner join {orders} as o
                on c.c_custkey = o.o_custkey
                inner join {lineitem} as l
                on l.l_orderkey = o.o_orderkey
                inner join {supplier} as s
                on l.l_suppkey = s.s_suppkey 
                inner join {nation} as n
                on s.s_nationkey = n.n_nationkey
                inner join {region} as r
                on n.n_regionkey = r.r_regionkey
                inner join {customer} c2
                on c2.c_nationkey = s.s_nationkey
            where
                r.r_name = 'ASIA'
                and o.o_orderdate >= date '1994-01-01'
                and o.o_orderdate < date '1995-01-01' 
            group by
                n.n_name,
                o.o_orderkey,
                l.l_linenumber
            order by
                revenue desc
        """,

    # Edited:
        # - Became implicit joins into explicit joins
        # - Added o.o_orderkey, l.l_linenumber into group by clause 
        # - Changed ('1994-01-01' + interval '1' year) by date '1995-01-01' 
        #   to became the query deterministic.
        # - Even that there is a difference with evaluations on Calcite,
        # the query passes on PySpark and BigQuery but fails on Drill
        # >=($6, -(0.06:DECIMAL(3, 2), 0.01:DECIMAL(3, 2))),
        #   <=($6, +(0.06:DECIMAL(3, 2), 0.01:DECIMAL(3, 2))) became
        # >=($2, 0.05:DECIMAL(4, 2)), <=($2, 0.07:DECIMAL(4, 2))

    "TEST_06": """
            select
                sum(l_extendedprice*l_discount) as revenue
            from
                {lineitem}
            where
                l_shipdate >= date '1994-01-01'
                and l_shipdate < date '1994-01-01' + interval '1' year
                and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                and l_quantity < 24
        """,

    # Edited:
        # - implicit joins without table aliases causes
        #   parsing errors on Drill

    "TEST_07": """
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
                    {supplier} as s
                    inner join {lineitem} as l 
                    on s.s_suppkey = l.l_suppkey
                    inner join {orders} as o
                    on o.o_orderkey = l.l_orderkey
                    inner join {customer} as c
                    on c.c_custkey = o.o_custkey
                    inner join {nation} as n1
                    on s.s_nationkey = n1.n_nationkey
                    inner join {nation} as n2
                    on c.c_nationkey = n2.n_nationkey
                where
                    (
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
        """,

    # Edited:
        # - implicit joins generated some condition=[true] on Blazingsql
        # - 'nation' colum name was renamed to nationl because it produces
        #   a parse error on Drill
        # - added table aliases to avoid ambiguity on Drill

    "TEST_08": """
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
                    {part} as p
                    inner join {lineitem} as l on p.p_partkey = l.l_partkey
                    inner join {supplier} as s on s.s_suppkey = l.l_suppkey
                    inner join {orders} as o on o.o_orderkey = l.l_orderkey
                    inner join {customer} as c on c.c_custkey = o.o_custkey
                    inner join {nation} as n1 on
                        n1.n_nationkey = c.c_nationkey
                    inner join {nation} as n2 on
                        n2.n_nationkey = s.s_nationkey
                    inner join {region} as r on
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
        """,

    # Edited:
        # - implicit joins generated some condition=[true] on Blazingsql
        # - 'nation' colum name was renamed to nationl because it
        #   produces a parse error on Drill
        # - implicit joins without table aliases causes parsing
        #   errors on Drill

    "TEST_09": """
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
                    {lineitem} as l
                    inner join {orders} as o
                        on o.o_orderkey = l.l_orderkey
                    inner join {partsupp} as ps
                        on ps.ps_suppkey = l.l_suppkey
                    inner join {part} as p
                        on p.p_partkey = l.l_partkey
                    inner join {supplier} as s
                        on s.s_suppkey = l.l_suppkey
                    inner join {nation} as n
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
        """,

    # Edited:
        # - implicit joins without table aliases causes parsing
        #   errors on Drill
        # - no needed to converting to explicit joins, added
        #   only table aliases
        # - order by c.c_custkey, with null data it is necessary
        #   for it to match with drill or spark, because there is a "Limit"

    "TEST_10": """
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
                {customer} c
                inner join {orders} o
                on c.c_custkey = o.o_custkey
                inner join {lineitem} l
                on l.l_orderkey = o.o_orderkey
                inner join {nation} n
                on c.c_nationkey = n.n_nationkey
            where
                o.o_orderdate >= date '1993-10-01'
                and o.o_orderdate < date '1993-10-01' + interval '3' month
                and l.l_returnflag = 'R'
            group by
                c.c_custkey,
                c.c_name,
                c.c_acctbal,
                c.c_phone,
                n.n_name,
                c.c_address,
                c.c_comment
            order by
                revenue desc,
                c.c_custkey
            limit 20
        """,

    # Edited:
        # - 'value' colum name was renamed to valuep because it produces
        #   a parse error on Drill
        # WARNING: Join condition is currently not supported

    "TEST_11": """
            select
                ps_partkey,
                sum(ps_supplycost * ps_availqty) as valuep
            from
                {partsupp},
                {supplier},
                {nation}
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
                            {partsupp},
                            {supplier},
                            {nation}
                        where
                            ps_suppkey = s_suppkey
                            and s_nationkey = n_nationkey
                            and n_name = 'GERMANY'
                    )
            order by
                valuep desc
        """,

    # Edited:
        # - implicit joins without table aliases causes parsing
        #   errors on Drill
        # - no needed to converting to explicit joins, added
        #   only table aliases

    "TEST_12": """
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
                {orders} o
                inner join {lineitem} l
                on o.o_orderkey = l.l_orderkey
            where
                l.l_shipmode in ('MAIL', 'SHIP')
                and l.l_commitdate < l.l_receiptdate
                and l.l_shipdate < l.l_commitdate
                and l.l_receiptdate >= date '1994-01-01'
                and l.l_receiptdate < date '1994-01-01' +
                                            interval '1' year
            group by
                l.l_shipmode
            order by
                l.l_shipmode
        """,

    # Edited:
        # - added table aliases to avoid ambiguity on Drill

    "TEST_13": """
            select
                c_count, count(*) as custdist
            from (
                select
                    c.c_custkey,
                    count(o.o_orderkey)
                from
                    {customer} c left outer join {orders} o on
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
        """,

    # Edited:
        # - implicit joins without table aliases causes parsing
        #   errors on Drill
        # - no needed to converting to explicit joins, added
        #   only table aliases

    "TEST_14": """
            select
                100.00 * sum(case
                    when p.p_type like 'PROMO%'
                    then l.l_extendedprice*(1-l.l_discount)
                    else 0
                end) / sum(l.l_extendedprice * (1 - l.l_discount))
                        as promo_revenue
            from
                {lineitem} l
                inner join {part} p
                on l.l_partkey = p.p_partkey
            where
                l.l_shipdate >= date '1995-09-01'
                and l.l_shipdate < date '1995-09-01' + interval '1' month
        """,

    "TEST_15": """
            with revenue (suplier_no, total_revenue) as (
                select
                    l_suppkey,
                    cast(sum(l_extendedprice * (1-l_discount)) AS INTEGER)
                from
                    {lineitem}
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
                {supplier}
                inner join revenue
                on s_suppkey = suplier_no
            where
                total_revenue = (
                    select
                        max(total_revenue)
                    from
                        revenue
                )
            order by
                s_suppkey
        """,

    # Edited:
        # - Replacing 'create view' by 'with' clause
        # - Pyspark doest not support this syntax as is
        # WARNING: Drill presents undeterministic results

    "TEST_16": """
            select
                p.p_brand,
                p.p_type,
                p.p_size,
                count(distinct ps.ps_suppkey) as supplier_cnt
            from
                {partsupp} ps
                inner join {part} p on p.p_partkey = ps.ps_partkey
            where
                p.p_brand <> 'Brand#45'
                and p.p_type not like 'MEDIUM POLISHED%'
                and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
                and ps.ps_suppkey not in (
                    select
                        s_suppkey
                    from
                        {supplier}
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
        """,

    # Edited:
        # - Became implicit joins into explicit joins
        # - implicit joins generated some condition=[true] on Blazingsql
        # - added table aliases to avoid ambiguity on Drill

    "TEST_17": """
            select
                sum(l.l_extendedprice) / 7.0 as avg_yearly
            from
                {lineitem} l
                inner join {part} p
                on p.p_partkey = l.l_partkey
            where
                p.p_brand = 'Brand#23'
                and p.p_container = 'MED BOX'
                and l.l_quantity < (
                    select
                        0.2 * avg(l_quantity)
                    from
                        {lineitem}
                    where
                        l_partkey = p_partkey)
        """,

    # Edited:
        # - became implicit joins into explicit joins 
        # - no needed to converting to explicit joins, added
        #   only table aliases
        # - this query fails on Drill with all format files,
        #   but passes on PySpark

    "TEST_18": """
            select
                c.c_name,
                c.c_custkey,
                o.o_orderkey,
                o.o_orderdate,
                o.o_totalprice,
                sum(l.l_quantity)
            from
                {customer} c
                inner join {orders} o
                on c.c_custkey = o.o_custkey
                inner join {lineitem} l
                on o.o_orderkey = l.l_orderkey
            where
                o.o_orderkey in (
                    select
                        l_orderkey
                    from
                        {lineitem}
                    group by
                        l_orderkey having
                        sum(l_quantity) > 300
                )
            group by
                c.c_name,
                c.c_custkey,
                o.o_orderkey,
                o.o_orderdate,
                o.o_totalprice
            order by
                o.o_totalprice desc,
                o.o_orderdate,
                o.o_orderkey,
                c.c_custkey
            limit 100
        """,

    # Edited:
        # - became implicit joins into explicit joins
        # - implicit joins without table aliases causes parsing
        #   errors on Drill
        # - no needed to converting to explicit joins, added only
        #   table aliases

    "TEST_19": """
            select
                sum(l.l_extendedprice * (1 - l.l_discount) ) as revenue
            from
                {lineitem} l
                inner join {part} p 
                ON l.l_partkey = p.p_partkey
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
        """,

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

    "TEST_20": """
            select
                s.s_name,
                s.s_address
            from
                {supplier} s
                inner join {nation} n
                on s.s_nationkey = n.n_nationkey
            where
                s.s_suppkey in (
                    select
                        ps_suppkey
                    from
                        {partsupp}
                    where
                        ps_partkey in (
                            select
                                p_partkey
                            from
                                {part}
                            where
                                p_name like 'forest%'
                        )
                    and ps_availqty > (
                        select
                            0.5 * sum(l_quantity)
                        from
                            {lineitem}
                        where
                            l_partkey = ps_partkey
                            and l_suppkey = ps_suppkey
                            and l_shipdate >= date '1994-01-01'
                            and l_shipdate <
                                date '1994-01-01' + interval '1' year
                    )
                )
                and n.n_name = 'CANADA'
            order by
                s.s_name
        """,

    # Edited:
        # - implicit joins without table aliases causes parsing errors
        #   on Drill no needed to converting to explicit joins, added
        #   only table aliases
        # - this query fails on Drill with all format files, but passes
        #   on PySpark

    "TEST_21": """
            select
                s_name,
                count(*) as numwait
            from
                {supplier}
                inner join {lineitem} l1 on s_suppkey = l1.l_suppkey
                inner join {orders} on o_orderkey = l1.l_orderkey
                inner join {nation} on n_nationkey = s_nationkey
            where
                o_orderstatus = 'F'
                and l1.l_receiptdate > l1.l_commitdate
                and exists (
                    select
                        *
                    from
                        {lineitem} l2
                    where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
                )
                and not exists (
                    select
                        *
                    from
                        {lineitem} l3
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
        """,

    # Edited:
        # - implicit joins generated some condition=[true] on Blazingsql
        # - by comparing with Pyspark there is no needed
        #   of adding more table aliases

    "TEST_22": """
            select
                cntrycode,
                count(*) as numcust,
                sum(c_acctbal) as totacctbal
            from (
                select
                    substring(c_phone from 1 for 2) as cntrycode,
                    c_acctbal
                from
                    {customer}
                where
                    substring(c_phone from 1 for 2) in
                        ('13','31','23','29','30','18','17')
                    and c_acctbal > (
                        select
                            avg(c_acctbal)
                        from
                            {customer}
                        where
                            c_acctbal > 0.00
                            and substring (c_phone from 1 for 2) in
                            ('13','31','23','29','30','18','17')
                    )
                    and not exists (
                        select
                            *
                        from
                            {orders}
                        where
                            o_custkey = c_custkey
                    )
                ) as custsale
            group by
                cntrycode
            order by
                cntrycode
        """

        # WARNING: Join condition is currently not supported
}


def map_tables(tpch_query, tables = {}):
    tpch_tables = {
        'nation': tables.get("nation", "nation"),
        'region': tables.get("region", "region"),
        'customer': tables.get("customer", "customer"),
        'lineitem': tables.get("lineitem", "lineitem"),
        'orders': tables.get("orders", "orders"),
        'supplier': tables.get("supplier", "supplier"),
        'part': tables.get("part", "part"),
        'partsupp': tables.get("partsupp", "partsupp")
    }

    return tpch_query.format(**tpch_tables)


def get_tpch_query(test_id, tables = {}):
    return map_tables(query_templates.get(test_id), tables)
