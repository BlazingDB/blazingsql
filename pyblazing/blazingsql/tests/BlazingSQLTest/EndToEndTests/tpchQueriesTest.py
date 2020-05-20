from pydrill.client import PyDrill
from DataBase import createSchema as cs
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution
from blazingsql import BlazingContext
from Utils import gpuMemory, test_name, skip_test, init_context
from pynvml import *
from blazingsql import DataType
from Configuration import ExecutionMode

def main(dask_client, drill, dir_data_file, bc, nRals):
    
    start_mem = gpuMemory.capture_gpu_memory_usage()
    
    queryType = 'TPCH Queries' 
    
    def executionTest(queryType):   
    
        tables = ["nation", "region", "customer", "lineitem", "orders", "supplier", "part", "partsupp"]
        data_types =  [DataType.DASK_CUDF, DataType.CUDF, DataType.CSV, DataType.ORC, DataType.PARQUET] # TODO json
        
        #Create Tables ------------------------------------------------------------------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType): continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)
        
            #Run Query -----------------------------------------------------------------------------
            worder = 1 #Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.001
            
            print('==============================')
            print(queryType)
            print('==============================')
            
            
            queryId = 'TEST_01'                        
            query = """ select
                    l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,
                    sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
                    sum(l_quantity)/count(l_quantity) as avg_qty, sum(l_extendedprice)/count(l_extendedprice) as avg_price, sum(l_discount)/count(l_discount) as avg_disc,
                    count(*) as count_order
                from 
                    lineitem
                where
                    l_shipdate <= date '1998-09-01'
                group by
                    l_returnflag, l_linestatus
                order by
                    l_returnflag, l_linestatus"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
            queryId = 'TEST_02' 
            query = """ select 
                        s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment
                    from 
                        supplier as s 
                    INNER JOIN nation as n ON s.s_nationkey = n.n_nationkey 
                    INNER JOIN partsupp as ps ON s.s_suppkey = ps.ps_suppkey
                    INNER JOIN part as p ON p.p_partkey = ps.ps_partkey 
                    INNER JOIN region as r ON r.r_regionkey = n.n_regionkey
                    where r.r_name = 'EUROPE' and p.p_size = 15
                        and p.p_type like '%BRASS'
                        and ps.ps_supplycost = (
                            select 
                                min(psq.ps_supplycost)
                            from 
                                partsupp as psq
                            INNER JOIN supplier as sq ON sq.s_suppkey = psq.ps_suppkey
                            INNER JOIN nation as nq ON nq.n_nationkey = sq.s_nationkey
                            INNER JOIN region as rq ON rq.r_regionkey = nq.n_regionkey
                            where
                                rq.r_name = 'EUROPE'
                        )
                    order by 
                        s.s_acctbal desc, n.n_name, s.s_name, p.p_partkey"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
            queryId = 'TEST_03'
            query = """ select 
                    l.l_orderkey, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue, o.o_orderdate, o.o_shippriority
                from 
                    orders as o
                INNER JOIN lineitem as l ON l.l_orderkey = o.o_orderkey
                INNER JOIN customer as c ON c.c_custkey = o.o_custkey
                where
                    c.c_mktsegment = 'BUILDING'
                    and o.o_orderdate < date '1995-03-15' 
                    and l.l_shipdate > date '1995-03-15'
                group by
                    l.l_orderkey, o.o_orderdate, o.o_shippriority
                order by
                    revenue desc, o.o_orderdate"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
            queryId = 'TEST_04'
            query = """ select
                    o_orderpriority, count(*) as order_count
                from
                    orders
                where
                    o_orderdate >= date '1993-07-01'
                    and o_orderdate < date '1994-10-01'
                    and exists (select
                                    *
                                    from
                                    lineitem
                                    where
                                    l_orderkey = o_orderkey
                                    and l_commitdate < l_receiptdate)
                group by
                    o_orderpriority 
                order by 
                    o_orderpriority"""
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0, use_percentage, fileSchemaType)
    
            queryId = 'TEST_05'
            query = """ select
                        n.n_name, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
                    from orders o 
                    inner join lineitem l on l.l_orderkey = o.o_orderkey
                    inner join customer c on o.o_custkey = c.c_custkey
                    inner join supplier s on l.l_suppkey = s.s_suppkey and c.c_nationkey = s.s_nationkey
                    inner join nation n on n.n_nationkey = s.s_nationkey
                    inner join region r on n.n_regionkey = r.r_regionkey
                    where
                        r.r_name = 'ASIA' 
                        and o.o_orderdate >= date '1994-01-01'
                        and o.o_orderdate < date '1995-01-01'                    
                    group by
                        n.n_name
                    order by
                        revenue desc"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.01, use_percentage, fileSchemaType)
    
            queryId = 'TEST_06'
            query = """ select
                    sum(l_extendedprice*l_discount) as revenue
                from
                    lineitem
                where
                    l_shipdate >= date '1994-01-01' 
                    and l_shipdate < date '1995-01-01'
                    and l_discount between 0.05 and 0.07 
                    and l_quantity < 24"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.1, use_percentage, fileSchemaType)
            
            #FALTA MODIFICAR
            queryId = 'TEST_07'
            query = """ select
                            supp_nation, cust_nation, l_year, sum(volume) as revenue
                        from 
                            (select 
                                n1.n_name as supp_nation, n2.n_name as cust_nation,
                                extract(year from l.l_shipdate) as l_year, l.l_extendedprice * (1 - l.l_discount) as volume
                            from 
                                nation as n1
                            INNER JOIN supplier as s ON s.s_nationkey = n1.n_nationkey
                            INNER JOIN lineitem as l ON l.l_suppkey = s.s_suppkey 
                            INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey
                            INNER JOIN customer as c ON c.c_custkey = o.o_custkey
                            INNER JOIN nation as n2 ON n2.n_nationkey = c.c_nationkey
                            where
                                (
                                    (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                                    or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') 
                                )
                            and l.l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping
                        group by
                            supp_nation, cust_nation, l_year
                        order by 
                            supp_nation, cust_nation, l_year
                            """
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
            queryId = 'TEST-08'
            query = """ select 
                    o_year, sum(case when nationl = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
                from (
                    select 
                        extract(year from o.o_orderdate) as o_year, l.l_extendedprice * (1 - l.l_discount) as volume, n2.n_name as nationl
                    from 
                        part as p
                    INNER JOIN lineitem as l ON p.p_partkey = l.l_partkey
                    INNER JOIN supplier as s ON s.s_suppkey = l.l_suppkey
                    INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey 
                    INNER JOIN customer as c ON c.c_custkey = o.o_custkey
                    INNER JOIN nation as n1 ON n1.n_nationkey = c.c_nationkey 
                    INNER JOIN region as r ON r.r_regionkey = n1.n_regionkey
                    INNER JOIN nation as n2 ON n2.n_nationkey = s.s_nationkey
                    where 
                        r.r_name = 'AMERICA' 
                        and o.o_orderdate >= date '1995-01-01' and o.o_orderdate <= date '1996-12-31'
                        and p.p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
                group by
                    o_year
                order by
                    o_year"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
            queryId = 'TEST_09'
            query = """ select
                    nationl, o_year, sum(amount) as sum_profit
                from
                    ( select n_name as nationl, extract(year from o_orderdate) as o_year,
                            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                        from lineitem
                        INNER JOIN orders ON o_orderkey = l_orderkey
                        INNER JOIN partsupp ON ps_suppkey = l_suppkey and ps_partkey = l_partkey
                        INNER JOIN part ON p_partkey = l_partkey
                        INNER JOIN supplier ON s_suppkey = l_suppkey
                        INNER JOIN nation ON n_nationkey = s_nationkey                     
                        where
                            p_name like '%green%' ) as profit
                group by 
                    nationl, o_year 
                order by 
                    nationl, o_year desc"""
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
            queryId = 'TEST_10'
            query = """ select
                    c.c_custkey, c.c_name, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue, c.c_acctbal,
                    n.n_name, c.c_address, c.c_phone, c.c_comment
                from
                    customer as c
                INNER JOIN nation as n ON n.n_nationkey = c.c_nationkey
                INNER JOIN orders as o ON o.o_custkey = c.c_custkey
                INNER JOIN lineitem as l ON l.l_orderkey = o.o_orderkey
                where 
                o.o_orderdate >= date '1993-10-01'
                and o.o_orderdate < date '1994-10-01'
                and l.l_returnflag = 'R'
                group by
                    c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            #falta cambiar
            queryId = 'TEST_11'
            query = """ select
                            ps.ps_partkey, sum(ps.ps_supplycost * ps.ps_availqty) as valuep
                        from 
                            partsupp as ps
                        INNER JOIN supplier as s ON ps.ps_suppkey = s.s_suppkey
                        INNER JOIN nation as n ON s.s_nationkey = n.n_nationkey
                        where
                            n.n_name = 'GERMANY'
                        group by 
                            ps.ps_partkey having sum(ps.ps_supplycost * ps.ps_availqty) > ( select
                                                                        sum(psq.ps_supplycost)
                                                                        from
                                                                            partsupp as psq
                                                                        INNER JOIN supplier as sq ON psq.ps_suppkey = sq.s_suppkey
                                                                        INNER JOIN nation as nq ON sq.s_nationkey = nq.n_nationkey
                                                                        where
                                                                            nq.n_name = 'GERMANY'
                                                                        )
                        order by 
                            valuep desc"""
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.01, use_percentage, fileSchemaType)
    
            queryId = 'TEST_12'
            query = """ select
                    l.l_shipmode, sum(case when o.o_orderpriority ='1-URGENT' or o.o_orderpriority ='2-HIGH'
                    then 1 else 0 end) as high_line_count, sum(case when o.o_orderpriority <> '1-URGENT'
                    and o.o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
                from
                    lineitem as l
                INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey
                where
                    l.l_shipmode in ('MAIL', 'SHIP') 
                    and l.l_commitdate < l.l_receiptdate
                    and l.l_shipdate < l.l_commitdate 
                    and l.l_receiptdate >= date '1994-01-01'
                    and l.l_receiptdate < date '1995-01-01'
                group by l.l_shipmode
                order by l.l_shipmode"""

            if fileSchemaType != DataType.ORC: # TODO CRASH percy kharoly c.cordova rommel we should fix this
                runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
        
            queryId = 'TEST_13'
            query = """ select
                    c_count, count(*) as custdist
                from
                    (select
                        c.c_custkey, count(o.o_orderkey)
                    from
                        customer as c
                    LEFT OUTER JOIN orders as o ON c.c_custkey = o.o_custkey
                    where o.o_comment not like '%special%requests%'
                    group by
                        c.c_custkey) as c_orders (c_custkey, c_count)
                group by
                    c_count
                order by
                    custdist desc, c_count desc"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
            queryId = 'TEST_14'
            query = """ select 100.00 * sum( case when p.p_type like 'PROMO%' then l.l_extendedprice * (1 - l.l_discount)
                                            else 0 end) / sum(l.l_extendedprice * (1 - l.l_discount) ) as promo_revenue
                        from 
                            lineitem as l
                        INNER JOIN part as p ON p.p_partkey = l.l_partkey
                        where
                            l.l_shipdate >= date '1995-09-01' 
                            and l.l_shipdate < date '1995-10-01'"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
            queryId = 'TEST_15'
            query = """with revenue (suplier_no, total_revenue) as
                    (
                    select
                    l_suppkey, cast(sum(l_extendedprice * (1-l_discount)) as int)
                    from
                    lineitem
                    where
                    l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'
                    group by
                    l_suppkey
                    )
                    select
                    s.s_suppkey, s.s_name, s.s_address, s.s_phone, re.total_revenue
                    from
                    supplier as s
                    INNER JOIN revenue as re on s.s_suppkey = re.suplier_no
                    where
                    re.total_revenue = cast((select max(total_revenue) from revenue) as int)
                    order by
                    s.s_suppkey"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            #falta cambiar
            queryId = 'TEST_16'
            query = """ select
                            p.p_brand, p.p_type, p.p_size, count(distinct ps.ps_suppkey) as supplier_cnt
                        from partsupp ps
                        inner join part p on p.p_partkey = ps.ps_partkey
                        where
                            p.p_brand <> 'Brand#45'
                            and p.p_type not like 'MEDIUM POLISHED%' and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
                            and ps.ps_suppkey not in (select s.s_suppkey from supplier s where s.s_comment like '%Customer%Complaints%')
                        group by
                            p.p_brand, p.p_type, p.p_size
                        order by
                            supplier_cnt desc, p.p_brand, p.p_type, p.p_size"""
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            #falta cambiar
            queryId = 'TEST_17'
            query = """ select
                            sum(l1.l_extendedprice) / 7.0 as avg_yearly
                        from lineitem l1
                        inner join part p on p.p_partkey = l1.l_partkey
                        where
                            p.p_brand = 'Brand#23' and p.p_container = 'MED BOX'
                            and l1.l_quantity < (select 0.2 * avg(l2.l_quantity) from lineitem l2
                                            where l2.l_partkey = p.p_partkey)"""
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            queryId = 'TEST_18'
            query = """ select
                    c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity)
                from
                    customer c
                    inner join orders o on c.c_custkey = o.o_custkey
                    inner join lineitem l on o.o_orderkey = l.l_orderkey
                where
                    o.o_orderkey in (select l2.l_orderkey from lineitem l2 group by l2.l_orderkey having
                                    sum(l2.l_quantity) > 300)
                group by
                    c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice
                order by
                    o.o_totalprice desc, o.o_orderdate"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            #falta cambiar
            queryId = 'TEST_19'
            query = """ select 
                            sum(l.l_extendedprice * (1 - l.l_discount) ) as revenue
                        from 
                            lineitem as l
                        INNER JOIN part as p ON l.l_partkey = p.p_partkey
                        where
                            (
                            p.p_brand = 'Brand#12'
                            and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                            and l.l_quantity >= 1 and l.l_quantity <= 11
                            and p.p_size between 1 and 5
                            and l.l_shipmode in ('AIR', 'AIR REG')
                            and l.l_shipinstruct = 'DELIVER IN PERSON'
                            )
                            or
                            (
                            p.p_brand = 'Brand#23'
                            and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                            and l.l_quantity >= 10 and l.l_quantity <= 20
                            and p.p_size between 1 and 10
                            and l.l_shipmode in ('AIR', 'AIR REG')
                            and l.l_shipinstruct = 'DELIVER IN PERSON'
                            )
                            or
                            (
                            p.p_brand = 'Brand#34'
                            and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                            and l.l_quantity >= 20 and l.l_quantity <= 30
                            and p.p_size between 1 and 15
                            and l.l_shipmode in ('AIR', 'AIR REG')
                            and l.l_shipinstruct = 'DELIVER IN PERSON'
                            )"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)        
            #falta cambiar  
            queryId = 'TEST_20'
            query = """ select s.s_name, s.s_address
                        from supplier s
                        inner join nation n on s.s_nationkey = n.n_nationkey
                        where
                            s.s_suppkey in (select ps.ps_suppkey from partsupp ps where ps.ps_partkey in (select p.p_partkey
                                                                                                from part p where
                                                                                            p.p_name like 'forest%')
                                        and ps_availqty > (select 0.5 * sum(l.l_quantity) from lineitem l where
                                                            l.l_partkey = ps.ps_partkey and l.l_suppkey = ps.ps_suppkey
                                                            and l.l_shipdate >= date '1994-01-01' 
                                                            and l.l_shipdate < date '1994-01-01' - interval '1' year))
                            and n.n_name = 'CANADA'
                        order by s.s_name"""
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            #falta cambiar  
            queryId = 'TEST_21'
            query = """ select
                            s_name, count(*) as numwait
                        from
                            supplier
                            inner join lineitem l1 on s_suppkey = l1.l_suppkey
                            inner join orders on o_orderkey = l1.l_orderkey
                            inner join nation on s_nationkey = n_nationkey
                        where
                            o_orderstatus = 'F'
                            and l1.l_receiptdate > l1.l_commitdate and exists (select * from lineitem l2
                                                                                where l2.l_orderkey = l1.l_orderkey
                                                                                    and l2.l_suppkey <> l1.l_suppkey)
                            and not exists (select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey
                                            and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate)
                            and n_name = ' SAUDI ARABIA'
                        group by
                            s_name
                        order by
                            numwait desc, s_name"""
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            #falta cambiar  
            queryId = 'TEST_22'
            query = """ select
                            cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
                        from (select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer
                            where substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17')
                            and c_acctbal > (select avg(c_acctbal) from customer where c_acctbal > 0.00
                            and substring (c_phone from 1 for 2) in ('13','31','23','29','30','18','17'))
                            and not exists (select * from orders where o_custkey = c_custkey)) as custsale
                        group by cntrycode
                        order by cntrycode"""
            #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break
          
    executionTest(queryType)
    
    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
    
if __name__ == '__main__':

    Execution.getArgs()
    
    nvmlInit()

    drill = "drill" #None

    compareResults = True
    if 'compare_results' in Settings.data['RunSettings']:
        compareResults = Settings.data['RunSettings']['compare_results'] 

    if (Settings.execution_mode == ExecutionMode.FULL_MODE and compareResults == "true") or Settings.execution_mode == ExecutionMode.GENERATOR:
        # Create Table Drill ------------------------------------------------------------------------------------------------------
        print("starting drill")
        from pydrill.client import PyDrill
        drill = PyDrill(host = 'localhost', port = 8047)
        cs.init_drill_schema(drill, Settings.data['TestSettings']['dataDirectory'])

    #Create Context For BlazingSQL
    
    bc, dask_client = init_context()

    nRals = Settings.data['RunSettings']['nRals']

    main(dask_client, drill, Settings.data['TestSettings']['dataDirectory'], bc, nRals)
    
    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
    

# from DataBase import createSchema as cs
# from Configuration import Settings as Settings
# from Runner import runTest
# from Utils import Execution
# from blazingsql import DataType, S3EncryptionType
# from Utils import gpuMemory, test_name, init_context, skip_test
# from pynvml import *

# def main(dask_client, drill, dir_data_lc, bc, nRals):
    
#     start_mem = gpuMemory.capture_gpu_memory_usage()
    
#     queryType = 'TPCH Queries' 

#     def create_tables_sf1000(table, s3_path):

#         if table == "nation" or table == "region":
#             bc.create_table(table, s3_path + "/" + '0_0_0.parquet')
#         else:
#             order_files =[]
#             for i in range(300):
#                 order_files.append(s3_path + "/" +str(i)+'.parquet')
#             bc.create_table(table, order_files)

#         print(table + " table  created")

#     def create_tables_sf100():
#         # create tpch tables
#         order_files =[]
#         for i in range(16):
#             order_files.append('s3://bsql_data/tpch_sf100/orders/0_0_'+str(i)+'.parquet')
#         bc.create_table('orders', order_files)

#         # create tpch tables
#         lineitem_files =[]
#         for i in range(72):
#             lineitem_files.append('s3://bsql_data/tpch_sf100/lineitem/0_0_'+str(i)+'.parquet')
#         bc.create_table('lineitem', lineitem_files)

#         # create tpch tables
#         customer_files =[]
#         for i in range(4):
#             customer_files.append('s3://bsql_data/tpch_sf100/customer/0_0_'+str(i)+'.parquet')
#         bc.create_table('customer', customer_files)


#         # create tpch tables
#         part_files =[]
#         for i in range(2):
#             part_files.append('s3://bsql_data/tpch_sf100/part/0_0_'+str(i)+'.parquet')
#         bc.create_table('part', part_files)

#         # create tpch tables
#         partsupp_files =[]
#         for i in range(6):
#             partsupp_files.append('s3://bsql_data/tpch_sf100/partsupp/0_0_'+str(i)+'.parquet')
#         bc.create_table('partsupp', partsupp_files)

#         # create tpch tables
#         supplier_files =[]
#         for i in range(1):
#             supplier_files.append('s3://bsql_data/tpch_sf100/supplier/0_0_'+str(i)+'.parquet')
#         bc.create_table('supplier', supplier_files)

#         bc.create_table('nation', 's3://bsql_data/tpch_sf1/nation/0_0_0.parquet')
#         bc.create_table('region', 's3://bsql_data/tpch_sf1/region/0_0_0.parquet')

#     def create_tables_sf300():
#         order_files =[]
#         for i in range(99):
#             order_files.append('s3://bsql_data/tpch_sf300/orders/'+str(i)+'.parquet')
#         bc.create_table('orders', order_files)

#         # create tpch tables
#         lineitem_files =[]
#         for i in range(99):
#             lineitem_files.append('s3://bsql_data/tpch_sf300/lineitem/'+str(i)+'.parquet')
#         bc.create_table('lineitem', lineitem_files)

#         # create tpch tables
#         customer_files =[]
#         for i in range(99):
#             customer_files.append('s3://bsql_data/tpch_sf300/customer/'+str(i)+'.parquet')
#         bc.create_table('customer', customer_files)


#         # create tpch tables
#         part_files =[]
#         for i in range(99):
#             part_files.append('s3://bsql_data/tpch_sf300/part/'+str(i)+'.parquet')
#         bc.create_table('part', part_files)

#         # create tpch tables
#         partsupp_files =[]
#         for i in range(99):
#             partsupp_files.append('s3://bsql_data/tpch_sf300/partsupp/'+str(i)+'.parquet')
#         bc.create_table('partsupp', partsupp_files)

#         # create tpch tables
#         supplier_files =[]
#         for i in range(1):
#             supplier_files.append('s3://bsql_data/tpch_sf300/supplier/'+str(i)+'.parquet')
#         bc.create_table('supplier', supplier_files)

#         bc.create_table('nation', 's3://bsql_data/tpch_sf1/nation/0_0_0.parquet')
#         bc.create_table('region', 's3://bsql_data/tpch_sf1/region/0_0_0.parquet')
    
#     def executionTest(queryType): 

#         #Read Data TPCH------------------------------------------------------------------------------------------------------------
#         authority = "bsql_data"

#         dir_data_fs=""
#         dir_data_alt=""
        
#         if Settings.data['TestSettings']['dataSize'] == 'sf100' or Settings.data['TestSettings']['dataSize'] == 'sf1000':  
#             bc.s3(authority, bucket_name='blazingsql-colab')
#             s3_folder = "tpch_sf1000" #Settings.data['TestSettings']['S3Folder']
#             dir_data_fs = "s3://" + authority + "/" + s3_folder + "/"
#             dir_data_alt = "s3://" + authority + "/" + "tpch_sf1" + "/" #for nation & region
#         #else:
          # TODO percy kharo e2e-gpuci security
#         bc.s3(authority, bucket_name='blazingsql-bucket', encryption_type=S3EncryptionType.NONE,


#         access_key_id='', secret_key='')
#         dir_data_fs = "s3://" + authority + "/" + "DataSet100Mb2part/" #dir_df  
        
#         tables = ["nation", "region", "customer", "lineitem", "orders", "supplier", "part", "partsupp"]

#         data_types =  [DataType.DASK_CUDF, DataType.CUDF, DataType.CSV, DataType.ORC, DataType.PARQUET] # TODO json DataType.CSV, 
    
#         for fileSchemaType in data_types:
#             if skip_test(dask_client, nRals, fileSchemaType, queryType): continue
#             #cs.create_tables(bc, dir_data_fs, fileSchemaType, tables=tables)
            
#             # if Settings.data['TestSettings']['dataSize'] == 'sf100':
#             #     create_tables_sf100()
#             # elif Settings.data['TestSettings']['dataSize'] == 'sf1000':
#             #     for table in tables:
#             #         if table == "nation" or table == "region":
#             #             s3_path = dir_data_alt + table + "/"
#             #         else:
#             #             s3_path = dir_data_fs + table + "/"
#             #         create_tables(table, s3_path)
#             # else:
            
#             cs.create_tables(bc, dir_data_fs, fileSchemaType, tables=tables)

#             #create_tables_300()
                
#         #   Run Query -----------------------------------------------------------------------------
#             worder = 1 # Parameter to indicate if its necessary to order the resulsets before compare them
#             use_percentage = False
#             acceptable_difference = 0.01
                     
#             print('==============================')
#             print(queryType)
#             print('==============================')
           
#             queryId = 'TEST_01'                        
#             query = """ select
#                     l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,
#                     sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
#                     sum(l_quantity)/count(l_quantity) as avg_qty, sum(l_extendedprice)/count(l_extendedprice) as avg_price, sum(l_discount)/count(l_discount) as avg_disc,
#                     count(*) as count_order
#                 from 
#                     lineitem
#                 where
#                     l_shipdate <= date '1998-09-01'
#                 group by
#                     l_returnflag, l_linestatus
#                 order by
#                     l_returnflag, l_linestatus"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_02' 
#             query = """ select 
#                         s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment
#                     from 
#                         supplier as s 
#                     INNER JOIN nation as n ON s.s_nationkey = n.n_nationkey 
#                     INNER JOIN partsupp as ps ON s.s_suppkey = ps.ps_suppkey
#                     INNER JOIN part as p ON p.p_partkey = ps.ps_partkey 
#                     INNER JOIN region as r ON r.r_regionkey = n.n_regionkey
#                     where r.r_name = 'EUROPE' and p.p_size = 15
#                         and p.p_type like '%BRASS'
#                         and ps.ps_supplycost = (
#                             select 
#                                 min(psq.ps_supplycost)
#                             from 
#                                 partsupp as psq
#                             INNER JOIN supplier as sq ON sq.s_suppkey = psq.ps_suppkey
#                             INNER JOIN nation as nq ON nq.n_nationkey = sq.s_nationkey
#                             INNER JOIN region as rq ON rq.r_regionkey = nq.n_regionkey
#                             where
#                                 rq.r_name = 'EUROPE'
#                         )
#                     order by 
#                         s.s_acctbal desc, n.n_name, s.s_name, p.p_partkey"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_03'
#             query = """ select 
#                     l.l_orderkey, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue, o.o_orderdate, o.o_shippriority
#                 from 
#                     orders as o
#                 INNER JOIN lineitem as l ON l.l_orderkey = o.o_orderkey
#                 INNER JOIN customer as c ON c.c_custkey = o.o_custkey
#                 where
#                     c.c_mktsegment = 'BUILDING'
#                     and o.o_orderdate < date '1995-03-15' 
#                     and l.l_shipdate > date '1995-03-15'
#                 group by
#                     l.l_orderkey, o.o_orderdate, o.o_shippriority
#                 order by
#                     revenue desc, o.o_orderdate"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_04'
#             query = """ select
#                     o_orderpriority, count(*) as order_count
#                 from
#                     orders
#                 where
#                     o_orderdate >= date '1993-07-01'
#                     and o_orderdate < date '1994-10-01'
#                     and exists (select
#                                     *
#                                     from
#                                     lineitem
#                                     where
#                                     l_orderkey = o_orderkey
#                                     and l_commitdate < l_receiptdate)
#                 group by
#                     o_orderpriority 
#                 order by 
#                     o_orderpriority"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_05'
#             query = """ select
#                         n.n_name, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
#                     from orders o 
#                     inner join lineitem l on l.l_orderkey = o.o_orderkey
#                     inner join customer c on o.o_custkey = c.c_custkey
#                     inner join supplier s on l.l_suppkey = s.s_suppkey and c.c_nationkey = s.s_nationkey
#                     inner join nation n on n.n_nationkey = s.s_nationkey
#                     inner join region r on n.n_regionkey = r.r_regionkey
#                     where
#                         r.r_name = 'ASIA' 
#                         and o.o_orderdate >= date '1994-01-01'
#                         and o.o_orderdate < date '1995-01-01'                    
#                     group by
#                         n.n_name
#                     order by
#                         revenue desc"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.01, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_06'
#             query = """ select
#                     sum(l_extendedprice*l_discount) as revenue
#                 from
#                     lineitem
#                 where
#                     l_shipdate >= date '1994-01-01' 
#                     and l_shipdate < date '1995-01-01'
#                     and l_discount between 0.05 and 0.07 
#                     and l_quantity < 24"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.1, use_percentage, fileSchemaType)
            
#             #FALTA MODIFICAR
#             queryId = 'TEST_07'
#             query = """ select
#                             supp_nation, cust_nation, l_year, sum(volume) as revenue
#                         from 
#                             (select 
#                                 n1.n_name as supp_nation, n2.n_name as cust_nation,
#                                 extract(year from l.l_shipdate) as l_year, l.l_extendedprice * (1 - l.l_discount) as volume
#                             from 
#                                 nation as n1
#                             INNER JOIN supplier as s ON s.s_nationkey = n1.n_nationkey
#                             INNER JOIN lineitem as l ON l.l_suppkey = s.s_suppkey 
#                             INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey
#                             INNER JOIN customer as c ON c.c_custkey = o.o_custkey
#                             INNER JOIN nation as n2 ON n2.n_nationkey = c.c_nationkey
#                             where
#                                 (
#                                     (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
#                                     or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') 
#                                 )
#                             and l.l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping
#                         group by
#                             supp_nation, cust_nation, l_year
#                         order by 
#                             supp_nation, cust_nation, l_year
#                             """
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
#             queryId = 'TEST-08'
#             query = """ select 
#                     o_year, sum(case when nationl = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
#                 from (
#                     select 
#                         extract(year from o.o_orderdate) as o_year, l.l_extendedprice * (1 - l.l_discount) as volume, n2.n_name as nationl
#                     from 
#                         part as p
#                     INNER JOIN lineitem as l ON p.p_partkey = l.l_partkey
#                     INNER JOIN supplier as s ON s.s_suppkey = l.l_suppkey
#                     INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey 
#                     INNER JOIN customer as c ON c.c_custkey = o.o_custkey
#                     INNER JOIN nation as n1 ON n1.n_nationkey = c.c_nationkey 
#                     INNER JOIN region as r ON r.r_regionkey = n1.n_regionkey
#                     INNER JOIN nation as n2 ON n2.n_nationkey = s.s_nationkey
#                     where 
#                         r.r_name = 'AMERICA' 
#                         and o.o_orderdate >= date '1995-01-01' and o.o_orderdate <= date '1996-12-31'
#                         and p.p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
#                 group by
#                     o_year
#                 order by
#                     o_year"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_09'
#             query = """ select
#                     nationl, o_year, sum(amount) as sum_profit
#                 from
#                     ( select n_name as nationl, extract(year from o_orderdate) as o_year,
#                             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
#                         from lineitem
#                         INNER JOIN orders ON o_orderkey = l_orderkey
#                         INNER JOIN partsupp ON ps_suppkey = l_suppkey and ps_partkey = l_partkey
#                         INNER JOIN part ON p_partkey = l_partkey
#                         INNER JOIN supplier ON s_suppkey = l_suppkey
#                         INNER JOIN nation ON n_nationkey = s_nationkey                     
#                         where
#                             p_name like '%green%' ) as profit
#                 group by 
#                     nationl, o_year 
#                 order by 
#                     nationl, o_year desc"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_10'
#             query = """ select
#                     c.c_custkey, c.c_name, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue, c.c_acctbal,
#                     n.n_name, c.c_address, c.c_phone, c.c_comment
#                 from
#                     customer as c
#                 INNER JOIN nation as n ON n.n_nationkey = c.c_nationkey
#                 INNER JOIN orders as o ON o.o_custkey = c.c_custkey
#                 INNER JOIN lineitem as l ON l.l_orderkey = o.o_orderkey
#                 where 
#                 o.o_orderdate >= date '1993-10-01'
#                 and o.o_orderdate < date '1994-10-01'
#                 and l.l_returnflag = 'R'
#                 group by
#                     c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
#             #falta cambiar
#             queryId = 'TEST_11'
#             query = """ select
#                             ps.ps_partkey, sum(ps.ps_supplycost * ps.ps_availqty) as valuep
#                         from 
#                             partsupp as ps
#                         INNER JOIN supplier as s ON ps.ps_suppkey = s.s_suppkey
#                         INNER JOIN nation as n ON s.s_nationkey = n.n_nationkey
#                         where
#                             n.n_name = 'GERMANY'
#                         group by 
#                             ps.ps_partkey having sum(ps.ps_supplycost * ps.ps_availqty) > ( select
#                                                                         sum(psq.ps_supplycost)
#                                                                         from
#                                                                             partsupp as psq
#                                                                         INNER JOIN supplier as sq ON psq.ps_suppkey = sq.s_suppkey
#                                                                         INNER JOIN nation as nq ON sq.s_nationkey = nq.n_nationkey
#                                                                         where
#                                                                             nq.n_name = 'GERMANY'
#                                                                         )
#                         order by 
#                             valuep desc"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.01, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_12'
#             query = """ select
#                     l.l_shipmode, sum(case when o.o_orderpriority ='1-URGENT' or o.o_orderpriority ='2-HIGH'
#                     then 1 else 0 end) as high_line_count, sum(case when o.o_orderpriority <> '1-URGENT'
#                     and o.o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
#                 from
#                     lineitem as l
#                 INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey
#                 where
#                     l.l_shipmode in ('MAIL', 'SHIP') 
#                     and l.l_commitdate < l.l_receiptdate
#                     and l.l_shipdate < l.l_commitdate 
#                     and l.l_receiptdate >= date '1994-01-01'
#                     and l.l_receiptdate < date '1995-01-01'
#                 group by l.l_shipmode
#                 order by l.l_shipmode"""

#             if fileSchemaType != DataType.ORC: # TODO CRASH percy kharoly c.cordova rommel we should fix this
#                 runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
        
#             queryId = 'TEST_13'
#             query = """ select
#                     c_count, count(*) as custdist
#                 from
#                     (select
#                         c.c_custkey, count(o.o_orderkey)
#                     from
#                         customer as c
#                     LEFT OUTER JOIN orders as o ON c.c_custkey = o.o_custkey
#                     where o.o_comment not like '%special%requests%'
#                     group by
#                         c.c_custkey) as c_orders (c_custkey, c_count)
#                 group by
#                     c_count
#                 order by
#                     custdist desc, c_count desc"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_14'
#             query = """ select 100.00 * sum( case when p.p_type like 'PROMO%' then l.l_extendedprice * (1 - l.l_discount)
#                                             else 0 end) / sum(l.l_extendedprice * (1 - l.l_discount) ) as promo_revenue
#                         from 
#                             lineitem as l
#                         INNER JOIN part as p ON p.p_partkey = l.l_partkey
#                         where
#                             l.l_shipdate >= date '1995-09-01' 
#                             and l.l_shipdate < date '1995-10-01'"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
#             queryId = 'TEST_15'
#             query = """with revenue (suplier_no, total_revenue) as
#                     (
#                     select
#                     l_suppkey, cast(sum(l_extendedprice * (1-l_discount)) as int)
#                     from
#                     lineitem
#                     where
#                     l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'
#                     group by
#                     l_suppkey
#                     )
#                     select
#                     s.s_suppkey, s.s_name, s.s_address, s.s_phone, re.total_revenue
#                     from
#                     supplier as s
#                     INNER JOIN revenue as re on s.s_suppkey = re.suplier_no
#                     where
#                     re.total_revenue = cast((select max(total_revenue) from revenue) as int)
#                     order by
#                     s.s_suppkey"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
#             #falta cambiar
#             queryId = 'TEST_16'
#             query = """ select
#                             p.p_brand, p.p_type, p.p_size, count(distinct ps.ps_suppkey) as supplier_cnt
#                         from partsupp ps
#                         inner join part p on p.p_partkey = ps.ps_partkey
#                         where
#                             p.p_brand <> 'Brand#45'
#                             and p.p_type not like 'MEDIUM POLISHED%' and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
#                             and ps.ps_suppkey not in (select s.s_suppkey from supplier s where s.s_comment like '%Customer%Complaints%')
#                         group by
#                             p.p_brand, p.p_type, p.p_size
#                         order by
#                             supplier_cnt desc, p.p_brand, p.p_type, p.p_size"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
#             #falta cambiar
#             queryId = 'TEST_17'
#             query = """ select
#                             sum(l1.l_extendedprice) / 7.0 as avg_yearly
#                         from lineitem l1
#                         inner join part p on p.p_partkey = l1.l_partkey
#                         where
#                             p.p_brand = 'Brand#23' and p.p_container = 'MED BOX'
#                             and l1.l_quantity < (select 0.2 * avg(l2.l_quantity) from lineitem l2
#                                             where l2.l_partkey = p.p_partkey)"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
#             queryId = 'TEST_18'
#             query = """ select
#                     c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity)
#                 from
#                     customer c
#                     inner join orders o on c.c_custkey = o.o_custkey
#                     inner join lineitem l on o.o_orderkey = l.l_orderkey
#                 where
#                     o.o_orderkey in (select l2.l_orderkey from lineitem l2 group by l2.l_orderkey having
#                                     sum(l2.l_quantity) > 300)
#                 group by
#                     c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice
#                 order by
#                     o.o_totalprice desc, o.o_orderdate"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
#             #falta cambiar
#             queryId = 'TEST_19'
#             query = """ select 
#                             sum(l.l_extendedprice * (1 - l.l_discount) ) as revenue
#                         from 
#                             lineitem as l
#                         INNER JOIN part as p ON l.l_partkey = p.p_partkey
#                         where
#                             (
#                             p.p_brand = 'Brand#12'
#                             and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
#                             and l.l_quantity >= 1 and l.l_quantity <= 11
#                             and p.p_size between 1 and 5
#                             and l.l_shipmode in ('AIR', 'AIR REG')
#                             and l.l_shipinstruct = 'DELIVER IN PERSON'
#                             )
#                             or
#                             (
#                             p.p_brand = 'Brand#23'
#                             and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
#                             and l.l_quantity >= 10 and l.l_quantity <= 20
#                             and p.p_size between 1 and 10
#                             and l.l_shipmode in ('AIR', 'AIR REG')
#                             and l.l_shipinstruct = 'DELIVER IN PERSON'
#                             )
#                             or
#                             (
#                             p.p_brand = 'Brand#34'
#                             and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
#                             and l.l_quantity >= 20 and l.l_quantity <= 30
#                             and p.p_size between 1 and 15
#                             and l.l_shipmode in ('AIR', 'AIR REG')
#                             and l.l_shipinstruct = 'DELIVER IN PERSON'
#                             )"""
#             runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)        
#             #falta cambiar  
#             queryId = 'TEST_20'
#             query = """ select s.s_name, s.s_address
#                         from supplier s
#                         inner join nation n on s.s_nationkey = n.n_nationkey
#                         where
#                             s.s_suppkey in (select ps.ps_suppkey from partsupp ps where ps.ps_partkey in (select p.p_partkey
#                                                                                                 from part p where
#                                                                                             p.p_name like 'forest%')
#                                         and ps_availqty > (select 0.5 * sum(l.l_quantity) from lineitem l where
#                                                             l.l_partkey = ps.ps_partkey and l.l_suppkey = ps.ps_suppkey
#                                                             and l.l_shipdate >= date '1994-01-01' 
#                                                             and l.l_shipdate < date '1994-01-01' - interval '1' year))
#                             and n.n_name = 'CANADA'
#                         order by s.s_name"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
#             #falta cambiar  
#             queryId = 'TEST_21'
#             query = """ select
#                             s_name, count(*) as numwait
#                         from
#                             supplier
#                             inner join lineitem l1 on s_suppkey = l1.l_suppkey
#                             inner join orders on o_orderkey = l1.l_orderkey
#                             inner join nation on s_nationkey = n_nationkey
#                         where
#                             o_orderstatus = 'F'
#                             and l1.l_receiptdate > l1.l_commitdate and exists (select * from lineitem l2
#                                                                                 where l2.l_orderkey = l1.l_orderkey
#                                                                                     and l2.l_suppkey <> l1.l_suppkey)
#                             and not exists (select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey
#                                             and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate)
#                             and n_name = ' SAUDI ARABIA'
#                         group by
#                             s_name
#                         order by
#                             numwait desc, s_name"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
#             #falta cambiar  
#             queryId = 'TEST_22'
#             query = """ select
#                             cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
#                         from (select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer
#                             where substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17')
#                             and c_acctbal > (select avg(c_acctbal) from customer where c_acctbal > 0.00
#                             and substring (c_phone from 1 for 2) in ('13','31','23','29','30','18','17'))
#                             and not exists (select * from orders where o_custkey = c_custkey)) as custsale
#                         group by cntrycode
#                         order by cntrycode"""
#             #runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

#     executionTest(queryType)
    
#     end_mem = gpuMemory.capture_gpu_memory_usage()

#     gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
    
# if __name__ == '__main__':

#     Execution.getArgs()
    
#     nvmlInit()

#     compare_results = True
#     if 'compare_results' in Settings.data['RunSettings']:
#         compare_results = Settings.data['RunSettings']['compare_results'] 

#     drill = None

#     if compare_results:
#         # Create Table Drill ------------------------------------------------------------------------------------------------------
#         from pydrill.client import PyDrill
#         drill = PyDrill(host = 'localhost', port = 8047)
#         cs.init_drill_schema(drill, Settings.data['TestSettings']['dataDirectory'])

#     #Create Context For BlazingSQL
#     bc, dask_client = init_context()
    
#     nRals = Settings.data['RunSettings']['nRals']

#     main(dask_client, drill, Settings.data['TestSettings']['dataDirectory'], bc, nRals)
    
#     runTest.save_log()
    
#     gpuMemory.print_log_gpu_memory()
    
