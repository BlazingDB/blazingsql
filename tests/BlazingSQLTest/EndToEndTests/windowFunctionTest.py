from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Window Function"


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

            # TODO: RANK() and DENSE_RANK(): cudf aggs no supported currently

            # TODO: BOUNDED/UNBOUNDED, ROW/RANGE: Calcite issue when get optimized plan

            # ------------------- ORDER BY ------------------------
            
            # queryId = "TEST_01"
            # query = """select min(n_nationkey) over 
            #                 (
            #                     order by n_regionkey
            #                 ) min_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation order by n_name"""
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
            # query = """select min(n_nationkey) over 
            #                 (
            #                     order by n_regionkey,
            #                     n_name desc
            #                 ) min_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation order by n_name"""
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
            # query = """select max(n_nationkey) over 
            #                 (
            #                     order by n_regionkey,
            #                     n_name desc
            #                 ) max_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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
            # query = """select count(n_nationkey) over 
            #                 (
            #                     order by n_regionkey,
            #                     n_name
            #                 ) count_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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
            # query = """select row_number() over 
            #                 (
            #                     order by n_regionkey desc,
            #                     n_name
            #                 ) row_num,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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
            # query = """select sum(n_nationkey) over 
            #                 (
            #                     order by n_nationkey desc
            #                 ) sum_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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
            # query = """select avg(cast(n_nationkey as double)) over 
            #                 (
            #                     order by n_regionkey,
            #                     n_name desc
            #                 ) avg_keys,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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
            # query = """select first_value(n_nationkey) over
            #                 (
            #                     order by n_regionkey desc,
            #                     n_name
            #                 ) first_val,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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

            # queryId = "TEST_09"
            # query = """select last_value(n_nationkey) over
            #                 (
            #                     order by n_regionkey desc,
            #                     n_name
            #                 ) last_val,
            #                 n_nationkey, n_name, n_regionkey
            #             from nation"""
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

            # ----------------- PARTITION BY ----------------------

            queryId = "TEST_11"
            query = """select min(n_nationkey) over
                            (
                                partition by n_regionkey
                            ) min_keys,
                            n_nationkey, n_name, n_regionkey
                        from nation order by n_name"""
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
            query = """select max(o_orderkey) over
                            (
                                partition by o_custkey
                            ) max_keys,
                            o_custkey, o_orderstatus, o_totalprice
                        from orders
                        where o_orderstatus = 'O'
                        and o_totalprice > 98000.5
                        order by o_orderkey, o_totalprice
                        limit 1200"""
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
            query = """select count(l_orderkey) over
                            (
                                partition by l_partkey
                            ) count_keys,
                            l_linenumber, l_extendedprice, l_tax
                        from lineitem
                        where l_partkey < 250
                        and l_linenumber > 4
                        order by l_orderkey, l_partkey"""
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
            query = """select sum(o_custkey) over 
                            (
                                partition by o_orderpriority
                            ) sum_keys,
                            o_clerk,
                            cast(o_shippriority as double) as o_ship_double
                        from orders
                        where o_orderstatus <> 'O'
                        and o_totalprice > 35000
                        and o_orderpriority = '3-MEDIUM'
                        order by o_orderpriority"""
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
            query = """select avg(cast(n.n_nationkey as double)) over 
                            (
                                partition by n.n_regionkey
                            ) avg_keys,
                            n.n_nationkey, n.n_name, n.n_regionkey, l.l_comment
                        from nation as n
                        inner join lineitem as l
                        on n.n_nationkey = l.l_orderkey 
                        order by n.n_nationkey, avg_keys"""
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

            queryId = "TEST_16"
            query = """select sum(o_custkey) over 
                            (
                                partition by o_orderpriority, o_orderstatus
                            ) sum_keys,
                            o_clerk,
                            cast(o_shippriority as double) as o_ship_double,
                            o_orderpriority
                        from orders
                        where o_orderstatus = 'O'
                        and o_totalprice < 4550
                        and o_orderpriority = '1-URGENT'
                        order by o_orderpriority"""
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
            query = """select min(c_custkey) over
                            (
                                partition by c_mktsegment, c_name, c_custkey
                            ) min_keys,
                            c_custkey, c_mktsegment
                        from customer
                        where c_acctbal > 4525.0
                        and c_mktsegment not in ('AUTOMOBILE', 'HOUSEHOLD')
                        order by c_custkey, c_nationkey
                        limit 250"""
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
            query = """select avg(cast(n.n_nationkey as double)) over 
                            (
                                partition by n.n_regionkey, n.n_name, n.n_nationkey
                            ) avg_keys,
                            n.n_nationkey, n.n_name, n.n_regionkey, l.l_comment
                        from nation as n
                        inner join lineitem as l
                        on n.n_nationkey = l.l_orderkey 
                        order by n.n_nationkey, avg_keys"""
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
            query = """with first_window_order as (
                            select max(o_totalprice) over
                                (
                                    partition by o_orderpriority
                                    order by o_totalprice, o_custkey
                                ) o_max_prices,
                                min(o_totalprice) over
                                (
                                    partition by o_orderpriority
                                    order by o_totalprice, o_custkey
                                ) o_min_prices,
                                o_orderkey, o_orderpriority, o_custkey,
                                o_totalprice, o_clerk
                            from orders
                        ), order_operated as (
                            select * from first_window_order
                            where o_max_prices < 19750.0
                            and o_clerk <> 'Clerk#000000880'
                            and o_orderpriority in ('2-HIGH', '5-LOW')
                            order by o_orderkey, o_custkey, o_totalprice
                            limit 1250
                        )
                        select sum(o_max_prices) over
                                (
                                    partition by o_orderpriority
                                    order by o_totalprice, o_custkey
                                ) sum_max_prices,
                               o_orderkey, o_min_prices, o_orderpriority from order_operated
                        order by o_orderkey, o_min_prices, o_totalprice
                        limit 450"""
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
            query = """with reduced_order as (
                            select o_orderkey, o_custkey, o_orderpriority,
                                o_orderstatus, o_totalprice, o_clerk
                            from orders
                            where o_totalprice < 1750.0
                            and o_clerk <> 'Clerk#000000880'
                            order by o_orderkey, o_custkey, o_totalprice
                            limit 3500
                        ), window_orders as (
                            select min(o_totalprice) over
                                (
                                    partition by o_orderpriority
                                    order by o_totalprice
                                ) o_min_prices,
                                o_orderkey, o_orderpriority, o_orderstatus
                            from reduced_order
                        )
                        select o_orderkey, o_min_prices, o_orderpriority
                        from window_orders
                        where o_orderstatus in ('O', 'F')
                        and o_orderpriority = '2-HIGH'
                        order by o_orderkey, o_min_prices"""
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

            # ------------ PARTITION BY + ORDER BY ----------------

            queryId = "TEST_21"
            query = """select min(c_custkey) over
                            (
                                partition by c_nationkey
                                order by c_name
                            ) min_keys,
                            c_custkey, c_mktsegment
                        from customer
                        where c_acctbal > 2525.0
                        and c_mktsegment not in ('AUTOMOBILE', 'HOUSEHOLD')
                        order by c_custkey, c_nationkey"""
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
            query = """select max(l_partkey) over
                            (
                                partition by l_linestatus
                                order by l_quantity desc, l_orderkey
                            ) max_keys,
                            l_linestatus, l_extendedprice
                        from lineitem
                        where l_shipmode not in ('MAIL', 'SHIP', 'AIR')
                        and l_linestatus = 'F'
                        order by l_orderkey, max_keys
                        limit 50"""
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

            queryId = "TEST_23"
            query = """select count(o_orderkey) over
                            (
                                partition by o_orderstatus, o_orderpriority
                                order by o_orderkey, o_clerk
                            ) count_keys,
                            o_totalprice
                        from orders
                        where o_totalprice < 1352.0
                        order by o_custkey, o_orderpriority, o_orderkey
                        limit 50"""
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
            query = """select sum(n.n_nationkey) over 
                            (
                                partition by n.n_regionkey
                                order by n.n_nationkey desc, n.n_name desc
                            ) sum_keys,
                            n.n_nationkey, c.c_address, c.c_custkey
                        from nation n
                        inner join customer c on n.n_nationkey = c.c_custkey
                        where c.c_mktsegment <> 'household'
                        and n.n_nationkey in (1, 2, 3, 8, 9, 12)"""
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
                                partition by nn.n_region1
                                order by nn.n_nam1
                            ) avg_keys,
                            nn.n_natio1, nn.n_nam1, nn.n_region1
                        from new_nation nn
                        order by nn.n_natio1, avg_keys"""
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
            query = """select row_number() over 
                            (
                                partition by c_nationkey
                                order by c_custkey desc
                            ) row_num,
                            c_phone, UPPER(SUBSTRING(c_name, 1, 8))
                        from customer
                        where c_acctbal < 95.0
                        order by c_custkey, row_num"""
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
            query = """select row_number() over 
                            (
                                partition by c_nationkey, c_mktsegment
                                order by c_custkey desc, c_name
                            ) row_num,
                            c_phone, UPPER(SUBSTRING(c_name, 1, 8))
                        from customer
                        where c_acctbal < 155.0
                        order by c_custkey, row_num desc"""
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
            query = """select lag(l_partkey, 2) over 
                            (
                                partition by l_linestatus
                                order by l_orderkey, l_quantity desc
                            ) lag_keys, 
                            l_linestatus, l_partkey, l_extendedprice
                        from lineitem
                        where l_partkey < 750
                        and l_linenumber >= 6
                        order by l_extendedprice, l_partkey, lag_keys
                        limit 30"""
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

            queryId = "TEST_29"
            query = """select lead(l_partkey, 3) over 
                            (
                                partition by l_linestatus
                                order by l_extendedprice
                            ) lead_keys, 
                            l_linestatus, l_partkey, l_extendedprice
                        from lineitem
                        where l_partkey < 950
                        and l_linenumber >= 7
                        order by l_extendedprice, l_partkey
                        limit 40"""
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

            queryId = "TEST_30"
            query = """select (o_orderkey + o_custkey) as key_priority,
                            max(o_totalprice) over
                            (
                                partition by o_orderpriority
                                order by o_totalprice, o_custkey
                            ) o_max_prices,
                            o_custkey + o_totalprice, 
                            min(o_totalprice) over
                            (
                                partition by o_orderpriority
                                order by o_totalprice, o_custkey
                            ) o_min_prices,
                            o_custkey - o_totalprice + 5
                        from orders
                        where o_orderstatus not in ('O', 'F')
                        and o_totalprice < 85000
                        and o_orderpriority <> '2-HIGH'
                        order by key_priority, o_max_prices"""
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

            # ---------- multiple WF with the same OVER clause ------------

            queryId = "TEST_31"
            query = """select min(o_orderkey) over
                            (
                                partition by o_orderstatus
                                order by o_orderdate
                            ) min_keys, 
                            max(o_orderkey) over
                            (
                                partition by o_orderstatus
                                order by o_orderdate
                            ) max_keys, o_orderkey, o_orderpriority
                        from orders
                        where o_orderpriority <> '2-HIGH'
                        and o_clerk = 'Clerk#000000880'
                        order by o_orderkey"""
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
            query = """select min(n_nationkey) over
                            (
                                partition by n_regionkey
                                order by n_name
                            ) min_keys, 
                            max(n_nationkey) over
                            (
                                partition by n_regionkey
                                order by n_name
                            ) max_keys,
                            count(n_nationkey) over
                            (
                                partition by n_regionkey
                                order by n_name
                            ) count_keys, n_nationkey, n_name, n_regionkey
                        from nation order by n_nationkey"""
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

            queryId = "TEST_33"
            query = """select min(o_orderkey) over
                            (
                                partition by o_orderstatus, o_clerk
                                order by o_orderdate
                            ) min_keys, 
                            max(o_orderkey) over
                            (
                                partition by o_orderstatus, o_clerk
                                order by o_orderdate
                            ) max_keys, o_orderkey, o_orderpriority
                        from orders
                        where o_orderpriority <> '2-HIGH'
                        and o_clerk = 'Clerk#000000880'
                        order by o_orderkey"""
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

            # NOTE: order by in the over clause is mandatory for spark
            queryId = "TEST_34"
            query = """select min(n_nationkey) over
                            (
                                partition by n_regionkey order by n_name
                            ) min_keys,
                            lag(n_nationkey, 2) over
                            (
                                partition by n_regionkey order by n_name
                            ) lag_col,
                            max(n_nationkey) over
                            (
                                partition by n_regionkey order by n_name
                            ) max_keys,
                            n_nationkey, n_name, n_regionkey
                            from nation order by n_nationkey"""
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

            queryId = "TEST_35"
            query = """select sum(o_custkey) over 
                            (
                                partition by o_orderstatus, o_orderpriority
                                order by o_orderkey
                            ) sum_keys,
                            lag(o_custkey, 2) over 
                            (
                                partition by o_orderstatus, o_orderpriority
                                order by o_orderkey
                            ) lag_keys,
                            cast(o_shippriority as double) as o_ship_double,
                            o_orderpriority
                        from orders
                        where o_orderstatus <> 'O'
                        and o_totalprice <= 6000
                        and o_orderpriority in ('2-HIGH', '1-URGENT')
                        order by o_orderpriority"""
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

            queryId = "TEST_36"
            query = """select sum(o_custkey) over 
                            (
                                partition by o_orderstatus, o_orderpriority
                                order by o_orderkey
                            ) sum_keys,
                            lead(o_custkey, 3) over 
                            (
                                partition by o_orderstatus, o_orderpriority
                                order by o_orderkey
                            ) lead_keys,
                            cast(o_shippriority as double) as o_ship_double,
                            o_orderpriority
                        from orders
                        where o_orderstatus <> 'O'
                        and o_totalprice <= 6000
                        and o_orderpriority in ('2-HIGH', '1-URGENT')
                        order by o_orderpriority"""
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

            queryId = "TEST_37"
            query = """select 
                            max(n.n_nationkey) over 
                            (
                                partition by l.l_partkey
                                order by l.l_extendedprice
                            ) max_keys,
                            lead(n.n_nationkey, 2) over 
                            (
                                partition by l.l_partkey
                                order by l.l_extendedprice
                            ) lead_keys,
                            n.n_nationkey, l.l_extendedprice, l.l_comment
                        from nation as n
                        inner join lineitem as l
                        on n.n_nationkey = l.l_partkey 
                        order by l.l_extendedprice, l_comment
                        limit 10"""
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

            # ------------ ROWS bounding ----------------

            queryId = "TEST_50"
            query = """select min(n_nationkey) over
                            (
                                partition by n_regionkey
                                order by n_name
                                ROWS BETWEEN 1 PRECEDING
                                AND 1 FOLLOWING
                            ) min_val,
 							n_nationkey, n_regionkey, n_name
                        from nation order by n_nationkey"""
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

            queryId = "TEST_51"
            query = """select min(o_orderkey) over
                            (
                                partition by o_orderstatus, o_clerk
                                order by o_orderdate
                                ROWS BETWEEN 2 PRECEDING
                                AND 1 FOLLOWING
                            ) min_keys, 
                            max(o_orderkey) over
                            (
                                partition by o_orderstatus, o_clerk
                                order by o_orderdate
                                ROWS BETWEEN 2 PRECEDING
                                AND 1 FOLLOWING
                            ) max_keys, o_orderkey, o_orderpriority
                        from orders
                        where o_orderpriority <> '2-HIGH'
                        and o_clerk = 'Clerk#000000880'
                        order by o_orderkey
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

            queryId = "TEST_52"
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
                        partition by nn.n_region1
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
                fileSchemaType,
            )

            queryId = "TEST_53"
            query = """select max(l_partkey) over
                            (
                                partition by l_linestatus
                                order by l_quantity desc, l_orderkey
                                ROWS BETWEEN 6 PRECEDING
                                AND 2 FOLLOWING
                            ) max_keys,
                            l_linestatus, l_extendedprice
                        from lineitem
                        where l_shipmode not in ('MAIL', 'SHIP', 'AIR')
                        and l_linestatus = 'F'
                        order by l_orderkey, max_keys
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

            # TODO: FIRST_VALUE() and LAST_VALUE(): cordova add new tests here

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
