from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test, utilityHive
import shutil
import pandas
import tempfile

queryType = "Hive File"

tables = ["orders", "customer", "lineitem", "nation", "part", "supplier"]
data_types = [DataType.CSV, DataType.PARQUET]

def executionTestAuto(dask_client, spark, dir_data_file, bc, nRals):

    # Create Tables -----------------------------------------------------
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue

        location =  tmpPath + cs.get_extension(fileSchemaType) + '/'

        # Create tables for hive
        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.AUTO,
                                         partitions={},
                                         partitions_schema=[],
                                         tables=tables)

        # Run Query ------------------------------------------------------
        # Parameter to indicate if its necessary to order
        # the resulsets before compare them
        worder = 1
        use_percentage = False
        acceptable_difference = 0.01

        print("==============================")
        print(queryType)
        print("==============================")

        # https://github.com/BlazingDB/blazingsql/issues/1541
        queryId = "TEST_01"
        query = "select o_totalprice from orders where o_orderstatus = 'F' and o_orderdate <= '1992-01-31' and o_orderpriority IS NOT NULL and o_orderstatus IS NOT NULL and o_orderdate IS NOT NULL order by o_orderkey"
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
        #     query_spark=query,
        # )

        queryId = "TEST_02"
        query = """select c_nationkey, c_acctbal + 3 as c_acctbal_new
                    from customer where c_acctbal > 1000 and c_mktsegment = 'AUTOMOBILE' 
                    and c_nationkey IS NOT NULL and c_mktsegment IS NOT NULL"""
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
            query_spark=query,
        )

        queryId = "TEST_03"
        query = """ select l.l_orderkey, l.l_linenumber from lineitem as l
                    inner join orders as o on l.l_orderkey = o.o_orderkey
                    and l.l_commitdate < o.o_orderdate
                    and l.l_receiptdate > o.o_orderdate
                    where o.o_orderdate <= '1992-01-31'
                    and o.o_orderpriority IS NOT NULL 
                    and o.o_orderstatus IS NOT NULL 
                    and o.o_orderdate IS NOT NULL"""
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
            query_spark=query,
        )

        queryId = "TEST_04"
        query = """ select l_linenumber, l_shipmode, l_returnflag 
                    from lineitem 
                    where l_shipmode in ('AIR','FOB','MAIL','RAIL') and l_returnflag in ('A', 'N')
                    and l_shipmode IS NOT NULL
                    and l_linestatus IS NOT NULL
                    and l_returnflag IS NOT NULL
                    order by l_orderkey"""
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
            query_spark=query,
        )

        # "nation", "part", "supplier"]

        queryId = "TEST_05"
        query = """select n_regionkey as rkey, n_nationkey from nation
                    where n_regionkey < 3 and n_nationkey > 5"""
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
            query_spark=query,
        )

        if int(nRals) == 1:
            queryId = "TEST_06"
            query = """select p.p_partkey, p.p_mfgr
                        from part p
                        where p.p_type like '%STEEL'
                        and p.p_container IS NOT NULL"""
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
                query_spark=query,
            )

            queryId = "TEST_07"
            query = """select p_partkey, p_mfgr, p_container
                        from part
                        where p_size = 35 and p_container like 'WRAP%'
                        and p_container IS NOT NULL"""
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
                query_spark=query,
            )

            queryId = "TEST_08"
            query = """select count(s_suppkey), count(s_nationkey)
                        from supplier
                        where s_nationkey IS NOT NULL"""
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
                query_spark=query,
            )

        queryId = "TEST_09"
        query = """select n1.n_nationkey as supp_nation,
                    n2.n_nationkey as cust_nation,
                    l.l_extendedprice * l.l_discount
                    from supplier as s
                    inner join lineitem as l on s.s_suppkey = l.l_suppkey
                    inner join orders as o on o.o_orderkey = l.l_orderkey
                    inner join customer as c on c.c_custkey = o.o_custkey
                    inner join nation as n1 on s.s_nationkey = n1.n_nationkey
                    inner join nation as n2 on c.c_nationkey = n2.n_nationkey
                    where n1.n_nationkey = 1
                    and n2.n_nationkey = 2
                    and o.o_orderkey < 20
                    and o.o_orderdate <= '1992-01-31'
                    and o.o_orderpriority IS NOT NULL 
                    and o.o_orderstatus IS NOT NULL 
                    and o.o_orderdate IS NOT NULL
                    and c.c_nationkey IS NOT NULL
                    and c.c_mktsegment IS NOT NULL"""
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
            query_spark=query,
        )

        queryId = "TEST_10"
        query = """ select s_name, s_address, s_nationkey
                    from supplier
                    where s_nationkey < 10
                    order by s_suppkey"""
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
            query_spark=query,
        )

        if Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break

def executionTestWithPartitions(dask_client, spark, dir_data_file, bc, nRals):
    # Create Tables -----------------------------------------------------
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue

        location =  tmpPath + cs.get_extension(fileSchemaType) + '/'

        # Create tables with partitions
        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM',
                                                                 '4-NOT SPECIFIED',
                                                                 '5-LOW'],
                                             'o_orderstatus': ['F', 'O', 'P'],
                                             'o_orderdate': [pandas.Timestamp('1992-01-01 00:00:00'),
                                                             pandas.Timestamp('1992-01-02 00:00:00'),
                                                             pandas.Timestamp('1992-01-03 00:00:00'),
                                                             pandas.Timestamp('1992-01-04 00:00:00'),
                                                             pandas.Timestamp('1992-01-05 00:00:00'),
                                                             pandas.Timestamp('1992-01-06 00:00:00'),
                                                             pandas.Timestamp('1992-01-07 00:00:00'),
                                                             pandas.Timestamp('1992-01-08 00:00:00'),
                                                             pandas.Timestamp('1992-01-09 00:00:00'),
                                                             pandas.Timestamp('1992-01-10 00:00:00'),
                                                             pandas.Timestamp('1992-01-11 00:00:00'),
                                                             pandas.Timestamp('1992-01-12 00:00:00'),
                                                             pandas.Timestamp('1992-01-13 00:00:00'),
                                                             pandas.Timestamp('1992-01-14 00:00:00'),
                                                             pandas.Timestamp('1992-01-15 00:00:00'),
                                                             pandas.Timestamp('1992-01-16 00:00:00'),
                                                             pandas.Timestamp('1992-01-17 00:00:00'),
                                                             pandas.Timestamp('1992-01-18 00:00:00'),
                                                             pandas.Timestamp('1992-01-19 00:00:00'),
                                                             pandas.Timestamp('1992-01-20 00:00:00'),
                                                             pandas.Timestamp('1992-01-21 00:00:00'),
                                                             pandas.Timestamp('1992-01-22 00:00:00'),
                                                             pandas.Timestamp('1992-01-23 00:00:00'),
                                                             pandas.Timestamp('1992-01-24 00:00:00'),
                                                             pandas.Timestamp('1992-01-25 00:00:00'),
                                                             pandas.Timestamp('1992-01-26 00:00:00'),
                                                             pandas.Timestamp('1992-01-27 00:00:00'),
                                                             pandas.Timestamp('1992-01-28 00:00:00'),
                                                             pandas.Timestamp('1992-01-29 00:00:00'),
                                                             pandas.Timestamp('1992-01-30 00:00:00'),
                                                             pandas.Timestamp('1992-01-31 00:00:00')]},
                                         partitions_schema=[('o_orderpriority', 'str'),
                                                            ('o_orderstatus', 'str'),
                                                            ('o_orderdate', 'timestamp')],
                                         tables=['orders'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'c_nationkey': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                                                             16, 17, 18, 19, 20, 21, 22, 23, 24],
                                             'c_mktsegment': ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD',
                                                              'MACHINERY']},
                                         partitions_schema=[('c_nationkey', 'int32'),
                                                            ('c_mktsegment', 'str')],
                                         tables=['customer'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'l_shipmode': ['AIR', 'FOB', 'MAIL', 'RAIL', 'REG', 'AIR', 'SHIP',
                                                            'TRUCK'],
                                             'l_linestatus': ['F', 'O'],
                                             'l_returnflag': ['A', 'N', 'R']
                                         },
                                         partitions_schema=[('l_shipmode', 'str'),
                                                            ('l_linestatus', 'str'),
                                                            ('l_returnflag', 'str')],
                                         tables=['lineitem'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'n_regionkey': [0, 1, 2, 3, 4]},
                                         partitions_schema=[('n_regionkey', 'int32')],
                                         tables=['nation'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'p_container': ['JUMBO BAG', 'JUMBO BOX', 'JUMBO CAN', 'JUMBO CASE',
                                                             'JUMBO DRUM', 'JUMBO JAR', 'JUMBO PACK', 'JUMBO PKG',
                                                             'LG BAG', 'LG BOX', 'LG CAN', 'LG CASE', 'LG DRUM',
                                                             'LG JAR', 'LG PACK', 'LG PKG', 'MED BAG', 'MED BOX',
                                                             'MED CAN', 'MED CASE', 'MED DRUM', 'MED JAR',
                                                             'MED PACK',
                                                             'MED PKG', 'SM BAG', 'SM BOX', 'SM CAN', 'SM CASE',
                                                             'SM DRUM', 'SM JAR', 'SM PACK', 'SM PKG', 'WRAP BAG',
                                                             'WRAP BOX', 'WRAP CAN', 'WRAP CASE', 'WRAP DRUM',
                                                             'WRAP JAR', 'WRAP PACK', 'WRAP PKG']},
                                         partitions_schema=[('p_container', 'str')],
                                         tables=['part'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             's_nationkey': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                                                             16, 17, 18, 19, 20, 21, 22, 23, 24]},
                                         partitions_schema=[('s_nationkey', 'int32')],
                                         tables=['supplier'])

        # Run Query ------------------------------------------------------
        # Parameter to indicate if its necessary to order
        # the resulsets before compare them
        worder = 1
        use_percentage = False
        acceptable_difference = 0.01

        print("==============================")
        print(queryType)
        print("==============================")

        queryId = "TEST_01"
        query = "select o_totalprice from orders where o_orderstatus = 'F' and o_orderdate <= '1992-01-31' and o_orderpriority IS NOT NULL and o_orderstatus IS NOT NULL order by o_orderkey"
        # TODO: Fernando Cordova, gpuci randomly crashes with CSV
        if fileSchemaType != DataType.CSV:
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
                query_spark=query,
            )

        queryId = "TEST_02"
        query = """select c_nationkey, c_acctbal + 3 as c_acctbal_new
                            from customer where c_acctbal > 1000 and c_mktsegment = 'AUTOMOBILE' 
                            and c_nationkey IS NOT NULL and c_mktsegment IS NOT NULL"""
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
            query_spark=query,
        )

        queryId = "TEST_03"
        query = """ select l.l_orderkey, l.l_linenumber from lineitem as l
                            inner join orders as o on l.l_orderkey = o.o_orderkey
                            and l.l_commitdate < o.o_orderdate
                            and l.l_receiptdate > o.o_orderdate
                            where o.o_orderdate <= '1992-01-31'
                            and o.o_orderpriority IS NOT NULL 
                            and o.o_orderstatus IS NOT NULL 
                            and o.o_orderdate IS NOT NULL"""
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
            query_spark=query,
        )

        queryId = "TEST_04"
        query = """ select l_linenumber, l_shipmode, l_returnflag 
                            from lineitem 
                            where l_shipmode in ('AIR','FOB','MAIL','RAIL') and l_returnflag in ('A', 'N')
                            and l_shipmode IS NOT NULL
                            and l_linestatus IS NOT NULL
                            and l_returnflag IS NOT NULL
                            order by l_orderkey"""
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
            query_spark=query,
        )

        # "nation", "part", "supplier"]

        queryId = "TEST_05"
        query = """select n_regionkey as rkey, n_nationkey from nation
                            where n_regionkey < 3 and n_nationkey > 5"""
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
            query_spark=query,
        )

        if int(nRals) == 1:
            queryId = "TEST_06"
            query = """select p.p_partkey, p.p_mfgr
                                from part p
                                where p.p_type like '%STEEL'"""
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
                query_spark=query,
            )

            queryId = "TEST_07"
            query = """select p_partkey, p_mfgr, p_container
                                from part
                                where p_size = 35 and p_container like 'WRAP%'"""
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
                query_spark=query,
            )

            queryId = "TEST_08"
            query = """select count(s_suppkey), count(s_nationkey)
                                from supplier"""
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
                query_spark=query,
            )

        queryId = "TEST_09"
        query = """select n1.n_nationkey as supp_nation,
                            n2.n_nationkey as cust_nation,
                            l.l_extendedprice * l.l_discount
                            from supplier as s
                            inner join lineitem as l on s.s_suppkey = l.l_suppkey
                            inner join orders as o on o.o_orderkey = l.l_orderkey
                            inner join customer as c on c.c_custkey = o.o_custkey
                            inner join nation as n1 on s.s_nationkey = n1.n_nationkey
                            inner join nation as n2 on c.c_nationkey = n2.n_nationkey
                            where n1.n_nationkey = 1
                            and n2.n_nationkey = 2
                            and o.o_orderkey < 20
                            and o.o_orderdate <= '1992-01-31'
                            and o.o_orderpriority IS NOT NULL 
                            and o.o_orderstatus IS NOT NULL 
                            and o.o_orderdate IS NOT NULL
                            and c.c_nationkey IS NOT NULL
                            and c.c_mktsegment IS NOT NULL"""
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
            query_spark=query,
        )

        queryId = "TEST_10"
        query = """ select s_name, s_address, s_nationkey
                            from supplier
                            where s_nationkey < 10
                            order by s_suppkey"""
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
            query_spark=query,
        )

        if Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break

def executionTestWithSomePartitions(dask_client, spark, dir_data_file, bc, nRals):
    # Create Tables -----------------------------------------------------
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue

        location =  tmpPath + cs.get_extension(fileSchemaType) + '/'

        # Create tables with partitions
        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'o_orderpriority': ['1-URGENT', '3-MEDIUM',
                                                                 '4-NOT SPECIFIED',
                                                                 '5-LOW'],
                                             'o_orderstatus': ['F', 'O'],
                                             'o_orderdate': [pandas.Timestamp('1992-01-01 00:00:00'),
                                                             pandas.Timestamp('1992-01-02 00:00:00'),
                                                             pandas.Timestamp('1992-01-03 00:00:00'),
                                                             pandas.Timestamp('1992-01-04 00:00:00'),
                                                             pandas.Timestamp('1992-01-05 00:00:00'),
                                                             pandas.Timestamp('1992-01-06 00:00:00'),
                                                             pandas.Timestamp('1992-01-07 00:00:00'),
                                                             pandas.Timestamp('1992-01-08 00:00:00'),
                                                             pandas.Timestamp('1992-01-09 00:00:00'),
                                                             pandas.Timestamp('1992-01-10 00:00:00'),
                                                             pandas.Timestamp('1992-01-11 00:00:00'),
                                                             pandas.Timestamp('1992-01-12 00:00:00'),
                                                             pandas.Timestamp('1992-01-13 00:00:00'),
                                                             pandas.Timestamp('1992-01-14 00:00:00'),
                                                             pandas.Timestamp('1992-01-15 00:00:00')]},
                                         partitions_schema=[('o_orderpriority', 'str'),
                                                            ('o_orderstatus', 'str'),
                                                            ('o_orderdate', 'timestamp')],
                                         tables=['orders'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'c_nationkey': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                             'c_mktsegment': ['AUTOMOBILE', 'BUILDING', 'HOUSEHOLD',
                                                              'MACHINERY']},
                                         partitions_schema=[('c_nationkey', 'int32'),
                                                            ('c_mktsegment', 'str')],
                                         tables=['customer'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'l_shipmode': ['AIR', 'FOB', 'REG', 'AIR', 'SHIP', 'TRUCK'],
                                             'l_linestatus': ['F'],
                                             'l_returnflag': ['A', 'N', 'R']
                                         },
                                         partitions_schema=[('l_shipmode', 'str'),
                                                            ('l_linestatus', 'str'),
                                                            ('l_returnflag', 'str')],
                                         tables=['lineitem'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'n_regionkey': [2, 3, 4]},
                                         partitions_schema=[('n_regionkey', 'int32')],
                                         tables=['nation'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             'p_container': ['LG BAG', 'LG BOX', 'LG CAN', 'LG CASE', 'LG DRUM',
                                                             'LG JAR', 'LG PACK', 'LG PKG', 'MED BAG', 'MED BOX',
                                                             'MED CAN', 'MED CASE', 'MED DRUM', 'MED JAR',
                                                             'MED PACK',
                                                             'MED PKG', 'SM BAG', 'SM BOX', 'SM CAN', 'SM CASE',
                                                             'SM DRUM', 'SM JAR', 'SM PACK', 'SM PKG', 'WRAP BAG',
                                                             'WRAP BOX', 'WRAP CAN', 'WRAP CASE', 'WRAP DRUM',
                                                             'WRAP JAR', 'WRAP PACK', 'WRAP PKG']},
                                         partitions_schema=[('p_container', 'str')],
                                         tables=['part'])

        cs.create_hive_partitions_tables(bc=bc,
                                         dir_partitions=location,
                                         fileSchemaType=fileSchemaType,
                                         createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
                                         partitions={
                                             's_nationkey': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 16, 17, 18, 19, 20, 21, 22,
                                                             23, 24]},
                                         partitions_schema=[('s_nationkey', 'int32')],
                                         tables=['supplier'])

        # Run Query ------------------------------------------------------
        # Parameter to indicate if its necessary to order
        # the resulsets before compare them
        worder = 1
        use_percentage = False
        acceptable_difference = 0.01

        print("==============================")
        print(queryType)
        print("==============================")

        queryId = "TEST_001"
        query = "select o_totalprice from orders where o_orderstatus = 'F' order by o_orderkey"
        query_spark = """   select o_totalprice 
                            from orders 
                            where o_orderstatus = 'F' and o_orderpriority <> '2-HIGH' 
                            and o_orderstatus <> 'P' and o_orderdate <= '1992-01-15'
                            and o_orderpriority IS NOT NULL and o_orderstatus IS NOT NULL and o_orderdate IS NOT NULL 
                            order by o_orderkey"""
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
            query_spark=query_spark,
        )

        queryId = "TEST_002"
        query = """ select l.l_orderkey, l.l_linenumber from lineitem as l
                            inner join orders as o on l.l_orderkey = o.o_orderkey
                            and l.l_commitdate < o.o_orderdate
                            and l.l_receiptdate > o.o_orderdate"""
        query_spark = """   select l.l_orderkey, l.l_linenumber from lineitem as l
                            inner join orders as o on l.l_orderkey = o.o_orderkey
                            and l.l_commitdate < o.o_orderdate
                            and l.l_receiptdate > o.o_orderdate
                            where l.l_linestatus not in ('O')
                            and l.l_shipmode not in ('MAIL', 'RAIL')
                            and o.o_orderpriority <> '2-HIGH'
                            and o.o_orderstatus <> 'P'
                            and o.o_orderdate <= '1992-01-15'
                            and o.o_orderpriority IS NOT NULL 
                            and o.o_orderstatus IS NOT NULL 
                            and o.o_orderdate IS NOT NULL"""
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
            query_spark=query_spark,
        )

        queryId = "TEST_003"
        query = """ select l_linenumber, l_shipmode, l_returnflag
                            from lineitem
                            where l_shipmode in ('AIR','FOB','MAIL','RAIL') and l_returnflag in ('A', 'N')
                            order by l_orderkey"""
        query_spark = """   select l_linenumber, l_shipmode, l_returnflag
                            from lineitem
                            where l_shipmode in ('AIR','FOB') and l_returnflag in ('A', 'N')
                            and l_linestatus not in ('O')
                            and l_shipmode IS NOT NULL
                            and l_linestatus IS NOT NULL
                            and l_returnflag IS NOT NULL
                            order by l_orderkey"""
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
            query_spark=query_spark,
        )

        queryId = "TEST_004"
        query = """select n_regionkey as rkey, n_nationkey from nation
                            where n_regionkey < 3 and n_nationkey > 5"""
        query_spark = """select n_regionkey as rkey, n_nationkey from nation
                            where n_regionkey < 3 and n_nationkey > 5
                            and n_regionkey not in (0, 1)"""
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
            query_spark=query_spark,
        )

        if int(nRals) == 1:
            queryId = "TEST_005"
            query = """select p.p_partkey, p.p_mfgr
                                from part p
                                where p.p_type like '%STEEL'"""
            query_spark = """select p.p_partkey, p.p_mfgr
                                from part p
                                where p.p_type like '%STEEL'
                                and p_container not in ('JUMBO BAG', 'JUMBO BOX', 'JUMBO CAN', 'JUMBO CASE',
                                                        'JUMBO DRUM', 'JUMBO JAR', 'JUMBO PACK', 'JUMBO PKG')"""

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
                query_spark=query_spark,
            )

            queryId = "TEST_006"
            query = """select p_partkey, p_mfgr, p_container
                                from part
                                where p_size = 35 and p_container like 'WRAP%'"""
            query_spark = """select p_partkey, p_mfgr, p_container
                                from part
                                where p_size = 35 and p_container like 'WRAP%'"""
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
                query_spark=query_spark,
            )

            queryId = "TEST_007"
            query = """ select count(s_suppkey), count(s_nationkey)
                        from supplier"""
            query_spark = """   select count(s_suppkey), count(s_nationkey)
                                from supplier
                                where s_nationkey not in (10, 11, 12, 13, 14, 15)"""
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
                query_spark=query_spark,
            )

        queryId = "TEST_008"
        query = """select n1.n_nationkey as supp_nation,
                            n2.n_nationkey as cust_nation,
                            l.l_extendedprice * l.l_discount
                            from supplier as s
                            inner join lineitem as l on s.s_suppkey = l.l_suppkey
                            inner join orders as o on o.o_orderkey = l.l_orderkey
                            inner join customer as c on c.c_custkey = o.o_custkey
                            inner join nation as n1 on s.s_nationkey = n1.n_nationkey
                            inner join nation as n2 on c.c_nationkey = n2.n_nationkey
                            where n1.n_nationkey = 1
                            and n2.n_nationkey = 2
                            and o.o_orderkey < 20"""
        query_spark = """select n1.n_nationkey as supp_nation,
                            n2.n_nationkey as cust_nation,
                            l.l_extendedprice * l.l_discount
                            from supplier as s
                            inner join lineitem as l on s.s_suppkey = l.l_suppkey
                            inner join orders as o on o.o_orderkey = l.l_orderkey
                            inner join customer as c on c.c_custkey = o.o_custkey
                            inner join nation as n1 on s.s_nationkey = n1.n_nationkey
                            inner join nation as n2 on c.c_nationkey = n2.n_nationkey
                            where n1.n_nationkey = 1
                            and n2.n_nationkey = 2
                            and o.o_orderkey < 20
                            and o.o_orderstatus = 'F' and o.o_orderpriority <> '2-HIGH' and o.o_orderstatus <> 'P'
                            and o.o_orderdate <= '1992-01-15'
                            and c.c_nationkey > 10 and c.c_mktsegment <> 'FURNITURE'
                            and l.l_linestatus not in ('O') and l.l_shipmode not in ('MAIL', 'RAIL')
                            and n1.n_regionkey not in (0, 1) and n2.n_regionkey not in (0, 1)
                            and s_nationkey not in (10, 11, 12, 13, 14, 15)
                            and o.o_orderpriority IS NOT NULL 
                            and o.o_orderstatus IS NOT NULL 
                            and o.o_orderdate IS NOT NULL
                            and c.c_nationkey IS NOT NULL
                            and c.c_mktsegment IS NOT NULL
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
            query_spark=query_spark,
        )

        queryId = "TEST_009"
        query = """ select s_name, s_address, s_nationkey
                            from supplier
                            where s_nationkey < 10
                            order by s_suppkey"""
        query_spark = """ select s_name, s_address, s_nationkey
                            from supplier
                            where s_nationkey < 10
                            order by s_suppkey"""
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
            query_spark=query_spark,
        )

        if Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break

def main(dask_client, spark, dir_data_file, bc, nRals):
    global tmpPath
    tempdir = tempfile.TemporaryDirectory(dir="/tmp")
    tmpPath = tempdir.name + '/BlazingSQL/partitions/'

    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
    if testsWithNulls == "true":
        tmpPath += "nulls/"
    else:
        tmpPath += "without-nulls/"

    start_mem = gpuMemory.capture_gpu_memory_usage()

    for fileSchemaType in data_types:
        createPartitions(fileSchemaType, dir_data_file)

    executionTestAuto(dask_client, spark, dir_data_file, bc, nRals)
    executionTestWithPartitions(dask_client, spark, dir_data_file, bc, nRals)
    executionTestWithSomePartitions(dask_client, spark, dir_data_file, bc, nRals)

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)

def createPartitions(fileSchemaType, dir_data_file):
    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
    if testsWithNulls == "true":
        dir_data = dir_data_file + "/tpch-with-nulls"
    else:
        dir_data = dir_data_file + "/tpch"

    ext = cs.get_extension(fileSchemaType)

    # orders table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "orders", ext),
                                         file_format=ext,
                                         table_name="orders",
                                         partitions={
                                             'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM',
                                                                 '4-NOT SPECIFIED',
                                                                 '5-LOW'],
                                             'o_orderstatus': ['F', 'O', 'P'],
                                             'o_orderdate': [pandas.Timestamp('1992-01-01 00:00:00'),
                                                             pandas.Timestamp('1992-01-02 00:00:00'),
                                                             pandas.Timestamp('1992-01-03 00:00:00'),
                                                             pandas.Timestamp('1992-01-04 00:00:00'),
                                                             pandas.Timestamp('1992-01-05 00:00:00'),
                                                             pandas.Timestamp('1992-01-06 00:00:00'),
                                                             pandas.Timestamp('1992-01-07 00:00:00'),
                                                             pandas.Timestamp('1992-01-08 00:00:00'),
                                                             pandas.Timestamp('1992-01-09 00:00:00'),
                                                             pandas.Timestamp('1992-01-10 00:00:00'),
                                                             pandas.Timestamp('1992-01-11 00:00:00'),
                                                             pandas.Timestamp('1992-01-12 00:00:00'),
                                                             pandas.Timestamp('1992-01-13 00:00:00'),
                                                             pandas.Timestamp('1992-01-14 00:00:00'),
                                                             pandas.Timestamp('1992-01-15 00:00:00'),
                                                             pandas.Timestamp('1992-01-16 00:00:00'),
                                                             pandas.Timestamp('1992-01-17 00:00:00'),
                                                             pandas.Timestamp('1992-01-18 00:00:00'),
                                                             pandas.Timestamp('1992-01-19 00:00:00'),
                                                             pandas.Timestamp('1992-01-20 00:00:00'),
                                                             pandas.Timestamp('1992-01-21 00:00:00'),
                                                             pandas.Timestamp('1992-01-22 00:00:00'),
                                                             pandas.Timestamp('1992-01-23 00:00:00'),
                                                             pandas.Timestamp('1992-01-24 00:00:00'),
                                                             pandas.Timestamp('1992-01-25 00:00:00'),
                                                             pandas.Timestamp('1992-01-26 00:00:00'),
                                                             pandas.Timestamp('1992-01-27 00:00:00'),
                                                             pandas.Timestamp('1992-01-28 00:00:00'),
                                                             pandas.Timestamp('1992-01-29 00:00:00'),
                                                             pandas.Timestamp('1992-01-30 00:00:00'),
                                                             pandas.Timestamp('1992-01-31 00:00:00')]},
                                         partitions_schema=[('o_orderpriority', 'str'),
                                                            ('o_orderstatus', 'str'),
                                                            ('o_orderdate', 'timestamp')],
                                         output=tmpPath + ext + '/orders/',
                                         num_files=1)

    # customer table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "customer", ext),
                                           file_format=ext,
                                           table_name="customer",
                                           partitions={
                                               'c_nationkey': [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24],
                                               'c_mktsegment': ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY']},
                                           partitions_schema=[('c_nationkey', 'int32'),
                                                            ('c_mktsegment', 'str')],
                                           output= tmpPath + ext + '/customer/',
                                           num_files=1)

    # lineitem table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "lineitem", ext),
                                           file_format=ext,
                                           table_name="lineitem",
                                           partitions={
                                                'l_shipmode':['AIR','FOB','MAIL','RAIL','REG','AIR','SHIP','TRUCK'],
                                                'l_linestatus':['F','O'],
                                                'l_returnflag':['A','N','R']
                                           },
                                           partitions_schema=[('l_shipmode', 'str'),
                                                            ('l_linestatus', 'str'),
                                                            ('l_returnflag', 'str')],
                                           output= tmpPath + ext + '/lineitem/',
                                           num_files=1)

    # nation table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "nation", ext),
                                           file_format=ext,
                                           table_name="nation",
                                           partitions={
                                               'n_regionkey': [0,1,2,3,4]},
                                           partitions_schema=[('n_regionkey', 'int32')],
                                           output= tmpPath + ext + '/nation/',
                                           num_files=1)

    # part table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "part", ext),
                                           file_format=ext,
                                           table_name="part",
                                           partitions={
                                               'p_container': ['JUMBO BAG', 'JUMBO BOX', 'JUMBO CAN', 'JUMBO CASE',
                                                               'JUMBO DRUM', 'JUMBO JAR', 'JUMBO PACK', 'JUMBO PKG',
                                                               'LG BAG', 'LG BOX', 'LG CAN', 'LG CASE', 'LG DRUM',
                                                               'LG JAR', 'LG PACK', 'LG PKG', 'MED BAG', 'MED BOX',
                                                               'MED CAN', 'MED CASE', 'MED DRUM', 'MED JAR', 'MED PACK',
                                                               'MED PKG', 'SM BAG', 'SM BOX', 'SM CAN', 'SM CASE',
                                                               'SM DRUM', 'SM JAR', 'SM PACK', 'SM PKG', 'WRAP BAG',
                                                               'WRAP BOX', 'WRAP CAN', 'WRAP CASE', 'WRAP DRUM',
                                                               'WRAP JAR', 'WRAP PACK', 'WRAP PKG']},
                                           partitions_schema=[('p_container', 'str')],
                                           output= tmpPath + ext + '/part/',
                                           num_files=1)

    # supplier table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "supplier", ext),
                                           file_format=ext,
                                           table_name="supplier",
                                           partitions={
                                               's_nationkey': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]},
                                           partitions_schema=[('s_nationkey', 'int32')],
                                           output= tmpPath + ext + '/supplier/',
                                           num_files=1)

if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    spark = "spark"

    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if (
        Settings.execution_mode == ExecutionMode.FULL and compareResults == "true"
    ) or Settings.execution_mode == ExecutionMode.GENERATOR:

        # Create Table Spark -------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark, Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL
    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(
        dask_client, spark, Settings.data["TestSettings"]["dataDirectory"], bc, nRals,
    )

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
