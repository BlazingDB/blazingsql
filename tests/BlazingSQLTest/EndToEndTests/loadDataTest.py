# from blazingsql import DataType
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from Runner import runTest
from Utils import Execution, init_context


def main(dask_client, bc):

    # Create Table Drill ------------------------------------------------

    from pydrill.client import PyDrill

    drill = PyDrill(host="localhost", port=8047)

    dir_data_lc = Settings.data["TestSettings"]["dataDirectory"]

    for x in range(5):

        # [numberOfFiles, type_nation, type_region, type_supplier,
        #  type_customer, type_lineitem, type_orders]

        run = []

        if x == 0:
            run = [1, "psv", "psv", "psv", "psv", "psv", "psv"]
        elif x == 1:
            run = [2, "parquet", "parquet", "parquet", "parquet",
                   "parquet", "parquet"]
        elif x == 2:
            run = [6, "parquet", "psv", "parquet", "psv", "parquet", "psv"]
        elif x == 3:
            run = [10, "psv", "parquet", "psv", "parquet", "psv", "parquet"]
        elif x == 4:
            run = [12, "psv", "psv", "parquet", "parquet", "psv", "parquet"]

        print("============================================================")
        print("Running " + str(x + 1) + ":")
        print("Número de Archivos: " + str(run[0]))
        print("Type of files for Nation: " + run[1])
        print("Type of files for Region: " + run[2])
        print("Type of files for Supplier: " + run[3])
        print("Type of files for Customer: " + run[4])
        print("Type of files for Lineitem: " + run[5])
        print("Type of files for Orders: " + run[6])
        print("============================================================")
        print("1")
        num_files = run[0]
        print("2")
        cs.init_drill_schema(drill, dir_data_lc, n_files=num_files)
        print("3")
        # Read Data TPCH-----------------------------------------------------
        nation_files = cs.get_filenames_table("nation", dir_data_lc,
                                              num_files, run[1])
        bc.create_table(
            "nation",
            nation_files,
            delimiter="|",
            dtype=cs.get_dtypes("nation"),
            names=cs.get_column_names("nation"),
        )

        region_files = cs.get_filenames_table("region", dir_data_lc,
                                              num_files, run[2])
        bc.create_table(
            "region",
            region_files,
            delimiter="|",
            dtype=cs.get_dtypes("region"),
            names=cs.get_column_names("region"),
        )

        supplier_files = cs.get_filenames_table(
            "supplier", dir_data_lc, num_files, run[3]
        )
        bc.create_table(
            "supplier",
            supplier_files,
            delimiter="|",
            dtype=cs.get_dtypes("supplier"),
            names=cs.get_column_names("supplier"),
        )

        customer_files = cs.get_filenames_table(
            "customer", dir_data_lc, num_files, run[4]
        )
        bc.create_table(
            "customer",
            customer_files,
            delimiter="|",
            dtype=cs.get_dtypes("customer"),
            names=cs.get_column_names("customer"),
        )

        lineitem_files = cs.get_filenames_table(
            "lineitem", dir_data_lc, num_files, run[5]
        )
        bc.create_table(
            "lineitem",
            lineitem_files,
            delimiter="|",
            dtype=cs.get_dtypes("lineitem"),
            names=cs.get_column_names("lineitem"),
        )

        orders_files = cs.get_filenames_table("orders", dir_data_lc,
                                              num_files, run[6])
        bc.create_table(
            "orders",
            orders_files,
            delimiter="|",
            dtype=cs.get_dtypes("orders"),
            names=cs.get_column_names("orders"),
        )

        # Run Query ------------------------------------------------------
        # Parameter to indicate if its necessary to order
        # the resulsets before compare them
        worder = 1
        use_percentage = False
        acceptable_difference = 0.01
        queryType = "Load Data Test"

        print("==============================")
        print(queryType)
        print("==============================")

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
            # fileSchemaType,
        )

        queryId = "TEST_02"
        query = "select count(n_nationkey), count(n_regionkey) from nation"
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
            # fileSchemaType,
        )

        queryId = "TEST_03"
        query = "select count(s_suppkey), count(s_nationkey) from supplier"
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
            # fileSchemaType,
        )

        queryId = "TEST_04"
        query = """select count(c_custkey), sum(c_acctbal),
                    sum(c_acctbal)/count(c_acctbal), min(c_custkey),
                    max(c_nationkey),
                    (max(c_nationkey) + min(c_nationkey))/2 c_nationkey
                    from customer
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
        )  # TODO: Change sum/count for avg KC

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
            # fileSchemaType,
        )

        queryId = "TEST_06"
        query = """select c_custkey, c_nationkey, c_acctbal
                from customer order by c_nationkey, c_acctbal"""
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
            # fileSchemaType,
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
            # fileSchemaType,
        )

        queryId = "TEST_08"
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
                and n2.n_nationkey = 2 and o.o_orderkey < 10000"""
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
            # fileSchemaType,
        )

        queryId = "TEST_09"
        query = """select c_custkey, c_nationkey as nkey
                from customer where c_custkey < 0 and c_nationkey >= 30"""
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
            # fileSchemaType,
        )

        queryId = "TEST_10"
        query = """select sin(c_acctbal), cos(c_acctbal),
                    sin(c_acctbal), acos(c_acctbal),
                    ln(c_acctbal), tan(c_acctbal),
                    atan(c_acctbal), floor(c_acctbal),
                    c_acctbal
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
            # fileSchemaType,
        )

        queryId = "TEST_11"
        query = """select n1.n_nationkey as n1key,
                n2.n_nationkey as n2key, n1.n_nationkey + n2.n_nationkey
                from nation as n1 full outer join nation as n2
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
            # fileSchemaType,
        )

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
            # fileSchemaType,
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
            # fileSchemaType,
        )

        queryId = "TEST_14"
        query = """select 100168549 - sum(o_orderkey)/count(o_orderkey),
                    56410984 / sum(o_totalprice),
                    (123 - 945/max(o_orderkey)) / (sum(81619/o_orderkey) /
                    count(81619/o_orderkey))
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
        )  # TODO: Change sum/count for avg KC

        queryId = "TEST_15"
        query = """select o_orderkey, sum(o_totalprice)/count(o_orderstatus)
                from orders where o_custkey < 100
                group by o_orderstatus, o_orderkey"""
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
            # fileSchemaType,
        )

        queryId = "TEST_16"
        query = """select o_orderkey, o_orderstatus
                from orders where o_custkey < 10
                and o_orderstatus <> 'O'
                order by o_orderkey, o_orderstatus
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
            # fileSchemaType,
        )

        queryId = "TEST_17"
        query = """select count(o_orderstatus)
                from orders where o_orderstatus <> 'O'"""
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
            # fileSchemaType,
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
            use_percentage,
            # fileSchemaType,
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
        )  # TODO: Change sum/count for avg KC

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
            # fileSchemaType,
        )

        queryId = "TEST_21"
        query = """with regionTemp as ( select r_regionkey,
                 r_name from region where r_regionkey > 2 ),
                nationTemp as(select n_nationkey, n_regionkey as fkey,
                n_name from nation where n_nationkey > 3
                order by n_nationkey)
                select regionTemp.r_name, nationTemp.n_name
                from regionTemp inner join nationTemp on
                regionTemp.r_regionkey = nationTemp.fkey"""
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
            # fileSchemaType,
        )

        queryId = "TEST_22"
        query = """select o.o_totalprice, l.l_partkey
                from orders as o
                left outer join lineitem as l on o.o_custkey = l.l_linenumber
                and l.l_suppkey = o.o_orderkey where l.l_linenumber < 1000"""
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
        query = """select o.o_orderkey, o.o_totalprice,
                l.l_partkey, l.l_returnflag from lineitem as l
                inner join orders as o on o.o_orderkey = l.l_orderkey
                inner join customer as c on c.c_custkey = o.o_custkey
                where c.c_custkey < 1000"""
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
        query = """select o.o_orderkey, o.o_totalprice,
                l.l_partkey, l.l_linestatus from orders as o
                full outer join lineitem as l on
                l.l_orderkey = o.o_orderkey where o.o_orderkey < 1000"""
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

        runTest.save_log()


if __name__ == "__main__":
    Execution.getArgs()
    # Create Context For BlazingSQL
    bc, dask_client = init_context()

    main(dask_client, bc)
