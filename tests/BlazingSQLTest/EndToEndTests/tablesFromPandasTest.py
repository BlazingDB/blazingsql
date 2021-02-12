from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution,  gpuMemory, init_context, skip_test

queryType = "Tables from Pandas"


def main(dask_client, drill, dir_data_file, bc, nRals):
    tables = ["nation", "region", "supplier", "customer", "lineitem", "orders"]
    data_types = [DataType.CUDF]  # TODO csv orc parquet json

    # Create Tables -----------------------------------------------------
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue
        cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

        # Run Query -----------------------------------------------------
        # Parameter to indicate if its necessary to order
        # the resulsets before compare them
        worder = 1
        use_percentage = False
        acceptable_difference = 0.01

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
            fileSchemaType,
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
            fileSchemaType,
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
            fileSchemaType,
        )

        queryId = "TEST_04"
        query = """select count(c_custkey), sum(c_acctbal), avg(c_acctbal),
                    min(c_custkey), max(c_nationkey),
                    (max(c_nationkey) + min(c_nationkey)) / 2 c_nationkey
                from customer where c_custkey < 100
                group by c_nationkey"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            0.01,
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

        queryId = "TEST_06"
        query = """select c_custkey, c_nationkey, c_acctbal
                from customer order by c_nationkey, c_custkey, c_acctbal"""
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
            fileSchemaType,
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
            fileSchemaType,
        )

        queryId = "TEST_09"
        query = """select c_custkey, c_nationkey as nkey
                from customer where c_custkey < 0 and c_nationkey >=30"""
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
        query = """select sin(c_acctbal), cos(c_acctbal), asin(c_acctbal),
                    acos(c_acctbal), ln(c_acctbal), tan(c_acctbal),
                    atan(c_acctbal), floor(c_acctbal), c_acctbal
                from customer"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            0.01,
            use_percentage,
            fileSchemaType,
        )

        queryId = "TEST_11"
        query = """select n1.n_nationkey as n1key, n2.n_nationkey as n2key,
                    n1.n_nationkey + n2.n_nationkey
                from nation as n1
                full outer join nation as n2
                on n1.n_nationkey = n2.n_nationkey + 6
                where n1.n_nationkey < 10
                and n1.n_nationkey > 5"""
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
        query = """select count(n1.n_nationkey) as n1key,
                    count(n2.n_nationkey) as n2key,
                    count(*) as cstar
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
            fileSchemaType,
        )

        queryId = "TEST_14"
        query = """select 100168549 - avg(CAST(o_orderkey AS DOUBLE)),
                    56410984 / sum(o_totalprice),
                    (123 - 945/max(o_orderkey)) /
                        avg(CAST(81619/o_orderkey AS DOUBLE))
                from orders"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            0.01,
            use_percentage,
            fileSchemaType,
        )

        queryId = "TEST_15"
        query = """select EXTRACT(YEAR FROM l_receiptdate) -
                        EXTRACT(YEAR FROM l_shipdate) as years_late,
                    EXTRACT(MONTH FROM l_receiptdate) -
                        EXTRACT(MONTH FROM l_shipdate) as months_late,
                    EXTRACT(DAY FROM l_receiptdate) -
                        EXTRACT(DAY FROM l_shipdate) as days_late
                from lineitem where l_shipdate < DATE '1993-01-01'"""
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


if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    drill = "drill"  # None

    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if ((Settings.execution_mode == ExecutionMode.FULL and
         compareResults == "true") or
            Settings.execution_mode == ExecutionMode.GENERATOR):
        # Create Table Drill ------------------------------------------------
        print("starting drill")
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill,
         Settings.data["TestSettings"]["dataDirectory"], bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
