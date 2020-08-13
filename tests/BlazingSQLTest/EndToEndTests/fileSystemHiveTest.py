import os

from blazingsql import DataType
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pyhive import hive
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, startHadoop

queryType = "Hive FileSystem"


def main(dask_client, drill, dir_data_lc, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        # Read Data TPCH------------------------------------------------------
        authority = "hadoop.docker.com:9000"
        ktoken = "../KrbHDFS/myconf/krb5cc_0"
        krbticket = os.path.abspath(ktoken)
        hdfs_host = "172.22.0.3"
        hdfs_port = 9000
        hdfs_driver = "libhdfs"
        print("Using krb ticket: " + krbticket)
        result, error_msg, fs = bc.hdfs(
            authority,
            host=hdfs_host,
            port=hdfs_port,
            user="jhs",
            driver=hdfs_driver,
            kerb_ticket=krbticket,
        )

        if result is False:
            msg = (
                """WARNING: Could not connect to HDFS instance %s:%d using
                  driver %s, error was: %s"""
                % (hdfs_host, hdfs_port, hdfs_driver, error_msg)
            )
            print(msg)
            print("WARNING: Will ignore " + queryType)
            return

        print("Success connection to HDFS:")
        print(fs)

        hdfs_dir_data_lc = "hdfs://" + authority + dir_data_lc
        print("TPCH files at: " + hdfs_dir_data_lc)

        tables = [
            "nation",
            "region",
            "supplier",
            "customer",
            "lineitem",
            "orders",
            "part",
        ]
        # tables = ['customer']
        # TODO json1
        data_types = [DataType.CSV, DataType.ORC, DataType.PARQUET]

        for fileSchemaType in data_types:
            # if skip_test(dask_client, nRals, fileSchemaType, queryType):
            #  continue
            cs.create_hive_tables(bc, hdfs_dir_data_lc, fileSchemaType,
                                  tables=tables)

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
            query = """select count(n_nationkey), count(n_regionkey)
                    from nation"""
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
            query = """select count(s_suppkey), count(s_nationkey)
                    from supplier"""
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
            query = """select count(c_custkey), sum(c_acctbal),
                    sum(c_acctbal)/count(c_acctbal),
                    min(c_custkey), max(c_nationkey),
                    (max(c_nationkey) + min(c_nationkey))/2 c_nationkey
                from customer where c_custkey < 100
                group by c_nationkey"""
            runTest.run_query(
                bc, drill, query, queryId, queryType, worder, "", 0.01, True
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
                fileSchemaType,
            )

            queryId = "TEST_10"
            query = """select sin(c_acctbal), cos(c_acctbal),
                    asin(c_acctbal), acos(c_acctbal),
                    ln(c_acctbal), tan(c_acctbal),
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
            query = """select n1.n_nationkey as n1key,
                    n2.n_nationkey as n2key, n1.n_nationkey + n2.n_nationkey
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

            queryId = "TEST_13"
            query = """select o_orderkey, o_custkey
                    from orders where o_orderkey < 10 and o_orderkey >= 1"""
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
            query = """select 100168549 - sum(o_orderkey)/count(o_orderkey),
                        56410984/sum(o_totalprice),
                        (123 - 945/max(o_orderkey)) /
                            (sum(81619/o_orderkey)/count(81619/o_orderkey))
                    from orders where o_orderkey < 50"""
            runTest.run_query(
                bc, drill, query, queryId, queryType, worder, "", 0.01, True
            )  # TODO: Change sum/count for avg KC

            queryId = "TEST_15"
            query = """select EXTRACT(YEAR FROM l_receiptdate) -
                          EXTRACT(YEAR FROM l_shipdate) as years_late,
                        EXTRACT(MONTH FROM l_receiptdate) -
                          EXTRACT(MONTH FROM l_shipdate) as months_late,
                        EXTRACT(DAY FROM l_receiptdate) -
                          EXTRACT(DAY FROM l_shipdate) as days_late
                    from lineitem
                    where l_shipdate < DATE '1993-01-01'"""
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

    executionTest()

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    compare_results = True
    if "compare_results" in Settings.data["RunSettings"]:
        compare_results = Settings.data["RunSettings"]["compare_results"]

    drill = None

    if compare_results:
        # Create Table Drill ------------------------------------------------
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

    startHadoop.start_hdfs()

    # Create Table Hive ----------------------------------------------------
    cursor = hive.connect("172.22.0.3").cursor()
    cs.init_hive_schema(drill, cursor,
                        Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL
    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    startHadoop.stop_hdfs()

    runTest.save_log()

    gpuMemory.print_log_gpu_memory()
