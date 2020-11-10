from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Coalesce"

    def executionTest(queryType):
        tables = ["nation", "region", "customer", "orders", "lineitem"]
        # TODO json
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
            # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            # if fileSchemaType == DataType.PARQUET : print('Save file arrow')

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select n.n_nationkey, COALESCE(r.r_regionkey,-1)
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    where n.n_nationkey < 10"""
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
                print_result=True,
            )

            queryId = "TEST_02"
            query = """select COALESCE(orders.o_orderkey, 100),
                    COALESCE(orders.o_totalprice, 0.01) from customer
                    left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 500"""
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
            query = """select COALESCE(orders.o_orderkey, customer.c_custkey),
                    COALESCE(orders.o_totalprice, customer.c_acctbal)
                    from customer left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 500"""
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
            query = """select customer.c_custkey, orders.o_orderkey,
                    COALESCE(orders.o_custkey,123456) from customer
                    left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 500"""
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
            query = """select COUNT(DISTINCT(COALESCE(n1.n_regionkey,32))),
                    AVG(COALESCE(n1.n_regionkey,32)) from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_06"
            query = """select SUM(COALESCE(n2.n_nationkey, 100)),
                    COUNT(DISTINCT(COALESCE(n1.n_nationkey,32))),
                    n2.n_regionkey as n1key from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    GROUP BY n2.n_regionkey"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_07"
            query = """select MIN(COALESCE(n.n_nationkey, r.r_regionkey)),
                    MAX(COALESCE(n.n_nationkey, 8)) from nation as n
                    left outer join region as r
                    on n.n_nationkey = r.r_regionkey"""
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
            query = """select AVG(CAST(COALESCE(n.n_nationkey,
                    r.r_regionkey) AS DOUBLE)),
                    MAX(COALESCE(n.n_nationkey, 8)),
                    COUNT(COALESCE(n.n_nationkey, 12)), n.n_nationkey
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    GROUP BY n.n_nationkey"""
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

            #     queryId = 'TEST_09'
            #     query = "select SUM(COALESCE(n2.n_nationkey, 100)),
            #       COUNT(DISTINCT(COALESCE(n1.n_nationkey,32))),
            #       COALESCE(n2.n_regionkey, 100) as n1key from nation as n1
            #       full outer join nation as n2
            #       on n1.n_nationkey = n2.n_nationkey + 6
            #       GROUP BY COALESCE(n2.n_regionkey, 100)"
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #       '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_10"
            query = "SELECT COALESCE(l_shipinstruct, l_comment) FROM lineitem"
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
            query = """select n.n_nationkey,
                    COALESCE(r.r_comment, n.n_comment) from nation as n
                    left outer join region as r
                    on n.n_nationkey = r.r_regionkey"""
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
            query = """SELECT COALESCE(l.l_shipinstruct, o.o_orderstatus)
                    FROM lineitem l inner join orders o
                    on l.l_orderkey = o.o_orderkey
                    where o.o_totalprice < 1574.23"""
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
            query = """ WITH
                t1_l AS ( SELECT * FROM orders ),
                t1_r AS ( SELECT * FROM customer ),
                main_lr AS(
                    SELECT
                        COALESCE(o.o_comment, c.c_comment) AS info
                    FROM
                        t1_l o FULL JOIN t1_r c
                        ON  o.o_custkey = c.c_custkey
                        AND o.o_orderkey = c.c_nationkey
                ) SELECT * FROM main_lr
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
            query = """
            WITH
            ltable3 AS (
                select lineitem.l_orderkey as orderkey,
                        lineitem.l_linestatus as linestatus
                from lineitem
                where mod(lineitem.l_orderkey, 2) = 0
            ),

            rtable1 AS (
                select lineitem.l_orderkey as orderkey,
                        lineitem.l_linestatus as linestatus
                from lineitem
                where mod(lineitem.l_partkey, 6) = 0
            ),
            rtable2 AS (
                select lineitem.l_orderkey as orderkey,
                        lineitem.l_linestatus as linestatus
                from lineitem
                where mod(lineitem.l_suppkey, 4) = 0
            ),
            rtable3 AS (
                select coalesce(l.orderkey, r.orderkey)  as orderkey,
                        coalesce(l.linestatus, r.linestatus) as linestatus
                from rtable1 l full join rtable2 r
                on l.orderkey = r.orderkey
                -- and l.linestatus = r.linestatus
            ),

            lastjoin AS (
                select l.orderkey,
                        coalesce(l.linestatus, r.linestatus) as linestatus
                from ltable3 l full join rtable3 r
                on l.orderkey = r.orderkey
                and l.linestatus = r.linestatus
            )

            select * from lastjoin
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

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break

    executionTest(queryType)

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


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

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
