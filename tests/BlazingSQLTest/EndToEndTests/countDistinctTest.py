from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution,  gpuMemory, init_context, skip_test


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Count Distinct"

    def executionTest():
        tables = [
            "partsupp",
            "lineitem",
            "part",
            "supplier",
            "orders",
            "customer",
            "region",
            "nation",
        ]
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
            acceptable_difference = 0.1

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select count(distinct (n_regionkey + n_nationkey)),
                    n_regionkey from nation group by n_regionkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "n_regionkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_02"
            query = """select count(distinct o_custkey), o_orderkey
                    from orders where o_orderkey < 100
                    group by o_orderkey, (o_orderkey + o_custkey)"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_orderkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_03"
            query = """select count(distinct(o_orderkey + o_custkey))
                    as new_col, sum(o_orderkey), o_custkey
                    from orders group by o_custkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "o_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = 'TEST_04'
            query = """select count(distinct(o_custkey)), avg(o_totalprice),
                (o_orderkey + o_custkey) as num from orders
                where o_custkey < 100 group by o_custkey, o_orderkey"""
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
            query = """select count(distinct(o_custkey)), max(o_totalprice),
                    min(o_totalprice), avg(o_totalprice)
                    from orders group by o_custkey"""
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

            queryId = "TEST_06"
            query = """select n_nationkey, count(distinct(
                    n_regionkey + n_nationkey))/count(n_nationkey)
                    from nation group by n_nationkey"""
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

            queryId = "TEST_07"
            query = """select count(distinct(o_orderdate)), count(distinct(o_custkey)),
                    count(distinct(o_totalprice)), sum(o_orderkey)
                    from orders group by o_custkey"""
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

            queryId = 'TEST_08'
            query = """select COUNT(DISTINCT(n.n_nationkey)),
                    AVG(r.r_regionkey) from nation as n left outer join region as r
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

            queryId = "TEST_09"
            query = """select MIN(n.n_nationkey), MAX(r.r_regionkey),
                    COUNT(DISTINCT(n.n_nationkey + r.r_regionkey))
                    from nation as n left outer join region as r
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

            queryId = 'TEST_10'
            query = """select COUNT(DISTINCT(n1.n_nationkey)) as n1key,
                    COUNT(DISTINCT(n2.n_nationkey)) as n2key from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_regionkey"""
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

            queryId = 'TEST_11'
            query = """select r.r_regionkey, n.n_nationkey,
                    COUNT(n.n_nationkey), COUNT(DISTINCT(r.r_regionkey)),
                    SUM(DISTINCT(n.n_nationkey + r.r_regionkey)) from nation as n
                    left outer join region as r on n.n_nationkey = r.r_regionkey
                    GROUP BY r.r_regionkey, n.n_nationkey"""
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
            query = """select n1.n_regionkey, n2.n_nationkey,
                    MIN(n1.n_regionkey), MAX(n1.n_regionkey),
                    AVG(n2.n_nationkey)
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    GROUP BY n1.n_regionkey, n2.n_nationkey"""
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

            queryId = 'TEST_13'
            query = """select COUNT(DISTINCT(n.n_nationkey)),
                    AVG(r.r_regionkey) from nation as n right outer join region as r
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

            queryId = 'TEST_14'
            query = """select r.r_regionkey, n.n_nationkey,
                    COUNT(n.n_nationkey), COUNT(DISTINCT(r.r_regionkey)),
                    SUM(DISTINCT(n.n_nationkey + r.r_regionkey)) from nation as n
                    right outer join region as r on n.n_nationkey = r.r_regionkey
                    GROUP BY r.r_regionkey, n.n_nationkey"""
            # TODO: Create an issue to track these cases (just in distributed mode)
            if fileSchemaType != DataType.DASK_CUDF and fileSchemaType != DataType.CUDF:
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

    executionTest()

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

    main(dask_client, drill,
         Settings.data["TestSettings"]["dataDirectory"], bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
