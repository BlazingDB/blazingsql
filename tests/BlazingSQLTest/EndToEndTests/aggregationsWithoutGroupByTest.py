from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Aggregations without group by"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["part", "region", "nation", "orders"]
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

            # Run Query -----------------------------------------------------
            # Parameter to indicate if its necessary to order the
            # resulsets before compare them
            worder = 0
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select count(p_partkey), sum(p_partkey),
                    avg(CAST(p_partkey AS DOUBLE)), max(p_partkey), min(p_partkey)
                    from part"""
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
            query = """select count(p_partkey), sum(p_partkey),
                    avg(p_partkey), max(p_partkey), min(p_partkey) from part
                    where p_partkey < 100"""
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
            query = """select count(p_partkey), sum(p_partkey),
                    avg(p_partkey), max(p_partkey), min(p_partkey)
                    from part where p_partkey < 0"""
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
            query = """select count(p_partkey) + sum(p_partkey),
                    (max(p_partkey) + min(p_partkey))/2
                    from part where p_partkey < 100"""
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
            query = """select MIN(n.n_nationkey), MAX(r.r_regionkey),
                    AVG(n.n_nationkey + r.r_regionkey) from nation as n
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

            queryId = "TEST_06"
            query = """select SUM(n1.n_nationkey) as n1key,
                    AVG(n2.n_nationkey +  n1.n_nationkey ) as n2key
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 10"""
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

            #      queryId = 'TEST_07'
            #      query = "select COUNT(n1.n_nationkey) as n1key,
            #              COUNT(DISTINCT(n2.n_nationkey +  n1.n_nationkey))
            #              as n2key from nation as n1 full outer join nation
            #              as n2 on n1.n_nationkey = n2.n_nationkey + 10"
            #      runTest.run_query(bc, drill, query, queryId, queryType,
            #                worder, '', acceptable_difference, use_percentage,
            #                fileSchemaType)

            if fileSchemaType != DataType.ORC:
                queryId = "TEST_08"
                query = """select min(o_orderdate), max(o_orderdate)
                    from orders where o_custkey between 10 and 20 """
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
        # Create Table Drill -----------------------------------------
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
