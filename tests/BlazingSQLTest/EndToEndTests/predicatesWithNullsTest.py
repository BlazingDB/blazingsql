from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution,  gpuMemory, init_context, skip_test

queryType = "Predicates With Nulls"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation", "region", "customer", "lineitem",
                  "orders", "supplier"]
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
            acceptable_difference = 0

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select MIN(n.n_nationkey), MAX(r.r_regionkey),
                    AVG(CAST((n.n_nationkey + r.r_regionkey) AS DOUBLE))
                    from nation as n
                    left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    where n.n_nationkey IS NULL"""
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
            query = """select SUM(n1.n_nationkey) as n1key,
                    AVG(CAST((n2.n_nationkey +  n1.n_nationkey) AS DOUBLE))
                    as n2key
                    from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 10
                    where n1.n_nationkey IS NOT NULL"""
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

            queryId = "TEST_03"
            query = """select COUNT(n1.n_nationkey) as n1key,
                    COUNT(n2.n_nationkey +  n1.n_nationkey) as n2key
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 10
                    where n1.n_nationkey IS NOT NULL"""
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
            query = """select COUNT(n1.n_regionkey),
                        AVG(CAST(n1.n_regionkey AS DOUBLE))
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6
                    WHERE n1.n_regionkey IS NOT NULL"""
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
            query = """select MIN(n.n_nationkey), MAX(n.n_nationkey)
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE n.n_nationkey IS NULL"""
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

            queryId = 'TEST_06'
            query = """select COUNT(n.n_nationkey), AVG(r.r_regionkey)
            from nation as n left outer join region as r
            on n.n_nationkey = r.r_regionkey
            WHERE n.n_regionkey IS NULL"""
            runTest.run_query(
                bc,
                spark, # Drill shows: Different number of columns blzSQLresult: 2 PyDrill result: 0
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_07"
            query = """select n.n_nationkey, n.n_name, r.r_regionkey,
                        r.r_name
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE r.r_name IS NULL"""
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
            query = """select n.n_nationkey, n.n_name, r.r_regionkey,
                        r.r_name
                    from nation as n left outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE n.n_name IS NOT NULL"""
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
                    AVG(CAST((n.n_nationkey + r.r_regionkey) AS DOUBLE))
                    from nation as n
                    right outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    where n.n_nationkey IS NULL"""
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

            queryId = "TEST_10"
            query = """select n.n_nationkey, n.n_name, r.r_regionkey,
                        r.r_name
                    from nation as n right outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE r.r_name IS NULL"""
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
            query = """select n.n_nationkey, n.n_name, r.r_regionkey,
                        r.r_name
                    from nation as n right outer join region as r
                    on n.n_nationkey = r.r_regionkey
                    WHERE n.n_name IS NOT NULL"""
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
    spark = "spark"

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

        # Create Table Spark ------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark, Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill, spark, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
