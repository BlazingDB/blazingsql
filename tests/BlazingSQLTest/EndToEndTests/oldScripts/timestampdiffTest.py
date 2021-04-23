from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Timestampdiff"


def main(dask_client, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["lineitem", "orders"]
        data_types = [DataType.CSV, DataType.PARQUET]  # TODO json

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.init_spark_schema(
                spark,
                Settings.data["TestSettings"]["dataDirectory"],
                fileSchemaType=fileSchemaType,
            )
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

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
            query = """select l_shipdate, l_commitdate,
                        timestampdiff(DAY, l_commitdate, l_shipdate) as diff
                    from lineitem  limit 20"""
            query_spark = """select l_shipdate, l_commitdate,
                    datediff(l_shipdate, l_commitdate) as diff
                    from lineitem  limit 20"""
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
                query_spark=query_spark,
            )

            queryId = "TEST_02"
            query = """select l_shipdate, timestampdiff(DAY,
                         date '1970-01-01', l_shipdate) as diff
                    from lineitem  limit 20"""
            query_spark = """select l_shipdate, datediff(l_shipdate,
                            date '1970-01-01') as diff
                        from lineitem  limit 20"""
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
                query_spark=query_spark,
            )

            queryId = "TEST_03"
            query = """select * from orders
                    where timestampdiff(DAY,  date '1995-02-04',
                        o_orderdate) < 25"""
            query_spark = """select * from orders where
                    datediff(o_orderdate, date '1995-02-04') < 25"""
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
                query_spark=query_spark,
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

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
