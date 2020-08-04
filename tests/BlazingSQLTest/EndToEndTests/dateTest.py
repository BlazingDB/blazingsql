from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from pyspark.sql import SparkSession
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Date"


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation", "region", "customer", "orders", "lineitem"]
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

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select EXTRACT(YEAR FROM l_receiptdate) -
                        EXTRACT(YEAR FROM l_shipdate) as years_late,
                    EXTRACT(MONTH FROM l_receiptdate) -
                        EXTRACT(MONTH FROM l_shipdate) as months_late,
                    EXTRACT(DAY FROM l_receiptdate)
                        - EXTRACT(DAY FROM l_shipdate) as days_late
                    from lineitem where l_shipdate < DATE '1993-01-01'"""
            if fileSchemaType == DataType.ORC:
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
            else:
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
            query = """select o_orderkey as okey, o_custkey as ckey,
                    (EXTRACT(YEAR FROM o_orderdate) - 5) from orders
                    where o_orderstatus = 'O' order by okey"""
            if fileSchemaType == DataType.ORC:
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
            else:
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
            query = """select orders.o_orderkey, orders.o_orderdate,
                    orders.o_orderstatus
                    from orders inner join lineitem
                        on lineitem.l_orderkey = orders.o_orderkey
                    where orders.o_orderkey < 30 and lineitem.l_orderkey < 20
                    order by orders.o_orderkey, lineitem.l_linenumber,
                        orders.o_custkey, lineitem.l_orderkey"""
            if fileSchemaType == DataType.ORC:
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
            else:
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
            query = """select customer.c_nationkey, customer.c_name,
                    orders.o_orderdate, lineitem.l_receiptdate
                    from customer left
                    outer join orders on customer.c_custkey = orders.o_custkey
                    inner join lineitem
                        on lineitem.l_orderkey = orders.o_orderkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 100
                    and orders.o_orderdate < '1990-01-01'
                    order by orders.o_orderkey, lineitem.l_linenumber"""
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
            query = """select orders.o_orderkey, orders.o_orderdate,
                    lineitem.l_receiptdate, orders.o_orderstatus
                    from orders inner join lineitem
                    on lineitem.l_receiptdate = orders.o_orderdate
                    where orders.o_orderkey < 30 and lineitem.l_orderkey < 20
                    order by orders.o_orderkey, lineitem.l_linenumber"""
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

            # if Settings.execution_mode == ExecutionMode.GENERATOR:
            #     print("==============================")
            #     break

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
        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark,
                             Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(
        dask_client,
        drill,
        spark,
        Settings.data["TestSettings"]["dataDirectory"],
        bc,
        nRals,
    )

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
