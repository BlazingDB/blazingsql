from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test


def main(dask_client, drill, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Concat"

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
            DataType.JSON
        ]

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
            query = """select c_mktsegment || ': ' || c_custkey || ' - ' ||
                    c_name from customer
                    order by c_custkey, c_mktsegment limit 50"""
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
            query = """select r.r_name || ' ' || n.n_name from region r
                        inner join nation n
                        on n.n_regionkey = r.r_regionkey
                        order by r.r_name"""
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
            query = """select c.c_name || ' ' || o.o_orderkey, o.o_orderstatus
                    from orders o
                    inner join customer c on o.o_custkey = c.c_custkey
                    where c.c_custkey < 10"""
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
            query = """select c.c_name, o.o_orderkey, o.o_orderstatus
                    from orders o
                    inner join customer c on o.o_custkey = c.c_custkey
                    where 'Customer#000000' || c.c_custkey like
                    'Customer#0000001'"""
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
            query = """select c_custkey, 'Cliente: ' || c_name from customer
                       order by c_custkey, c_mktsegment limit 50"""
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
            query = """select o.o_orderkey || c.c_name, o.o_orderstatus
                    from orders o
                    inner join customer c on o.o_custkey = c.c_custkey
                    where c.c_custkey < 10"""
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

            queryId = "TEST_07"
            query = """select o.o_orderkey, c.c_name ||
                    cast(c.c_custkey as VARCHAR), c.c_name || '-' ||
                    cast(c.c_custkey as VARCHAR), o.o_orderstatus
                    from orders o
                    inner join customer c on o.o_custkey = c.c_custkey
                    where c.c_custkey < 10"""
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
            query = """select c.c_name || ': ' || c.c_custkey, c.c_name ||
                     ': ' || c.c_comment from customer c
                    where c.c_custkey < 10"""
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
            query = """select o.o_orderkey, c.c_name || '-' ||
                     (c.c_custkey + 1), o.o_orderstatus from orders o
                    inner join customer c on o.o_custkey = c.c_custkey
                    where c.c_custkey < 20"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            # '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = "TEST_10"
            query = """select * from (
                        select c.c_custkey, 'Customer#000000' ||
                        c.c_custkey as n_name from customer c
                        where c.c_custkey < 10
                    ) as n where n.n_name = 'Customer#000000' ||
                     n.c_custkey"""
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
            query = """select c.c_custkey, c.c_name || '- ' ||
                    c.c_custkey, c.c_comment from customer c
                    where c.c_custkey < 0"""
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
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

        # Create Table Spark -------------------------------------------------
        from pyspark.sql import SparkSession

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
