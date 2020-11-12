from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "dayOfWeek"


def main(dask_client, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():

        tables = ["lineitem", "orders", "nation", "region"]
        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.PARQUET,
        ]  # TODO json

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
            query = """select o_orderkey, DAYOFWEEK(o_orderdate) as day_of_week
                    from orders where o_orderkey < 250 order by o_orderkey"""
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

            queryId = "TEST_02"
            query = """select o_orderkey, o_totalprice, DAYOFWEEK(o_orderdate) as day_of_week
                    from orders where o_orderkey < 1850 and DAYOFWEEK(o_orderdate) = 6
                    order by o_orderkey"""
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

            queryId = "TEST_03"
            query = """select o_orderkey, case when DAYOFWEEK(o_orderdate) = 6
                    OR DAYOFWEEK(o_orderdate) = 7 then 'Weekend'
                    else 'Weekday' end as day_of_week
                    from orders where o_orderkey > 5450 order by o_orderkey"""
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

            queryId = "TEST_04"
            query = """ with dayofweektable as (
                        select o_orderkey, DAYOFWEEK(o_orderdate) as num_of_week from orders
                    )
                    select o_orderkey, num_of_week, 
                        case when num_of_week = 1 then 'Mon'
                        when num_of_week = 2 then 'Tue'
                        when num_of_week = 3 then 'Wed'
                        when num_of_week = 4 then 'Thu'
                        when num_of_week = 5 then 'Fri'
                        when num_of_week = 6 then 'Sat'
                        else 'Sun' end as day_of_week
                    from dayofweektable order by o_orderkey limit 100"""
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

            queryId = "TEST_05"
            query = """with ordersdaystable as (
                        select o_orderkey as key, DAYOFWEEK(o_orderdate) as num_of_week from orders
                    ), lineitemdaystable as (
                        select l_orderkey as key, DAYOFWEEK(l_shipdate) as num_of_week from lineitem
                    )
                    select 'Saturday' as day_, count(o.num_of_week) as n_days
                    from ordersdaystable as o
                    inner join lineitemdaystable as l 
                    ON o.key = l.key
                    where l.num_of_week = 6
                    """
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

            queryId = "TEST_06"
            query = """with ordersperutable as (
                        select o_orderkey, DAYOFWEEK(o_orderdate) as num_of_week, n_name as country
                        from orders
                        inner join nation on DAYOFWEEK(o_orderdate) = n_nationkey
                        where n_name in ('PERU', 'ARGENTINA', 'BRAZIL', 'UNITED STATES') 
                    ), lineitemamericatable as (
                        select l_orderkey, DAYOFWEEK(l_shipdate) as num_of_week, r_name as region
                        from lineitem
                        inner join region on DAYOFWEEK(l_shipdate) = r_regionkey
                        where r_name = 'AMERICA'
                    )
                    select o_orderkey, o.num_of_week as num_day_o, 
                    case when o.num_of_week = 1 then 'Mon'
                        when o.num_of_week = 2 then 'Tue'
                        when o.num_of_week = 3 then 'Wed'
                        when o.num_of_week = 4 then 'Thu'
                        when o.num_of_week = 5 then 'Fri'
                        when o.num_of_week = 6 then 'Sat'
                        else 'Sun' end as day_of_week
                    from ordersperutable as o
                    inner join lineitemamericatable as l 
                    ON o_orderkey = l_orderkey
                    where o.num_of_week <> 7
                    and l.num_of_week <> 7
                    order by o_orderkey, o.num_of_week
                    limit 75"""
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
            
            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break

    executionTest()

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    spark = "spark"

    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if ((Settings.execution_mode == ExecutionMode.FULL and
         compareResults == "true") or
            Settings.execution_mode == ExecutionMode.GENERATOR):

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

        spark,
        Settings.data["TestSettings"]["dataDirectory"],
        bc,
        nRals,
    )

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
