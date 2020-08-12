from DataBase import createSchema as cs
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution
from Utils import gpuMemory, skip_test, init_context
from pynvml import nvmlInit
from blazingsql import DataType
from Configuration import ExecutionMode

queryType = 'Cross join'


def main(dask_client, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation", "region", "customer", "lineitem", "orders"]
        data_types = [DataType.DASK_CUDF, DataType.CUDF, DataType.CSV,
                      DataType.ORC, DataType.PARQUET]  # TODO json

        # Create Tables ------------------------------------------------------
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

            print('==============================')
            print(queryType)
            print('==============================')

            queryId = 'TEST_01'
            query = "select * from nation cross join region"
            runTest.run_query(bc, spark, query, queryId, queryType,
                              worder, '', acceptable_difference,
                              use_percentage, fileSchemaType)

            queryId = 'TEST_02'
            query = """
                    select o_orderkey, o_totalprice,
                         l_linenumber, l_shipmode
                    from orders cross join lineitem
                    where o_orderkey < 6
                    and l_receiptdate > date '1996-07-12'
                    and l_linenumber > 5
                    and o_totalprice < 74029.55
                    and o_clerk = 'Clerk#000000880'
                    and l_shipmode IN ('FOB', 'RAIL')
                    order by o_orderkey, o_totalprice, l_linenumber"""
            runTest.run_query(bc, spark, query, queryId, queryType,
                              worder, '', acceptable_difference,
                              use_percentage, fileSchemaType)

            queryId = 'TEST_03'
            query = """select o_orderkey, n_nationkey
                    from nation cross join orders
                    where o_totalprice > 4000.0
                    and o_orderdate > date '1998-07-12'
                    and o_orderkey > 425000
                    group by o_orderkey, n_nationkey
                    order by o_orderkey, n_nationkey"""
            runTest.run_query(bc, spark, query, queryId, queryType, worder,
                              '', acceptable_difference,
                              use_percentage, fileSchemaType)

            queryId = 'TEST_04'
            query = """
                    with cust_nation as
                    (
                        select c_custkey, c_name, n_nationkey, n_name
                        from customer inner join nation
                        on c_nationkey = n_nationkey
                        where n_nationkey > 21
                        and c_acctbal > 525.0
                        and c_custkey > 13450
                        order by c_custkey, n_nationkey
                    ), ord_lineitem as
                    (
                        select o_orderkey, l_quantity
                        from orders left join lineitem
                        on o_orderkey = l_orderkey
                        where l_shipdate > date '1998-11-12'
                        and o_totalprice > 3500.0
                        and l_quantity > 48.0
                        and l_shipmode in ('AIR', 'FOB', 'SHIP')
                        order by o_orderkey
                    )
                    select c_custkey, n_name, l_quantity
                    from ord_lineitem cross join cust_nation
                    where n_name = 'RUSSIA'
                    order by c_custkey"""
            runTest.run_query(bc, spark, query, queryId, queryType, worder,
                              '', acceptable_difference,
                              use_percentage, fileSchemaType)

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break

    executionTest()

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


if __name__ == '__main__':

    Execution.getArgs()

    nvmlInit()

    # NOTE: Drill doesn't support CROSS JOIN
    spark = "spark"

    compareResults = True
    if 'compare_results' in Settings.data['RunSettings']:
        compareResults = Settings.data['RunSettings']['compare_results']

    if ((Settings.execution_mode == ExecutionMode.FULL
         and compareResults == "true")
            or Settings.execution_mode == ExecutionMode.GENERATOR):
        # Create Table Spark --------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark,
                             Settings.data['TestSettings']['dataDirectory'])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data['RunSettings']['nRals']

    main(dask_client, spark, Settings.data['TestSettings']['dataDirectory'],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()

        
