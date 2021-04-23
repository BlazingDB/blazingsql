from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Common Table Expressions"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = [
            "nation",
            "region",
            "customer",
            "orders",
            "part",
            "partsupp",
            "supplier",
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
            # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """with nationTemp as
                    (select n_nationkey, n_regionkey as fkey
                    from nation where n_nationkey > 3
                    order by n_nationkey)
                    select region.r_regionkey, nationTemp.n_nationkey
                    from region inner join nationTemp
                    on region.r_regionkey = nationTemp.fkey"""
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
            query = """with regionTemp as (
                    select r_regionkey from region where r_regionkey > 2 ),
                    nationTemp as(select n_nationkey, n_regionkey as fkey
                    from nation where n_nationkey > 3 order by n_nationkey)
                    select regionTemp.r_regionkey, nationTemp.fkey
                    from regionTemp inner join nationTemp
                    on regionTemp.r_regionkey = nationTemp.fkey"""
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
            query = """with ordersTemp as (
                    select min(o_orderkey) as priorityKey, o_custkey
                    from orders group by o_custkey
                    ), ordersjoin as( select orders.o_custkey from orders
                    inner join ordersTemp
                    on ordersTemp.priorityKey = orders.o_orderkey)
                    select customer.c_custkey, customer.c_nationkey
                    from customer inner join ordersjoin
                    on ordersjoin.o_custkey =  customer.c_custkey"""
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

            queryId = 'TEST_04'
            query = """with ordersTemp as (
                    select min(orders.o_orderkey) as priorityKey, o_custkey
                    from orders group by o_custkey
                    ), ordersjoin as( select orders.o_orderkey, orders.o_custkey
                    / (orders.o_custkey + 1) as o_custkey,
                    (ordersTemp.priorityKey + 1) as priorityKey from orders
                    inner join ordersTemp on
                    (ordersTemp.priorityKey = orders.o_orderkey)
                    )
                    select (customer.c_custkey + 1)
                    / (customer.c_custkey - customer.c_custkey + 1) from customer
                    inner join ordersjoin on ordersjoin.o_custkey = customer.c_custkey
                    where (customer.c_custkey > 1 or customer.c_custkey < 100)"""
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

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
