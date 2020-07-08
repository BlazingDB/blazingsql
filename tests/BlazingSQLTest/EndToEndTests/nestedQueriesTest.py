from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution,  gpuMemory, init_context, skip_test

queryType = "Nested Queries"


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
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select maxPrice, avgSize
                    from (
                        select avg(CAST(p_size AS DOUBLE)) as avgSize,
                        max(p_retailprice) as maxPrice,
                        min(p_retailprice) as minPrice
                        from part
                    ) as partAnalysis
                    order by maxPrice, avgSize"""
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
            query = """select custOrders.avgPrice, custOrders.numOrders
                    from customer
                    inner join
                    (
                        select o_custkey as o_custkey,
                        avg(o_totalprice) as avgPrice,
                        count(o_totalprice) as numOrders
                        from orders
                        where o_custkey <= 100
                        group by o_custkey
                    ) as custOrders
                    on custOrders.o_custkey = customer.c_custkey
                    where customer.c_nationkey <= 5"""
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
            query = """select partSuppTemp.partKey, partAnalysis.avgSize
                    from
                    (
                        select min(p_partkey) as partKey,
                        avg(CAST(p_size AS DOUBLE)) as avgSize,
                        max(p_retailprice) as maxPrice,
                        min(p_retailprice) as minPrice from part
                    ) as partAnalysis
                    inner join
                    (
                        select ps_partkey as partKey, ps_suppkey as suppKey
                        from partsupp where ps_availqty > 2
                    ) as partSuppTemp
                    on partAnalysis.partKey = partSuppTemp.partKey
                    inner join
                    (
                        select s_suppkey as suppKey from supplier
                    ) as supplierTemp
                    on supplierTemp.suppKey = partSuppTemp.suppKey"""
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
            query = """select avg(CAST(custKey AS DOUBLE))
                    from
                    (
                        select customer.c_custkey as custKey
                        from
                        (
                            select min(o_custkey) as o_custkey from orders
                        ) as tempOrders
                        inner join customer on
                        tempOrders.o_custkey = customer.c_custkey
                        where customer.c_nationkey > 6
                    ) as joinedTables"""
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
