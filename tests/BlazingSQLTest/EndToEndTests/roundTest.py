from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution,  gpuMemory, init_context, skip_test


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Round"

    def executionTest(queryType):
        tables = ["nation", "region", "customer", "orders", "lineitem"]
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
            acceptable_difference = 0

            print("==============================")
            print(queryType + " Tests")
            print("==============================")

            queryId = "TEST_01"
            query = """select ROUND(orders.o_orderkey),
                        ROUND(orders.o_totalprice)
                    from customer
                    left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 500"""
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
            query = """select ROUND(orders.o_totalprice, 2),
                        ROUND(orders.o_totalprice, -2)
                    from customer
                    left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 500"""
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
            query = """select customer.c_custkey, orders.o_orderkey,
                    ROUND(orders.o_custkey,0)
                    from customer
                    left outer join orders
                    on customer.c_custkey = orders.o_custkey
                    where customer.c_nationkey = 3
                    and customer.c_custkey < 500"""
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
            query = """select MAX(ROUND(n1.n_regionkey,3))
                    from nation as n1
                    full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6 """
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
            )  # WSM NEED TO REVISIT THIS

            queryId = "TEST_05"
            query = "select ROUND(AVG(o_totalprice)) from orders"
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

    executionTest(queryType)

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
