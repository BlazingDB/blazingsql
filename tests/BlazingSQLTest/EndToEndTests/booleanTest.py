from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution,  gpuMemory, init_context, skip_test

queryType = "Boolean"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation", "bool_orders", "region", "customer"]
        bool_orders_index = tables.index("bool_orders")
        data_types = [DataType.CSV]  # TODO ORC gdf parquet json

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(
                bc,
                dir_data_file,
                fileSchemaType,
                tables=tables,
                bool_orders_index=bool_orders_index,
            )

            # Set up util params----------------------------------------------
            worder = 1
            use_percentage = False
            non_aceptable_diff = 0
            min_aceptable_diff = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            #         queryId = 'TEST_00'
            #         query = "select 1 from nation"
            #         runTest.run_query(bc, drill, query, queryId, queryType,
            #           worder, '', non_aceptable_diff, use_percentage,
            #           fileSchemaType)
            #
            #         queryId = 'TEST_0A'
            #         query = "select *, 2 from nation"
            #         runTest.run_query(bc, drill, query, queryId, queryType,
            #           worder, '', non_aceptable_diff, use_percentage,
            #           fileSchemaType)
            #
            # Run Queries ---------------------------------------------------
            queryId = "TEST_01"
            query = """select * from bool_orders
                    order by o_orderkey, o_custkey limit 300"""

            testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
            if testsWithNulls == "true":
                query = """select o_orderkey, o_custkey, o_totalprice, o_confirmed from bool_orders
                        order by o_orderkey, o_totalprice limit 300"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_02"
            query = """select o_custkey, o_confirmed from bool_orders
                    where o_confirmed is null order by o_custkey limit 30"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                non_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_03"
            query = """select o_custkey, 0.75 * o_totalprice, o_confirmed
                    from bool_orders
                    where o_confirmed = true
                    order by o_custkey, o_orderKey limit 20"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_04"
            query = """select o_custkey, o_confirmed from bool_orders
                    where o_confirmed is not NULL
                    order by o_custkey, o_confirmed desc limit 25"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                non_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_05"
            query = """select o_custkey, 0.95 * o_totalprice, o_confirmed
                    from bool_orders where o_confirmed is null
                    order by o_totalprice limit 400"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_06"
            query = """select o_custkey, 0.75 * o_totalprice as proffit
                    from bool_orders
                    where o_custkey < 300 and o_confirmed = False"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_07"
            query = """select o_custkey, 0.75 * o_totalprice as proffit
                    from bool_orders
                    where o_custkey < 300 and o_confirmed is not NULL
                    order by o_custkey, o_orderkey limit 200"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_08"
            # when count() and o_confirmed is null (o_confirmed is not null)
            # are mixed then fails
            query = """select count(o_orderstatus) from bool_orders
                    where o_orderstatus <> 'O' and o_confirmed = true"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                non_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_09"
            query = """select sum(o_orderkey)/count(o_orderkey), max(o_totalprice)
                    from bool_orders where o_confirmed = true
                    and o_orderkey < 2500"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                True,
                fileSchemaType,
            )  # TODO: Change sum/count for avg KC

            queryId = "TEST_10"
            query = """select sum(o_orderkey)/count(o_orderkey),
                        max(o_totalprice)
                    from bool_orders
                    where o_confirmed IS NULL and o_orderkey < 2500"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            # '', min_aceptable_diff, True) #TODO: Change sum/count for avg KC

            queryId = "TEST_11"
            query = """select o_custkey, min(o_totalprice) from bool_orders
                   where o_custkey < 300 and o_confirmed is not NULL
                   group by o_custkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            #         queryId = 'TEST_12'
            # TODO: Change sum/count for avg KC
            #         query = """select count(o_custkey), max(o_totalprice),
            #           min(o_totalprice), sum(o_totalprice)
            #           /count(o_totalprice) from bool_orders
            #           group by o_custkey"""
            #         runTest.run_query(bc, drill, query, queryId, queryType,
            #            worder, '', min_aceptable_diff, True)
            #
            #         queryId = 'TEST_13'
            #         query = """select count(o_custkey), o_orderkey,
            #               o_confirmed from bool_orders
            #               where o_orderkey < 100 and o_confirmed = FALSE
            #               group by o_orderkey, (o_orderkey + o_custkey),
            #               o_confirmed"""
            #         runTest.run_query(bc, drill, query, queryId, queryType,
            #            worder, "o_orderkey", non_aceptable_diff,
            #             use_percentage, fileSchemaType)
            #
            #         queryId = 'TEST_14'
            #         query = "select o.o_custkey, c.c_name as customer_name
            #               from bool_orders as o inner join customer as c
            #               on c.c_custkey = o.o_custkey
            #               where o.o_confirmed = False
            #               and o.o_orderstatus <> 'O'"
            #         runTest.run_query(bc, drill, query, queryId, queryType,
            #            worder, "o_custkey", non_aceptable_diff,
            #            use_percentage, fileSchemaType)
            #
            #         queryId = 'TEST_15'
            #         query = "select o.o_custkey, c.c_name, n.n_regionkey
            #               from nation as n inner join customer as c
            #               on n.n_nationkey = c.c_nationkey
            #               inner join bool_orders as o on
            #               c.c_custkey = o.o_custkey
            #               where o.o_confirmed = False
            #               and o.o_orderstatus <> 'O'"
            #         runTest.run_query(bc, drill, query, queryId, queryType,
            #            worder, "o_custkey", non_aceptable_diff,
            #            use_percentage, fileSchemaType)
            #
            #         queryId = 'TEST_16'
            #         query = "select o.o_custkey, c.c_name, n.n_regionkey
            #               from customer as c inner join bool_orders as o
            #               on c.c_custkey = o.o_custkey
            #               inner join nation as n
            #               on n.n_nationkey = c.c_nationkey
            #               where o.o_confirmed = false
            #               and o.o_orderstatus <> 'O'"
            #         runTest.run_query(bc, drill, query, queryId, queryType,
            #            worder, "o_custkey", non_aceptable_diff,
            #            use_percentage, fileSchemaType)
            #
            #         queryId = 'TEST_17'
            #         query = """select count(o_custkey), max(o_totalprice),
            #               min(o_totalprice), sum(o_totalprice)
            #               /count(o_totalprice)
            #               from bool_orders group by o_totalprice,
            #               o_custkey, o_orderkey order by o_totalprice,
            #                    o_custkey, o_orderkey desc"""
            #         runTest.run_query(bc, drill, query, queryId, queryType,
            #            0, '', min_aceptable_diff, True)

            queryId = "TEST_18"
            query = """select o_orderkey, o_confirmed from bool_orders
                        where o_confirmed IS TRUE
                        order by o_orderkey limit 15"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_19"
            query = """select o_orderkey, o_confirmed from bool_orders
                        where o_confirmed IS FALSE
                        order by o_orderkey limit 15"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_20"
            query = """select o_orderkey, o_confirmed from bool_orders
                        where o_confirmed IS NOT TRUE
                        order by o_orderkey limit 15"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_21"
            query = """select o_orderkey, o_confirmed from bool_orders
                        where o_confirmed IS NOT FALSE
                        order by o_orderkey limit 15"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_22"
            query = """select o_orderkey, o_confirmed from bool_orders
                        where o_confirmed IS NOT FALSE and o_confirmed IS FALSE
                        order by o_orderkey limit 15"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_23"
            query = """select count(*) from bool_orders where o_confirmed IS TRUE"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_24"
            query = """select count(*) from bool_orders where o_confirmed IS FALSE"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            # considers both `False` and nulls
            queryId = "TEST_25"
            query = """select count(*) from bool_orders where o_confirmed IS NOT TRUE"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
                use_percentage,
                fileSchemaType,
            )

            # considers both `True` and nulls
            queryId = "TEST_26"
            query = """select count(*) from bool_orders where o_confirmed IS NOT FALSE"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                min_aceptable_diff,
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
        cs.init_drill_schema(
            drill, Settings.data["TestSettings"]["dataDirectory"],
            bool_test=True
        )

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
