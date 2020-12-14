from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Column Basis"

# TODO percy make possible to run this test when nRals > 1


def main(dask_client, drill, dir_data_file, bc, nRals):
    # TODO percy should be possible to make this test distributed
    if int(nRals) != 1:
        print(queryType + " will run only when nRals = 1")
        return

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["customer", "nation"]
        data_types = [
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

            # Adding plus 3 to a column
            queryId = "TEST_01"

            query = "select c_custkey, c_nationkey, c_acctbal from customer"
            temp_gdf = bc.sql(query)

            temp_gdf["c_custkey"] = temp_gdf["c_custkey"] + 3

            bc.create_table("temp", temp_gdf)

            sql = "select * from main.temp"
            result_gdf = bc.sql(sql)

            query = """select c_custkey + 3, c_nationkey, c_acctbal
                    from customer"""

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
                nested_query=True,
                blz_result=result_gdf,
                print_result=True,
            )

            # Dropping a column
            queryId = "TEST_02"

            query = "select c_custkey, c_nationkey, c_acctbal from customer"

            temp_gdf = bc.sql(query)

            temp_gdf.drop(columns=["c_custkey"], inplace=True)

            bc.create_table("temp2", temp_gdf)

            sql = "select * from main.temp2"
            result_gdf = bc.sql(sql)

            query = "select c_nationkey, c_acctbal from customer"

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
                nested_query=True,
                blz_result=result_gdf,
                print_result=True,
            )

            # Adding plus 3 to a column and dropping another
            queryId = "TEST_03"

            query = """select c_custkey, c_nationkey, c_acctbal
                from customer where c_acctbal > 1000"""
            temp_gdf = bc.sql(query)

            temp_gdf["c_acctbal"] = temp_gdf["c_acctbal"] + 3
            temp_gdf.drop(columns=["c_custkey"], inplace=True)

            bc.create_table("temp3", temp_gdf)

            sql = "select * from main.temp3"
            result_gdf = bc.sql(sql)

            query = (
                """select c_nationkey, c_acctbal + 3
                    from customer where c_acctbal > 1000"""
            )

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
                nested_query=True,
                blz_result=result_gdf,
                print_result=True,
            )

            # Reutilizing blazinsql results twice
            queryId = "TEST_04"

            query = """select c_custkey, c_nationkey, c_acctbal
                    from customer where c_acctbal > 1000"""
            temp_gdf = bc.sql(query)

            temp_gdf["c_acctbal_new"] = temp_gdf["c_acctbal"] + 3

            temp_gdf.drop(columns=["c_acctbal"], inplace=True)
            temp_gdf.drop(columns=["c_custkey"], inplace=True)

            bc.create_table("temp0", temp_gdf)
            sql = "select * from main.temp0"

            temp2_gdf = bc.sql(sql)

            temp2_gdf.drop(columns=["c_nationkey"], inplace=True)

            bc.create_table("temp4", temp2_gdf)
            sql = "select * from main.temp4"

            result_gdf = bc.sql(sql)

            query = """select c_acctbal + 3 as c_acctbal_new
                    from customer where c_acctbal > 1000"""

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
                nested_query=True,
                blz_result=result_gdf,
                print_result=True,
            )

            # Adding another column
            queryId = "TEST_05"

            query = "select c_acctbal from customer where c_acctbal > 1000"
            temp_gdf = bc.sql(query)
            temp_gdf["c_acctbal_new"] = temp_gdf["c_acctbal"] + 3

            bc.create_table("temp5", temp_gdf)
            sql = "select * from main.temp5"

            result_gdf = bc.sql(sql)

            query = """select c_acctbal, c_acctbal + 3 as c_acctbal_new
                    from customer where c_acctbal > 1000"""

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
                nested_query=True,
                blz_result=result_gdf,
                print_result=True,
            )

            queryId = "TEST_06"

            result_gdf = bc.sql(
                """select n_nationkey, n_regionkey from nation
                 where n_nationkey < 0"""
            )

            bc.create_table("results", result_gdf)

            result_gdf1 = bc.sql(
                """select n.n_nationkey, r.n_regionkey
                 from main.nation as n left join main.results as r
                 on n.n_nationkey = r.n_nationkey"""
            )

            query = """select n.n_nationkey, r.n_regionkey from nation as n
            left join (select n_nationkey, n_regionkey from nation
            where n_nationkey < 0) as r on n.n_nationkey = r.n_nationkey"""

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
                nested_query=True,
                blz_result=result_gdf1,
            )

            queryId = "TEST_07"

            # Calling a cudf function that returns the same column
            result_gdf = bc.sql("select n_nationkey, n_regionkey from main.nation")

            result_gdf["n_nationkey"] = result_gdf[
                                            "n_nationkey"].astype("int32")

            bc.create_table("results_tmp", result_gdf)

            result_gdf1 = bc.sql("select * from main.results_tmp")

            query = "select n_nationkey, n_regionkey from nation"

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
                nested_query=True,
                blz_result=result_gdf1,
                print_result=True,
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
