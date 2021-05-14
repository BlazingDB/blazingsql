from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Interval"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["region", "interval_table", "orders"]
        interval_table_index = tables.index("interval_table")

        data_types = [DataType.CSV] # TODO ORC gdf parquet json

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(
                bc,
                dir_data_file,
                fileSchemaType,
                tables=tables,
                interval_table_index=interval_table_index,
            )

            # Run Query ------------------------------------------------------
            # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            # ======================== INTERVAL: SECONDS ===========================

            queryId = "TEST_01"
            query = """select INTERVAL '4' SECOND from region"""
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
            query = """select INTERVAL '1' SECOND + INTERVAL '3' SECOND from region"""
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

            # NOTE: Drill have a wrong result for this query
            queryId = "TEST_03"
            query = """select CAST(INTERVAL '1' DAY as INTERVAL SECOND) from region"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            queryId = "TEST_04"
            query = """select INTERVAL '15:30' MINUTE TO SECOND from region"""
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
            query = """select INTERVAL '01:10:02' HOUR TO SECOND from region"""
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
            query = """select INTERVAL '2 00:00:00' DAY TO SECOND from region"""
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
            query = """select INTERVAL '2 01:03:10' DAY TO SECOND from region"""
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
            query = """select 900 * INTERVAL '1' SECOND  from region"""
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
            query = """select CAST(date '1990-05-02' + INTERVAL '45' SECOND as date) from region"""
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

            queryId = "TEST_10"
            query = """select timestamp '1990-05-02 05:10:12' + INTERVAL '1' SECOND from region"""
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

            # ======================== INTERVAL: MINUTES ===========================

            queryId = "TEST_11"
            query = """select INTERVAL '4' MINUTE from region"""
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

            queryId = "TEST_12"
            query = """select INTERVAL '1' MINUTE + INTERVAL '3' MINUTE from region"""
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

            # NOTE: Drill have a wrong result for this query
            # queryId = "TEST_13"
            # query = """select CAST(INTERVAL '1' DAY as INTERVAL MINUTE) from region"""
            # runTest.run_query(
            #     bc,
            #     drill,
            #     query,
            #     queryId,
            #     queryType,
            #     worder,
            #     "",
            #     acceptable_difference,
            #     use_percentage,
            #     fileSchemaType,
            # )

            queryId = "TEST_14"
            query = """select INTERVAL '23:15' HOUR(2) TO MINUTE from region"""
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

            queryId = "TEST_15"
            query = """select INTERVAL '123:15' HOUR(3) TO MINUTE from region"""
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

            queryId = "TEST_16"
            query = """select INTERVAL '2 10:40' DAY(1) TO MINUTE from region"""
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

            queryId = "TEST_17"
            query = """select 150 * INTERVAL '1' MINUTE from region"""
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

            queryId = "TEST_18"
            query = """select CAST(date '1990-05-02' + INTERVAL '45' MINUTE AS DATE) from region"""
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

            queryId = "TEST_19"
            query = """select timestamp '1990-05-02 05:10:12' + INTERVAL '1' MINUTE from region"""
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

            # ======================== INTERVAL: HOURS ===========================

            queryId = "TEST_20"
            query = """select INTERVAL '4' HOUR from region"""
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

            queryId = "TEST_21"
            query = """select INTERVAL '1' HOUR + INTERVAL '3' HOUR from region"""
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

            queryId = "TEST_22"
            query = """select INTERVAL '2 10' DAY(1) TO HOUR from region"""
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

            queryId = "TEST_23"
            query = """select INTERVAL '125 10' DAY(3) TO HOUR from region"""
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

            queryId = "TEST_24"
            query = """select 150 * INTERVAL '1' HOUR from region"""
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

            queryId = "TEST_25"
            query = """select cast(date '1990-05-02' + INTERVAL '12' HOUR as date) from region"""
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

            queryId = "TEST_26"
            query = """select cast(date '1990-05-02' + INTERVAL '25' HOUR as date) from region"""
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

            queryId = "TEST_27"
            query = """select timestamp '1990-05-02 05:10:12' + INTERVAL '1' HOUR from region"""
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

            # ======================== INTERVAL: DAYS ===========================

            queryId = "TEST_28"
            query = """select INTERVAL '4' DAY from region"""
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

            queryId = "TEST_29"
            query = """select INTERVAL '1' DAY + INTERVAL '3' DAY from region"""
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

            queryId = "TEST_30"
            query = """select 150 * INTERVAL '1' DAY from region"""
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

            queryId = "TEST_31"
            query = """select cast(date '1990-05-02' + INTERVAL '1' DAY as date) from region"""
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

            queryId = "TEST_32"
            query = """select timestamp '1990-05-02 05:10:12' + INTERVAL '2' DAY from region"""
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

            # ======================== INTERVAL: MONTHS ===========================

            # TODO: For now, not supported MONTH INTERVAL operations

            # ========================= INTERVAL: YEAR ============================

            # TODO: For now, not supported YEAR INTERVAL operations

            # ====================== TIMESTAMP op INTERVAL ========================

            queryId = "TEST_41"
            query = """select o_orderdate, o_orderdate + INTERVAL '3' SECOND as add_sec_col
                        from orders order by o_totalprice limit 100"""
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

            queryId = "TEST_42"
            query = """select o_orderdate, o_orderdate - INTERVAL '2' SECOND as sub_sec_col
                        from orders order by o_totalprice limit 150"""
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

            queryId = "TEST_43"
            query = """select o_orderdate, o_orderdate + INTERVAL '6' MINUTE as add_min_col
                        from orders order by o_totalprice limit 50"""
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

            queryId = "TEST_44"
            query = """select o_orderdate, o_orderdate - INTERVAL '5' MINUTE as sub_min_col
                        from orders order by o_totalprice limit 50"""
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

            queryId = "TEST_45"
            query = """select o_orderdate, o_orderdate + INTERVAL '1' HOUR as add_hour_col
                        from orders order by o_totalprice limit 50"""
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

            queryId = "TEST_46"
            query = """select o_orderdate, o_orderdate - INTERVAL '4' HOUR as sub_hour_col
                        from orders order by o_totalprice limit 85"""
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

            queryId = "TEST_47"
            query = """select o_orderdate, o_orderdate + INTERVAL '1' DAY as add_day_col
                        from orders order by o_totalprice limit 100"""
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

            queryId = "TEST_48"
            query = """select o_orderdate, o_orderdate - INTERVAL '7' DAY as sub_day_col
                        from orders order by o_totalprice limit 50"""
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

            # ========================= DURATION op INTERVAL ============================

            queryId = "TEST_49"
            query = """select * from interval_table"""
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

            queryId = "TEST_50"
            query = """select i_duration_s, i_duration_ms from interval_table"""
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

            queryId = "TEST_51"
            query = """select i_id, i_duration_s from interval_table
                        order by i_duration_s desc limit 25"""
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

            # Join with DURATION same unit
            queryId = "TEST_52"
            query = """select r1.i_id + 1 as r1_id, r1.i_duration_s as r1_duration_s,
                            r2.i_duration_ms as r2_duration_ms
 			            from interval_table r1 inner join interval_table r2
                        on r1.i_duration_s = r2.i_duration_s
                        order by r1.i_id limit 70"""
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

            # Join with DURATION same unit
            queryId = "TEST_53"
            query = """select r1.i_id as r1_id, r1.i_duration_s as r1_duration_s,
                            r1.i_duration_ms as r1_duration_ms
 			            from interval_table r1 inner join interval_table r2
                        on r1.i_duration_ms = r2.i_duration_s"""
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

            queryId = "TEST_54"
            query = """select count(i_duration_s) from interval_table"""
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

            queryId = "TEST_55"
            query = """select sum(i_id) as sum_col, i_duration_s from interval_table
                        group by i_duration_s"""
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

            # union with DURARION same unit
            queryId = "TEST_56"
            query = """(select i_id, i_duration_ms from interval_table where i_id > 236)
                            union all 
                        (select i_id, i_duration_ms from interval_table where i_id < 55)"""
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

            # union with DURARION diff unit
            queryId = "TEST_57"
            query = """(select i_id, i_duration_s from interval_table limit 36)
                            union all 
                        (select i_id, i_duration_ms from interval_table limit 4)"""
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

            # NOTE: all this query works for Blazing but not for Drill
            # queryId = "TEST_58"
            # query = """select i_duration_s, INTERVAL '1' SECOND as col_to_sum,
            #                 i_duration_s + INTERVAL '1' SECOND as sum_cols
            #             from interval_table limit 36"""

            # queryId = "TEST_59"
            # query = """select i_duration_s, INTERVAL '1' MINUTE as col_to_sum,
            #                 i_duration_s + INTERVAL '1' MINUTE as sum_cols
            #             from interval_table limit 36"""

            # queryId = "TEST_60"
            # query = """select i_duration_s, INTERVAL '1' HOUR as col_to_sum,
            #                 i_duration_s + INTERVAL '1' HOUR as sum_cols
            #             from interval_table limit 36"""

            # queryId = "TEST_61"
            # query = """select i_duration_s, INTERVAL '1' DAY as col_to_sum,
            #                 i_duration_s + INTERVAL '1' DAY as sum_cols
            #             from interval_table limit 36"""


            # queryId = "TEST_62"
            # query = """select i_duration_s, INTERVAL '1' SECOND as col_to_sub,
            #                 i_duration_s - INTERVAL '1' SECOND as sub_cols
            #             from interval_table limit 36"""

            # queryId = "TEST_63"
            # query = """select i_duration_s, INTERVAL '1' MINUTE as col_to_sub,
            #                 i_duration_s - INTERVAL '1' MINUTE as sub_cols
            #             from interval_table limit 36"""

            # queryId = "TEST_64"
            # query = """select i_duration_s, INTERVAL '1' HOUR as col_to_sub,
            #                 i_duration_s - INTERVAL '1' HOUR as sub_cols
            #             from interval_table limit 36"""

            # queryId = "TEST_65"
            # query = """select i_duration_s, INTERVAL '1' DAY as col_to_sub,
            #                 i_duration_s - INTERVAL '1' DAY as sub_cols
            #             from interval_table limit 36"""

            # NOTE: This test will crash (Calcite issue -> null:TIME(0))
            """LogicalProject(i_id=[$0], i_duration_s=[null:TIME(0)], i_duration_ms=[$2])
                BindableTableScan(table=[[main, interval_table]], filters=[[IS NULL($1)]])"""
            # queryId = "TEST_66"
            # query = """select * from interval_table where i_duration_s is null"""


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
    #spark = "spark"

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
                             Settings.data["TestSettings"]["dataDirectory"],
                             interval_test=True)

        # Create Table Spark -------------------------------------------------
        #from pyspark.sql import SparkSession
        
        #spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        #cs.init_spark_schema(spark,
        #                     Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(
        dask_client,
        drill,
        #spark,
        Settings.data["TestSettings"]["dataDirectory"],
        bc,
        nRals,
    )

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
