from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "unsignedType"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():

        # Run Query ------------------------------------------------------

        print("==============================")
        print(queryType)

        print("==============================")

        # These tests are not validated because engines like `pyspark` and `drill`
        # do not consider unsigned types
        # These tests are just to verify that in C++ layer, unsigned types are enabled.

        # most of these numbers are limit of different int (or unsigned int) types
        data = [0, 127, 128, 255, 256, 32767, 32758, 65535, 65536, -5]

        import cudf

        # DataFrame with unsigned types
        df_u = cudf.DataFrame({'col_orig': data})
        df_u['col_uint8'] = df_u['col_orig'].astype('uint8')
        df_u['col_uint16'] = df_u['col_orig'].astype('uint16')
        df_u['col_uint32'] = df_u['col_orig'].astype('uint32')
        df_u['col_uint64'] = df_u['col_orig'].astype('uint64')

        df = cudf.DataFrame({'col_orig': data})
        df['col_int8'] = df['col_orig'].astype('int8')
        df['col_int16'] = df['col_orig'].astype('int16')
        df['col_int32'] = df['col_orig'].astype('int32')
        df['col_int64'] = df['col_orig'].astype('int64')

        bc.create_table('table_unsig', df_u)
        bc.create_table('table_sig', df)

        # ====================== Inner Test ======================

        try:

            query_join_1 = """select * from table_unsig inner join
                                table_sig on col_uint8 = col_int8"""
            bc.sql(query_join_1)

            query_join_2 = """select col_int8 + col_uint8 from table_unsig inner join
                                table_sig on col_uint16 = col_int8"""
            bc.sql(query_join_2)

            query_join_3 = """select col_int8 + col_uint16 from table_unsig inner join
                                table_sig on col_uint32 = col_int8"""
            bc.sql(query_join_3)

            query_join_4 = """select col_int8 - col_uint32 from table_unsig inner join
                                table_sig on col_uint64 = col_int8"""
            bc.sql(query_join_4)

            query_join_5 = """select col_int8 + col_uint64 from table_unsig inner join
                                table_sig on col_uint8 = col_int16"""
            bc.sql(query_join_5)

            query_join_6 = """select col_int16 * col_uint8 from table_unsig inner join
                                table_sig on col_uint16 = col_int16"""
            bc.sql(query_join_6)

            query_join_7 = """select col_int16 - col_uint16 from table_unsig inner join
                                table_sig on col_uint32 = col_int16"""
            bc.sql(query_join_7)

            query_join_8 = """select col_int16 - col_uint32 from table_unsig inner join
                                table_sig on col_uint64 = col_int16"""
            bc.sql(query_join_8)

            query_join_9 = """select col_int16 + col_uint64 from table_unsig inner join
                                table_sig on col_uint8 = col_int32"""
            bc.sql(query_join_9)

            query_join_10 = """select col_int32 + col_uint8 from table_unsig inner join
                                table_sig on col_uint16 = col_int32"""
            bc.sql(query_join_10)

            query_join_11 = """select col_int32 * col_uint16 from table_unsig inner join
                                table_sig on col_uint32 = col_int32"""
            bc.sql(query_join_11)

            query_join_12 = """select col_int32 - col_uint32 from table_unsig inner join
                                table_sig on col_uint64 = col_int32"""
            bc.sql(query_join_12)

            query_join_13 = """select col_int32 + col_uint64 from table_unsig inner join
                                table_sig on col_uint8 = col_int64"""
            bc.sql(query_join_13)

            query_join_14 = """select col_int64 - col_uint8 from table_unsig inner join
                                table_sig on col_uint16 = col_int64"""
            bc.sql(query_join_14)

            query_join_15 = """select col_int64 - col_uint32 from table_unsig inner join
                                table_sig on col_uint32 = col_int64"""
            bc.sql(query_join_15)

            query_join_16 = """select col_int64 - col_uint16 from table_unsig inner join
                                table_sig on col_uint64 = col_int64"""
            bc.sql(query_join_16)

        except Exception as e:
            print("SQL crash in UnsignedTypeTest performing a JOIN.")
            raise e

        # ====================== Union Test ======================

        try:

            query_union_1 = """(select * from table_unsig)
                                union all
                                (select * from table_sig)"""
            bc.sql(query_union_1)

            query_union_2 = """(select col_uint8 from table_unsig)
                                union all
                                (select col_int8 from table_sig)"""
            bc.sql(query_union_2)

            query_union_3 = """(select col_uint8 from table_unsig)
                                union all
                                (select col_int16 from table_sig)"""
            bc.sql(query_union_3)

            query_union_4 = """(select col_uint8 from table_unsig)
                                union all
                                (select col_int32 from table_sig)"""
            bc.sql(query_union_4)

            query_union_5 = """(select col_uint8 from table_unsig)
                                union all
                                (select col_int64 from table_sig)"""
            bc.sql(query_union_5)

            query_union_6 = """(select col_uint16 from table_unsig)
                                union all
                                (select col_int8 from table_sig)"""
            bc.sql(query_union_6)

            query_union_7 = """(select col_uint16 from table_unsig)
                                union all
                                (select col_int16 from table_sig)"""
            bc.sql(query_union_7)

            query_union_8 = """(select col_uint16 from table_unsig)
                                union all
                                (select col_int32 from table_sig)"""
            bc.sql(query_union_8)

            query_union_9 = """(select col_uint16 from table_unsig)
                                union all
                                (select col_int64 from table_sig)"""
            bc.sql(query_union_9)

            query_union_10 = """(select col_uint32 from table_unsig)
                                union all
                                (select col_int8 from table_sig)"""
            bc.sql(query_union_10)

            query_union_11 = """(select col_uint32 from table_unsig)
                                union all
                                (select col_int16 from table_sig)"""
            bc.sql(query_union_11)

            query_union_12 = """(select col_uint32 from table_unsig)
                                union all
                                (select col_int32 from table_sig)"""
            bc.sql(query_union_12)

            query_union_13 = """(select col_uint32 from table_unsig)
                                union all
                                (select col_int64 from table_sig)"""
            bc.sql(query_union_13)

            query_union_14 = """(select col_uint64 from table_unsig)
                                union all
                                (select col_int8 from table_sig)"""
            bc.sql(query_union_14)

            query_union_15 = """(select col_uint64 from table_unsig)
                                union all
                                (select col_int16 from table_sig)"""
            bc.sql(query_union_15)

            query_union_16 = """(select col_uint64 from table_unsig)
                                union all
                                (select col_int32 from table_sig)"""
            bc.sql(query_union_16)

            query_union_17 = """(select col_uint64 from table_unsig)
                                union all
                                (select col_int64 from table_sig)"""
            bc.sql(query_union_17)

        except Exception as e:
            print("SQL crash in UnsignedTypeTest performing an UNION.")
            raise e

        # ====================== Column Operations Test ======================

        try:

            query_col_op_1 = """select col_int8 + col_int16, col_int8 + col_int32,
                                    col_int8 + col_int64 from table_sig"""
            bc.sql(query_col_op_1)

            query_col_op_2 = """select col_int16 + col_int32, col_int16 + col_int64
                                 from table_sig"""
            bc.sql(query_col_op_2)

            query_col_op_3 = """select col_int16 * col_int32, col_int16 - col_int64
                                 from table_sig where col_int16 < 145866"""
            bc.sql(query_col_op_3)

            query_col_op_4 = """select col_int8 * col_int64, col_int32 - col_int16
                                 from table_sig where col_int8 < 86"""
            bc.sql(query_col_op_4)

            query_col_op_5 = """select col_int8 + col_int32, col_int64 - col_int16
                                 from table_sig where col_int32 < col_int16"""
            bc.sql(query_col_op_5)

            query_col_op_6 = """select col_int8 * col_int64, col_int32 - col_int16
                                 from table_sig where col_int16 <= col_int8"""
            bc.sql(query_col_op_6)

            query_col_op_7 = """select col_int8 * col_int16, col_int32 - col_int18
                                 from table_sig where col_int16 <= col_int8"""
            bc.sql(query_col_op_7)

            query_col_op_8 = """select col_uint64 + col_uint32, col_uint32 - col_uint16
                                 from table_unsig where col_uint16 <= col_uint8"""
            bc.sql(query_col_op_8)

            query_col_op_9 = """select col_uint32 - col_uint64, col_uint64 - col_uint16
                                 from table_unsig where col_uint16 <= col_uint8"""
            bc.sql(query_col_op_9)

            query_col_op_10 = """select col_uint8 * col_uint32, col_uint8 - col_uint16
                                 from table_unsig where col_uint8 < 86"""
            bc.sql(query_col_op_10)

        except Exception as e:
            print("SQL crash in UnsignedTypeTest performing Column Operations.")
            raise e

        if Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")

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
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client,
        drill,
        Settings.data["TestSettings"]["dataDirectory"],
        bc,
        nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
