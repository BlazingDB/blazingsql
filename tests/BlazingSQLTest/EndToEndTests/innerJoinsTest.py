from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution,  gpuMemory, init_context, skip_test

queryType = "Inner join"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation", "region", "customer", "lineitem",
                  "orders", "supplier"]
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
            query = """select nation.n_nationkey, region.r_regionkey
                    from nation inner join region
                    on region.r_regionkey = nation.n_nationkey"""
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
            query = """select avg(CAST(c.c_custkey AS DOUBLE)),
                        avg(CAST(c.c_nationkey AS DOUBLE)),
                        n.n_regionkey
                    from customer as c inner join nation as n
                    on c.c_nationkey = n.n_nationkey
                    group by n.n_regionkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                0.01,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_03"
            query = """select c.c_custkey, c.c_nationkey, n.n_regionkey
                    from customer as c
                    inner join nation as n
                    on c.c_nationkey = n.n_nationkey
                    where n.n_regionkey = 1
                    and c.c_custkey < 50"""
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
            query = """select avg(CAST(c.c_custkey AS DOUBLE)),
                        avg(c.c_acctbal), n.n_nationkey,
                        r.r_regionkey
                    from customer as c
                    inner join nation as n on c.c_nationkey = n.n_nationkey
                    inner join region as r on r.r_regionkey = n.n_regionkey
                    group by n.n_nationkey, r.r_regionkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                0.01,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_05"
            query = """select n1.n_nationkey as supp_nation,
                        n2.n_nationkey as cust_nation,
                        l.l_extendedprice * l.l_discount
                    from supplier as s
                    inner join lineitem as l
                    on s.s_suppkey = l.l_suppkey
                    inner join orders as o on o.o_orderkey = l.l_orderkey
                    inner join customer as c on c.c_custkey = o.o_custkey
                    inner join nation as n1 on s.s_nationkey = n1.n_nationkey
                    inner join nation as n2 on c.c_nationkey = n2.n_nationkey
                    where n1.n_nationkey = 1
                    and n2.n_nationkey = 2
                    and o.o_orderkey < 10000"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "",
                0.01,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_06"
            query = """SELECT n.n_nationkey + 1, n.n_regionkey from nation
                    AS n inner join region AS r ON
                    n.n_regionkey = r.r_regionkey"""
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
            query = """SELECT n.n_nationkey + 1, n.n_regionkey from nation
                    AS n INNER JOIN region AS r
                    ON n.n_regionkey = r.r_regionkey
                    and n.n_nationkey = 5"""
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
            query = """select * from nation n1 inner join nation n2
                    on n1.n_nationkey = n2.n_nationkey"""
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
            query = """select n1.n_nationkey, n2.n_nationkey
                    from nation n1 inner join nation n2
                    on n1.n_nationkey = n2.n_nationkey"""
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
            query = """select l.l_orderkey, l.l_linenumber, n.n_nationkey
                    from lineitem as l inner join nation as n
                    on l.l_orderkey = n.n_nationkey"""
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
            query = """select c.c_custkey, c.c_nationkey, n.o_orderkey
                    from customer as c
                    inner join orders as n on c.c_custkey = n.o_custkey
                    where n.o_orderkey < 100"""
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
            query = """select c.c_custkey, c.c_nationkey, o.o_orderkey
                    from customer as c
                    inner join orders as o on c.c_custkey = o.o_custkey
                    inner join nation as n on c.c_nationkey = n.n_nationkey
                    order by c_custkey, o.o_orderkey"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                0,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            #ERROR: Different values GDF and PSV
            # queryId = "TEST_13"
            # query = """select c.c_name, o.o_orderkey, o.o_totalprice,
            #             l.l_partkey, l.l_returnflag
            #         from lineitem as l
            #         inner join orders as o on o.o_orderkey = l.l_orderkey
            #         inner join customer as c on c.c_custkey = o.o_custkey
            #         and l.l_linenumber < 3 and c.c_custkey < 30"""
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
            #     print_result=True,
            # )

            #ERROR: Different values GDF and PSV
            # queryId = "TEST_14"
            # query = """select o.o_orderkey, o.o_totalprice, l.l_partkey
            #         from lineitem as l
            #         inner join orders as o on o.o_orderkey = l.l_orderkey * 2
            #         inner join customer as c on c.c_nationkey = o.o_custkey"""
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
            #     print_result=True,
            # )

            # These tests are not validated because engines like `pyspark` and `drill`
            # do not consider unsigned types
            # These tests are just to verify that in C++ layer, unsigned types are enabled.

            # most of these numbers are limit of different int (or unsigned int) types
            data = [0, 127, 128, 255, 256, 32767, 32758, 65535, 65536, -5]

            import cudf

            try:
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

                # Let's apply `union` statement for all its dtypes 
                query_join_1 = """select * from table_unsig inner join
                                    table_sig on col_uint8 = col_int8"""
                bc.sql(query_join_1)

                query_join_2 = """select * from table_unsig inner join
                                    table_sig on col_uint16 = col_int8"""
                bc.sql(query_join_2)

                query_join_3 = """select * from table_unsig inner join
                                    table_sig on col_uint32 = col_int8"""
                bc.sql(query_join_3)

                query_join_4 = """select * from table_unsig inner join
                                    table_sig on col_uint64 = col_int8"""
                bc.sql(query_join_4)

                query_join_5 = """select * from table_unsig inner join
                                    table_sig on col_uint8 = col_int16"""
                bc.sql(query_join_5)

                query_join_6 = """select * from table_unsig inner join
                                    table_sig on col_uint16 = col_int16"""
                bc.sql(query_join_6)

                query_join_7 = """select * from table_unsig inner join
                                    table_sig on col_uint32 = col_int16"""
                bc.sql(query_join_7)

                query_join_8 = """select * from table_unsig inner join
                                    table_sig on col_uint64 = col_int16"""
                bc.sql(query_join_8)

                query_join_9 = """select * from table_unsig inner join
                                    table_sig on col_uint8 = col_int32"""
                bc.sql(query_join_9)

                query_join_10 = """select * from table_unsig inner join
                                    table_sig on col_uint16 = col_int32"""
                bc.sql(query_join_10)

                query_join_11 = """select * from table_unsig inner join
                                    table_sig on col_uint32 = col_int32"""
                bc.sql(query_join_11)

                query_join_12 = """select * from table_unsig inner join
                                    table_sig on col_uint64 = col_int32"""
                bc.sql(query_join_12)

                query_join_13 = """select * from table_unsig inner join
                                    table_sig on col_uint8 = col_int64"""
                bc.sql(query_join_13)

                query_join_14 = """select * from table_unsig inner join
                                    table_sig on col_uint16 = col_int64"""
                bc.sql(query_join_14)

                query_join_15 = """select * from table_unsig inner join
                                    table_sig on col_uint32 = col_int64"""
                bc.sql(query_join_15)

                query_join_16 = """select * from table_unsig inner join
                                  table_sig on col_uint64 = col_int64"""
                bc.sql(query_join_16)

            except Exception as e:
                raise e

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
