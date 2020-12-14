from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test, test_name

testName = "Sum0 Test"


def main(dask_client, drill, dir_data_lc, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():

        # Read Data TPCH------------------------------------------------------

        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
            DataType.JSON
        ]

        for fileSchemaType in data_types:
            queryType = test_name(testName, fileSchemaType)
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_lc, fileSchemaType)

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
            query = """select count(c_custkey) as c1, count(c_acctbal) as c2
                    from customer"""
            algebra = """LogicalAggregate(group=[{}], c1=[COUNT($0)],
                             c2=[COUNT($1)])
            BindableTableScan(table=[[main, customer]],
                 projects=[[0, 5]], aliases=[[c_custkey, c_acctbal]])"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, algebra=algebra)

            queryId = "TEST_02"
            query = """select count(c_custkey), sum(c_acctbal),
                        sum(c_acctbal)/count(c_acctbal), min(c_custkey),
                        max(c_nationkey),
                        (max(c_nationkey) + min(c_nationkey))/2 c_nationkey
                    from customer where c_custkey < 100
                    group by c_nationkey"""
            algebra = """LogicalProject(EXPR$0=[$1], EXPR$1=[$2],
                     EXPR$2=[/($2, $3)], EXPR$3=[$4], EXPR$4=[$5],
                         c_nationkey=[/(+($5, $6), 2)])
            LogicalAggregate(group=[{0}], EXPR$0=[COUNT($1)],
                 EXPR$1=[SUM($2)], agg#2=[COUNT($2)], EXPR$3=[MIN($1)],
                  EXPR$4=[MAX($0)], agg#5=[MIN($0)])
            LogicalProject(c_nationkey=[$1], c_custkey=[$0], c_acctbal=[$2])
            BindableTableScan(table=[[main, customer]],
                filters=[[<($0, 100)]], projects=[[0, 3, 5]],
                 aliases=[[c_custkey, c_nationkey, c_acctbal]])"""
            # TODO: Change sum/count for avg KC
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            # '', acceptable_difference, True, algebra=algebra)

            queryId = "TEST_03"
            query = """select n1.n_nationkey as supp_nation,
                        n2.n_nationkey as cust_nation,
                        l.l_extendedprice * l.l_discount
                    from supplier as s inner join lineitem as l
                    on s.s_suppkey = l.l_suppkey inner join orders as o
                    on o.o_orderkey = l.l_orderkey
                    inner join customer as c on c.c_custkey = o.o_custkey
                    inner join nation as n1 on s.s_nationkey = n1.n_nationkey
                    inner join nation as n2 on c.c_nationkey = n2.n_nationkey
                    where n1.n_nationkey = 1
                    and n2.n_nationkey = 2 and o.o_orderkey < 10000"""
            algebra = """LogicalProject(supp_nation=[$1], cust_nation=[$3],
                         EXPR$2=[$2])
            LogicalJoin(condition=[=($0, $3)], joinType=[inner])
                LogicalProject(c_nationkey=[$1], n_nationkey=[$3], *=[$2])
                LogicalJoin(condition=[=($0, $3)], joinType=[inner])
                    LogicalProject(s_nationkey=[$0], c_nationkey=[$4], *=[$2])
                    LogicalJoin(condition=[=($3, $1)], joinType=[inner])
                        LogicalProject(s_nationkey=[$0], o_custkey=[$4],
                          *=[$2])
                        LogicalJoin(condition=[=($3, $1)], joinType=[inner])
                            LogicalProject(s_nationkey=[$1], l_orderkey=[$2],
                              *=[$4])
                            LogicalJoin(condition=[=($0, $3)],
                              joinType=[inner])
                                BindableTableScan(table=[[main, supplier]],
                                  projects=[[0, 3]],
                                  aliases=[[s_suppkey, s_nationkey]])
                                LogicalProject(l_orderkey=[$0], l_suppkey=[$1],
                                   *=[*($2, $3)])
                                BindableTableScan(table=[[main, lineitem]],
                                  projects=[[0, 2, 5, 6]],
                                   aliases=[[l_orderkey, l_suppkey, *]])
                            BindableTableScan(table=[[main, orders]],
                              filters=[[<($0, 10000)]], projects=[[0, 1]],
                                 aliases=[[o_orderkey, o_custkey]])
                        BindableTableScan(table=[[main, customer]],
                            projects=[[0, 3]],
                             aliases=[[c_custkey, c_nationkey]])
                    BindableTableScan(table=[[main, nation]],
                     filters=[[=($0, 1)]], projects=[[0]],
                      aliases=[[n_nationkey]])
                BindableTableScan(table=[[main, nation]],
                 filters=[[=($0, 2)]], projects=[[0]],
                  aliases=[[n_nationkey]])"""
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
                algebra=algebra,
            )

            queryId = "TEST_04"
            query = """select count(n1.n_nationkey) as n1key,
                        count(n2.n_nationkey) as n2key,
                        count(n2.n_nationkey) as cstar
                    from nation as n1 full outer join nation as n2
                    on n1.n_nationkey = n2.n_nationkey + 6"""
            algebra = """LogicalProject(n1key=[$0], n2key=[$1], cstar=[$1])
            LogicalAggregate(group=[{}], n1key=[COUNT($0)], cstar=[COUNT($1)])
                LogicalProject(n_nationkey=[$0], n_nationkey0=[$1])
                LogicalJoin(condition=[=($0, $2)], joinType=[full])
                    BindableTableScan(table=[[main, nation]],
                     projects=[[0]], aliases=[[n_nationkey]])
                    LogicalProject(n_nationkey=[$0], $f4=[+($0, 6)])
                    BindableTableScan(table=[[main, nation]],
                     projects=[[0]], aliases=[[n_nationkey, $f4]])"""
            # TODO: Change count(n2.n_nationkey) as cstar as count(*)
            # as cstar when it will be supported KC
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, algebra = algebra)

            queryId = "TEST_05"
            query = """select count(o_orderkey), sum(o_orderkey), o_clerk
                    from orders
                    where o_custkey < 1000
                    group by o_clerk, o_orderstatus"""
            algebra = """LogicalProject(EXPR$0=[$2], EXPR$1=[$3],
                                                     o_clerk=[$0])
            LogicalAggregate(group=[{0, 1}], EXPR$0=[COUNT($2)],
                                                EXPR$1=[SUM($2)])
                LogicalProject(o_clerk=[$3], o_orderstatus=[$2],
                                                 o_orderkey=[$0])
                BindableTableScan(table=[[main, orders]],
                    filters=[[<($1, 1000)]], projects=[[0, 1, 2, 6]],
                     aliases=[[o_orderkey, $f1, o_orderstatus, o_clerk]])"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, True, algebra = algebra)

            queryId = "TEST_06"
            # TODO: Change sum/count for avg KC
            query = """select sum(o_orderkey)/count(o_orderkey)
                    from orders group by o_orderstatus"""
            algebra = """LogicalProject(EXPR$0=[/($1, $2)])
            LogicalAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT($1)])
                BindableTableScan(table=[[main, orders]], projects=[[2, 0]],
                                 aliases=[[o_orderstatus, o_orderkey]])"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, True, algebra = algebra)

            queryId = "TEST_07"
            query = """with regionTemp as (
                        select r_regionkey, r_name
                        from region where r_regionkey > 2
                    ), nationTemp as
                    (
                        select n_nationkey, n_regionkey as fkey, n_name
                        from nation where n_nationkey > 3
                        order by n_nationkey
                    )
                    select regionTemp.r_name, nationTemp.n_name
                    from regionTemp inner join nationTemp
                    on regionTemp.r_regionkey = nationTemp.fkey"""
            algebra = """LogicalProject(r_name=[$1], n_name=[$3])
            LogicalJoin(condition=[=($0, $2)], joinType=[inner])
                BindableTableScan(table=[[main, region]],
                        filters=[[>($0, 2)]], projects=[[0, 1]],
                         aliases=[[r_regionkey, r_name]])
                LogicalProject(fkey=[$2], n_name=[$1])
                BindableTableScan(table=[[main, nation]],
                    filters=[[>($0, 3)]], projects=[[0, 1, 2]],
                     aliases=[[n_nationkey, n_name, fkey]])"""
            # runTest.run_query(bc, drill, query, queryId, queryType, worder,
            #  '', acceptable_difference, use_percentage, algebra = algebra)

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break

    executionTest()

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(testName, start_mem, end_mem)


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
