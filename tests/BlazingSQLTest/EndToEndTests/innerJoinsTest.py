from DataBase import createSchema as cs
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution
from blazingsql import BlazingContext
from Utils import gpuMemory, test_name, skip_test, init_context
from pynvml import *
from blazingsql import DataType
from Utils import dquery
from Configuration import ExecutionMode

queryType = 'Inner join'

def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["nation", "region", "customer", "lineitem", "orders", "supplier"]
        data_types =  [DataType.DASK_CUDF, DataType.CUDF, DataType.CSV, DataType.ORC, DataType.PARQUET] # TODO json

        #Create Tables ------------------------------------------------------------------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType): continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            #Run Query -----------------------------------------------------------------------------
            worder = 1 #Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0

            print('==============================')
            print(queryType)
            print('==============================')

            queryId = 'TEST_01'
            query = "select nation.n_nationkey, region.r_regionkey from nation inner join region on region.r_regionkey = nation.n_nationkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_02'
            query = "select avg(c.c_custkey), avg(CAST(c.c_nationkey AS DOUBLE)), n.n_regionkey from customer as c inner join nation as n on c.c_nationkey = n.n_nationkey group by n.n_regionkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.01, use_percentage, fileSchemaType)

            queryId = 'TEST_03'
            query = "select c.c_custkey, c.c_nationkey, n.n_regionkey from customer as c inner join nation as n on c.c_nationkey = n.n_nationkey where n.n_regionkey = 1 and c.c_custkey < 50"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_04'
            query = """select avg(CAST(c.c_custkey AS DOUBLE)), avg(c.c_acctbal), n.n_nationkey, r.r_regionkey from customer as c 
                inner join nation as n on c.c_nationkey = n.n_nationkey 
                inner join region as r on r.r_regionkey = n.n_regionkey group by n.n_nationkey, r.r_regionkey"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.01, use_percentage, fileSchemaType)

            queryId = 'TEST_05'
            query = """select n1.n_nationkey as supp_nation, n2.n_nationkey as cust_nation, l.l_extendedprice * l.l_discount from supplier as s 
            inner join lineitem as l on s.s_suppkey = l.l_suppkey inner join orders as o on o.o_orderkey = l.l_orderkey 
            inner join customer as c on c.c_custkey = o.o_custkey inner join nation as n1 on s.s_nationkey = n1.n_nationkey 
            inner join nation as n2 on c.c_nationkey = n2.n_nationkey 
            where n1.n_nationkey = 1 and n2.n_nationkey = 2 and o.o_orderkey < 10000"""
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', 0.01, use_percentage, fileSchemaType)

            queryId = 'TEST_06'
            query = "SELECT n.n_nationkey + 1, n.n_regionkey from nation AS n inner join region AS r ON n.n_regionkey = r.r_regionkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_07'
            query = "SELECT n.n_nationkey + 1, n.n_regionkey from nation AS n INNER JOIN region AS r ON n.n_regionkey = r.r_regionkey and n.n_nationkey = 5"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_08'
            query = "select * from nation n1 inner join nation n2 on n1.n_nationkey = n2.n_nationkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_09'
            query = "select n1.n_nationkey, n2.n_nationkey from nation n1 inner join nation n2 on n1.n_nationkey = n2.n_nationkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_10'
            query = "select l.l_orderkey, l.l_linenumber, n.n_nationkey from lineitem as l inner join nation as n on l.l_orderkey = n.n_nationkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break
          
    executionTest()
    
    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
    
if __name__ == '__main__':

    Execution.getArgs()
    
    nvmlInit()

    drill = "drill" #None

    compareResults = True
    if 'compare_results' in Settings.data['RunSettings']:
        compareResults = Settings.data['RunSettings']['compare_results'] 

    if (Settings.execution_mode == ExecutionMode.FULL and compareResults == "true") or Settings.execution_mode == ExecutionMode.GENERATOR:
        # Create Table Drill ------------------------------------------------------------------------------------------------------
        print("starting drill")
        from pydrill.client import PyDrill
        drill = PyDrill(host = 'localhost', port = 8047)
        cs.init_drill_schema(drill, Settings.data['TestSettings']['dataDirectory'])

    #Create Context For BlazingSQL
    
    bc, dask_client = init_context()

    nRals = Settings.data['RunSettings']['nRals']

    main(dask_client, drill, Settings.data['TestSettings']['dataDirectory'], bc, nRals)
    
    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()