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

queryType = 'Group by'

def main(dask_client, drill, dir_data_file, bc, nRals):
    
    start_mem = gpuMemory.capture_gpu_memory_usage()
    
    def executionTest ():
        tables = ["customer", "lineitem", "orders"]
        data_types =  [DataType.DASK_CUDF, DataType.CUDF, DataType.CSV, DataType.ORC, DataType.PARQUET] # TODO json
      
        #Create Tables ------------------------------------------------------------------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType): continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)
            
            #Run Query -----------------------------------------------------------------------------
            worder = 1 #Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.01
            
            print('==============================')
            print(queryType)
            print('==============================')
         
            queryId = 'TEST_01'
            query = "select count(c_custkey), sum(c_acctbal), avg(c_acctbal), min(c_custkey), max(c_nationkey), c_nationkey from customer group by c_nationkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "", acceptable_difference, use_percentage, fileSchemaType, print_result=True)
            
            queryId = 'TEST_02'
            query = "select count(c_custkey), sum(c_acctbal), count(c_acctbal), avg(c_acctbal), min(c_custkey), max(c_custkey), c_nationkey from customer where c_custkey < 50 group by c_nationkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "", acceptable_difference, use_percentage, fileSchemaType, print_result=True)
            
            queryId = 'TEST_03'
            query = "select count(c_custkey) + sum(c_acctbal) + avg(c_acctbal), min(c_custkey) - max(c_nationkey), c_nationkey * 2 as key from customer where  c_nationkey * 2 < 40 group by  c_nationkey * 2"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "", acceptable_difference, use_percentage, fileSchemaType)
            
            queryId = 'TEST_04'
            query = "select c_nationkey, count(c_acctbal) from customer group by c_nationkey, c_custkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "", acceptable_difference, use_percentage, fileSchemaType)

            if fileSchemaType != DataType.ORC: # TODO remove this if when we no longer have issues with ORC and dates
                queryId = 'TEST_05'
                query = """select l.l_suppkey, l.l_linestatus,  min(l.l_commitdate), max(l.l_commitdate),  max(l.l_orderkey), count(l.l_orderkey)  FROM  
                (SELECT l_suppkey, l_linestatus, l_shipmode, l_orderkey, l_commitdate from lineitem WHERE l_linenumber = 6 and l_commitdate < DATE '1993-01-01') AS l 
                LEFT OUTER JOIN orders AS o ON l.l_orderkey + 100 = o.o_orderkey  GROUP BY l.l_suppkey, l.l_linestatus order by l.l_suppkey, l.l_linestatus limit 10000"""
                runTest.run_query(bc, drill, query, queryId, queryType, worder, "", acceptable_difference, use_percentage, fileSchemaType)


            queryId = 'TEST_06'
            query = "select l.l_suppkey, l.l_linestatus, max(l.l_shipmode), max(l.l_orderkey), count(l.l_orderkey)  FROM lineitem AS l LEFT OUTER JOIN orders AS o ON l.l_orderkey + 100 = o.o_orderkey GROUP BY l.l_suppkey, l.l_linestatus"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "", acceptable_difference, use_percentage, fileSchemaType)

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
