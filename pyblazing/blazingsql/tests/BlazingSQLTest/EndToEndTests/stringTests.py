import cudf
import pyblazing

from DataBase import createSchema as cs
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution, test_name, skip_test, init_context
from blazingsql import BlazingContext
from Utils import gpuMemory
from pynvml import *
from blazingsql import DataType
from Utils import dquery
from Configuration import ExecutionMode

queryType = 'Simple String' 

def main(dask_client, drill, dir_data_file, bc, nRals):
    
    start_mem = gpuMemory.capture_gpu_memory_usage()
    
    
    def executionTest(): 
        tables = ["customer", "orders", "nation", "region"]
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
            query = "select o_orderkey, sum(o_totalprice)/count(o_orderstatus) from orders where o_custkey < 100 group by o_orderstatus, o_orderkey"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
         
            queryId = 'TEST_02'
            query = "select o_orderkey, o_orderstatus from orders where o_custkey < 10 and o_orderstatus <> 'O' order by o_orderkey, o_orderstatus limit 50"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
          
            queryId = 'TEST_03'
            query = "select count(o_orderstatus) from orders where o_orderstatus <> 'O'"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)            
        
            queryId = 'TEST_04'
            query = "select count(o_orderkey), sum(o_orderkey), o_clerk from orders where o_custkey < 1000 group by o_clerk, o_orderstatus"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)        
            
            queryId = 'TEST_05'
            query = "select avg(CAST(o_orderkey AS DOUBLE)) from orders group by o_orderstatus"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)    
        
            queryId = 'TEST_06'
            query = "select count(o_shippriority), sum(o_totalprice) from orders group by o_shippriority"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)                   
            
            queryId = 'TEST_07'
            query = """with regionTemp as ( select r_regionkey, r_name from region where r_regionkey > 2 ),
            nationTemp as(select n_nationkey, n_regionkey as fkey, n_name from nation where n_nationkey > 3 order by n_nationkey)
            select regionTemp.r_name, nationTemp.n_name from regionTemp inner join nationTemp on regionTemp.r_regionkey = nationTemp.fkey"""
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

    if (Settings.execution_mode == ExecutionMode.FULL_MODE and compareResults == "true") or Settings.execution_mode == ExecutionMode.GENERATOR:
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
    