from DataBase import createSchema as cs
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution
from blazingsql import BlazingContext
from Utils import gpuMemory, test_name, skip_test, init_context
from pynvml import *
from blazingsql import DataType
from Configuration import ExecutionMode

queryType = 'Union' 

def main(dask_client, drill, spark, dir_data_file, bc, nRals):
    
    start_mem = gpuMemory.capture_gpu_memory_usage()
    
    def executionTest(): 
        tables = ["orders"]
        data_types =  [DataType.DASK_CUDF, DataType.CUDF, DataType.CSV, DataType.ORC, DataType.PARQUET] # TODO json

        #Create Tables ------------------------------------------------------------------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType): continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)
            
            #Run Query -----------------------------------------------------------------------------
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01
            
            print('==============================')
            print(queryType)
            print('==============================')
            
            queryId = 'TEST_01'
            query = "(select o_orderkey, o_custkey from orders where o_orderkey < 100) union all (select o_orderkey, o_custkey from orders where o_orderkey < 300 and o_orderkey >= 200)"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "o_orderkey", acceptable_difference, use_percentage, fileSchemaType)
        
            queryId = 'TEST_02'
            query = "(select o_orderkey, o_custkey from orders where o_orderkey < 100) union all (select o_orderkey, o_custkey from orders where o_orderkey < 300 and o_orderkey >= 200) order by 2"
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "o_orderkey", acceptable_difference, use_percentage, fileSchemaType)
        
            queryId = 'TEST_03'
            query = "(select o_orderkey, o_totalprice as key from orders where o_orderkey < 100) union all (select o_orderkey, o_custkey as keyy from orders where o_orderkey < 300 and o_orderkey >= 200) "
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "o_orderkey", acceptable_difference, use_percentage, fileSchemaType)
            
            queryId = 'TEST_04'
            query =       """(select o_orderkey, null as keyy, o_totalprice, cast(null as int) as o_totalprice2, null as field5, null as field6 from orders where o_orderkey < 100) union all 
                             (select o_orderkey + 100.1 as o_orderkey, o_custkey as keyy, null as o_totalprice, o_totalprice as o_totalprice2, null as field5, cast(null as double) as field6 from orders where o_orderkey < 300 and o_orderkey >= 200)"""
            query_spark = """(select 
                                o_orderkey,
                                cast(null as int) as keyy,
                                o_totalprice,
                                cast(null as double) as o_totalprice2,
                                cast(null as int) as field5,
                                cast(null as double) as field6
                              from orders where o_orderkey < 100)
                              union all
                             (select 
                                o_orderkey + 100.1 as o_orderkey,
                                o_custkey as keyy,
                                cast(null as double) as o_totalprice,
                                o_totalprice as o_totalprice2,
                                cast(null as int) as field5,
                                cast(null as double) as field6
                              from orders where o_orderkey < 300 and o_orderkey >= 200)"""
            runTest.run_query(bc, spark, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType, query_spark=query_spark, print_result=True) 

            queryId = 'TEST_05'
            query = """(select                         o_orderkey,             100.1,  o_totalprice, cast(100 as float), 100,   1.1 from orders where o_orderkey < 100) 
            union all  (select  o_orderkey + 100.1 as o_orderkey,   o_custkey as keyy,          10000,      o_totalprice,  101.1, 100 from orders where o_orderkey < 300 and o_orderkey >= 200) """
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "o_orderkey", acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_06'
            query = """(select                       o_orderkey,                o_orderstatus,    o_orderstatus from orders where o_orderkey < 100) 
            union all  (select o_orderkey + 100.1 as o_orderkey, SUBSTRING(o_orderstatus, 2, 4),  'hello work'   from orders where o_orderkey < 300 and o_orderkey >= 200) """
            runTest.run_query(bc, drill, query, queryId, queryType, worder, "o_orderkey", acceptable_difference, use_percentage, fileSchemaType)


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
    spark = "spark"

    compareResults = True
    if 'compare_results' in Settings.data['RunSettings']:
        compareResults = Settings.data['RunSettings']['compare_results'] 

    if (Settings.execution_mode == ExecutionMode.FULL and compareResults == "true") or Settings.execution_mode == ExecutionMode.GENERATOR:
        # Create Table Drill ------------------------------------------------------------------------------------------------------
        from pydrill.client import PyDrill
        drill = PyDrill(host = 'localhost', port = 8047)
        cs.init_drill_schema(drill, Settings.data['TestSettings']['dataDirectory'])

        # Create Table Spark ------------------------------------------------------------------------------------------------------
        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark, Settings.data['TestSettings']['dataDirectory'])

    #Create Context For BlazingSQL
    
    bc, dask_client = init_context()

    nRals = Settings.data['RunSettings']['nRals']

    main(dask_client, drill, Settings.data['TestSettings']['dataDirectory'], bc, nRals)
    
    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()