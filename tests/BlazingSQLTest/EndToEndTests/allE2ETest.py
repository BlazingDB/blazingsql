from pyspark.sql import SparkSession
from dask.distributed import Client
from DataBase import createSchema as createSchema
from Configuration import Settings as Settings
from Runner import runTest
from Utils import Execution

from EndToEndTests import  aggregationsWithoutGroupByTest,\
                           unifyTablesTest, useLimitTest

from EndToEndTests import commonTableExpressionsTest
from EndToEndTests import fullOuterJoinsTest as fullOuterJoinsTest
from EndToEndTests import groupByTest as groupByTest
from EndToEndTests import orderbyTest as orderbyTest
from EndToEndTests import whereClauseTest as whereClauseTest
from EndToEndTests import innerJoinsTest as innerJoinsTest
from EndToEndTests import unionTest as unionTest
from EndToEndTests import leftOuterJoinsTest as leftOuterJoinsTest
from EndToEndTests import nonEquiJoinsTest
from EndToEndTests import unaryOpsTest as unaryOpsTest
from EndToEndTests import GroupByWitoutAggregations
from EndToEndTests import countWithoutGroupByTest
from EndToEndTests import coalesceTest as coalesceTest
from EndToEndTests import columnBasisTest as columnBasisTest
from EndToEndTests import nestedQueriesTest
from EndToEndTests import stringTests
from EndToEndTests import dateTest
from EndToEndTests import timestampTest
from EndToEndTests import predicatesWithNulls
#from EndToEndTests import countDistincTest
from EndToEndTests import tablesFromPandasTest
from EndToEndTests import loadDataTest

from EndToEndTests import booleanTest
from EndToEndTests import dirTest
from EndToEndTests import fileSystemLocalTest
from EndToEndTests import fileSystemS3Test
from EndToEndTests import fileSystemGSTest
from EndToEndTests import simpleDistributionTest
from EndToEndTests import wildCardTest
from EndToEndTests import caseTest
from EndToEndTests import bindableAliasTest
from EndToEndTests import castTest
from EndToEndTests import concatTest
from EndToEndTests import likeTest
from EndToEndTests import literalTest
from EndToEndTests import substringTest

from EndToEndTests import tpchQueriesTest
from EndToEndTests import timestampdiffTest
from EndToEndTests import concatTest
from EndToEndTests import roundTest

from Utils import gpuMemory, init_context
from pynvml import *

from blazingsql import BlazingContext

from Configuration import ExecutionMode

def main():
    print('**init end2end**')
    Execution.getArgs()
    nvmlInit()
    dir_data_file = Settings.data['TestSettings']['dataDirectory']
    nRals = Settings.data['RunSettings']['nRals']
    
    drill = "drill"
    spark = "spark"

    compareResults = True
    if 'compare_results' in Settings.data['RunSettings']:
        compareResults = Settings.data['RunSettings']['compare_results'] 

    if (Settings.execution_mode == ExecutionMode.FULL and compareResults == "true") or Settings.execution_mode == ExecutionMode.GENERATOR:

        # Create Table Drill ------------------------------------------------------------------------------------------------------
        from pydrill.client import PyDrill
        drill = PyDrill(host = 'localhost', port = 8047)
        createSchema.init_drill_schema(drill, Settings.data['TestSettings']['dataDirectory'], bool_test=True)

        # Create Table Spark ------------------------------------------------------------------------------------------------------
        spark = SparkSession.builder.appName("allE2ETest").getOrCreate()
        createSchema.init_spark_schema(spark, Settings.data['TestSettings']['dataDirectory'])

    #Create Context For BlazingSQL
    bc, dask_client = init_context()
    
    targetTestGroups = Settings.data['RunSettings']['targetTestGroups']
    runAllTests = (len(targetTestGroups) == 0) # if targetTestGroups was empty the user wants to run all the tests
    
    if runAllTests or ("aggregationsWithoutGroupByTest" in targetTestGroups): 
        aggregationsWithoutGroupByTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("coalesceTest" in targetTestGroups):
        coalesceTest.main(dask_client, drill, dir_data_file, bc, nRals) #we are not supporting coalesce yet
    
    if runAllTests or ("columnBasisTest" in targetTestGroups):
        columnBasisTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("commonTableExpressionsTest" in targetTestGroups):
        commonTableExpressionsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    #countDistincTest.main(dask_client, drill, dir_data_file, bc) #we are not supporting count distinct yet
    
    if runAllTests or ("countWithoutGroupByTest" in targetTestGroups):
        countWithoutGroupByTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("dateTest" in targetTestGroups):
        dateTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("timestampTest" in targetTestGroups):
        timestampTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("fullOuterJoinsTest" in targetTestGroups):
        fullOuterJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("groupByTest" in targetTestGroups):
        groupByTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("GroupByWitoutAggregations" in targetTestGroups):
        GroupByWitoutAggregations.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("innerJoinsTest" in targetTestGroups):
        innerJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("" in targetTestGroups):
        leftOuterJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("nonEquiJoinsTest" in targetTestGroups):
        nonEquiJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    #loadDataTest.main(dask_client, bc) #check this
    
    if runAllTests or ("nestedQueriesTest" in targetTestGroups):
        nestedQueriesTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("orderbyTest" in targetTestGroups):
        orderbyTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("predicatesWithNulls" in targetTestGroups):
        predicatesWithNulls.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("stringTests" in targetTestGroups):
        stringTests.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("tablesFromPandasTest" in targetTestGroups):
        tablesFromPandasTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("unaryOpsTest" in targetTestGroups):
        unaryOpsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("unifyTablesTest" in targetTestGroups):
        unifyTablesTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("unionTest" in targetTestGroups):
        unionTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("useLimitTest" in targetTestGroups):
        useLimitTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("whereClauseTest" in targetTestGroups):
        whereClauseTest.main(dask_client, drill, dir_data_file, bc, nRals)
 
    if runAllTests or ("bindableAliasTest" in targetTestGroups):
        bindableAliasTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("booleanTest" in targetTestGroups):
        booleanTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("caseTest" in targetTestGroups):
        caseTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("castTest" in targetTestGroups):
        castTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("concatTest" in targetTestGroups):
        concatTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("literalTest" in targetTestGroups):
        literalTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("dirTest" in targetTestGroups):
        dirTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    #fileSystemHdfsTest.main(dask_client, drill, dir_data_file, bc) #HDFS is not working yet
    
    #mixedFileSystemTest.main(dask_client, drill, dir_data_file, bc) #HDFS is not working yet
    
    if runAllTests or ("likeTest" in targetTestGroups):
        likeTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("simpleDistributionTest" in targetTestGroups):
        simpleDistributionTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("substringTest" in targetTestGroups):
        substringTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("wildCardTest" in targetTestGroups):
        wildCardTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("tpchQueriesTest" in targetTestGroups):  
        tpchQueriesTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    
    if runAllTests or ("roundTest" in targetTestGroups):  
        roundTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    if runAllTests or ("fileSystemLocalTest" in targetTestGroups):
        fileSystemLocalTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if Settings.execution_mode != ExecutionMode.GPUCI:
        if runAllTests or ("fileSystemS3Test" in targetTestGroups):
            fileSystemS3Test.main(dask_client, drill, dir_data_file, bc, nRals)
        
        if runAllTests or ("fileSystemGSTest" in targetTestGroups):
            fileSystemGSTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    #timestampdiffTest.main(dask_client, spark, dir_data_file, bc, nRals)
    
    
    if Settings.execution_mode != ExecutionMode.GENERATOR:

        result, error_msgs = runTest.save_log()
        
        max = 0 
        for i in range(0, len(Settings.memory_list)):
            if (Settings.memory_list[i].delta) > max:
                max = Settings.memory_list[i].delta
        
        print("MAX DELTA: " + str(max))
        print('*******************************************************************************')
            
        for i in range(0, len(Settings.memory_list)):
            print(Settings.memory_list[i].name + ":" +
                "   Start Mem: " + str(Settings.memory_list[i].start_mem) +
                "   End Mem: " + str(Settings.memory_list[i].end_mem) + 
                "   Diff: " + str(Settings.memory_list[i].delta))

        return result, error_msgs

    return True, []

if __name__ == '__main__':
    import time

    start = time.time() # in seconds

    result, error_msgs = main()
    
    if Settings.execution_mode != ExecutionMode.GENERATOR:
        # NOTE kahro william percy mario : here we tell to gpuci there was an error comparing with historic results
        # TODO william kharoly felipe we should try to enable and use this function in the future
        result = True
        if result == False:
            for error_msg in error_msgs:
                print(error_msg)
            import sys
            
            end = time.time() # in seconds
            elapsed = end - start # in seconds
            
            time_delta_desc = str(elapsed/60) + " minutes and " + str(int(elapsed) % 60) + " seconds"
            print("==>> E2E FAILED against previous run, total time was: " + time_delta_desc)
            
            # TODO percy kharo willian: uncomment this line when gpuci has all the env vars set
            #sys.exit(1) # return error exit status to the command prompt (shell)
