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

    if (Settings.execution_mode == ExecutionMode.FULL_MODE and compareResults == "true") or Settings.execution_mode == ExecutionMode.GENERATOR:

        # Create Table Drill ------------------------------------------------------------------------------------------------------
        from pydrill.client import PyDrill
        drill = PyDrill(host = 'localhost', port = 8047)
        createSchema.init_drill_schema(drill, Settings.data['TestSettings']['dataDirectory'], bool_test=True)

        # Create Table Spark ------------------------------------------------------------------------------------------------------
        spark = SparkSession.builder.appName("allE2ETest").getOrCreate()
        createSchema.init_spark_schema(spark, Settings.data['TestSettings']['dataDirectory'])

    #Create Context For BlazingSQL
    bc, dask_client = init_context()
    
    aggregationsWithoutGroupByTest.main(dask_client, drill, dir_data_file, bc, nRals)
    coalesceTest.main(dask_client, drill, dir_data_file, bc, nRals) #we are not supporting coalesce yet
    columnBasisTest.main(dask_client, drill, dir_data_file, bc, nRals)
    commonTableExpressionsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    #countDistincTest.main(dask_client, drill, dir_data_file, bc) #we are not supporting count distinct yet
    countWithoutGroupByTest.main(dask_client, drill, dir_data_file, bc, nRals)
    dateTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    timestampTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    fullOuterJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    groupByTest.main(dask_client, drill, dir_data_file, bc, nRals)
    GroupByWitoutAggregations.main(dask_client, drill, dir_data_file, bc, nRals)
    innerJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    leftOuterJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    nonEquiJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    #loadDataTest.main(dask_client, bc) #check this
    nestedQueriesTest.main(dask_client, drill, dir_data_file, bc, nRals)
    orderbyTest.main(dask_client, drill, dir_data_file, bc, nRals)
    predicatesWithNulls.main(dask_client, drill, dir_data_file, bc, nRals)
    stringTests.main(dask_client, drill, dir_data_file, bc, nRals)
    tablesFromPandasTest.main(dask_client, drill, dir_data_file, bc, nRals)
    unaryOpsTest.main(dask_client, drill, dir_data_file, bc, nRals)
    unifyTablesTest.main(dask_client, drill, dir_data_file, bc, nRals)
    unionTest.main(dask_client, drill, dir_data_file, bc, nRals)
    useLimitTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    whereClauseTest.main(dask_client, drill, dir_data_file, bc, nRals)

    bindableAliasTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    booleanTest.main(dask_client, drill, dir_data_file, bc, nRals)
    caseTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    castTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    concatTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    literalTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)
    dirTest.main(dask_client, drill, dir_data_file, bc, nRals)
    #fileSystemHdfsTest.main(dask_client, drill, dir_data_file, bc) #HDFS is not working yet
    #mixedFileSystemTest.main(dask_client, drill, dir_data_file, bc) #HDFS is not working yet
    likeTest.main(dask_client, drill, dir_data_file, bc, nRals)
    simpleDistributionTest.main(dask_client, drill, dir_data_file, bc, nRals)
    substringTest.main(dask_client, drill, dir_data_file, bc, nRals)
    wildCardTest.main(dask_client, drill, dir_data_file, bc, nRals)  
    tpchQueriesTest.main(dask_client, drill, dir_data_file, bc, nRals)  
    roundTest.main(dask_client, drill, dir_data_file, bc, nRals)
    fileSystemLocalTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if Settings.execution_mode != ExecutionMode.GPU_CI:        
        fileSystemS3Test.main(dask_client, drill, dir_data_file, bc, nRals)
        #fileSystemGSTest.main(dask_client, drill, dir_data_file, bc, nRals)
    
    #timestampdiffTest.main(dask_client, spark, dir_data_file, bc, nRals)
    
    
    if Settings.execution_mode != ExecutionMode.GENERATOR:

        runTest.save_log()
        
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

if __name__ == '__main__':
    main()
    
    

