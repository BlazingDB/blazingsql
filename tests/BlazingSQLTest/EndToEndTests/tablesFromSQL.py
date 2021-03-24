from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from Runner import runTest
from Utils import gpuMemory, skip_test
from EndToEndTests.tpchQueries import get_tpch_query

from .concurrentTest import *

queryType = "TablesFromSQL"

# {csv: {tb1: tb1_csv, ...}, parquet: {tb1: tb1_parquet, ...}}
datasource_tables = dict((ds, dict((t, t+"_"+str(ds).split(".")[1]) for t in tables)) for ds in data_types)

def run_queries(bc, dask_client, nRals, drill, dir_data_lc, tables):
    print("######## Starting queries ...########")
    done = {} # sampleId -> True|False (fetch completed?)
    extra_args = {
        "dir_data_lc": dir_data_lc,
        "tables": tables,
        "init_tables": True
    }
    currrentFileSchemaType = data_types[0]
    for sampleId, query, queryId, fileSchemaType in samples(bc, dask_client, nRals, **extra_args):
        datasourceDone = (fileSchemaType != currrentFileSchemaType)
        if datasourceDone and Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break_flag = True
            break

        print("==>> Run query for sample", sampleId)
        result_gdf = bc.sql(query)
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
        currrentFileSchemaType = fileSchemaType


def executionTest(dask_client, drill, dir_data_lc, bc, nRals):
    run_queries(bc, dask_client, nRals, drill, dir_data_lc, tables)

def main(dask_client, drill, dir_data_lc, bc, nRals):
    print("==============================")
    print(queryType)
    print("==============================")

    start_mem = gpuMemory.capture_gpu_memory_usage()
    executionTest(dask_client, drill, dir_data_lc, bc, nRals)
    end_mem = gpuMemory.capture_gpu_memory_usage()
    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
