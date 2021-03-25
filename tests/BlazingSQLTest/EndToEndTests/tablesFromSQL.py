from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from Runner import runTest
from Utils import gpuMemory, skip_test
from EndToEndTests.tpchQueries import get_tpch_query

from .concurrentTest import *

queryType = "TablesFromSQL"

data_types = [
    DataType.MYSQL,
] 

sql_table_filters = {
    "lineitem": "l_quantity < 24",
}

sql_table_batch_sizes = {
    "lineitem": 3000,
} 


def run_queries(bc, dask_client, nRals, drill, dir_data_lc, tables, **kwargs):
    sql_table_filter_map = kwargs.get("sql_table_filter_map", {})
    sql_table_batch_size_map = kwargs.get("sql_table_batch_size_map", {})
    print("######## Starting queries ...########")
    extra_args = {
        "table_names": tables,
        "init_tables": True,
        "ds_types": data_types,
        "sql_table_filter_map": sql_table_filter_map,
        "sql_table_batch_size_map": sql_table_batch_size_map,
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
    extra_args = {
        "sql_table_filter_map": sql_table_filters,
        "sql_table_batch_size_map": sql_table_batch_sizes,
    }
    run_queries(bc, dask_client, nRals, drill, dir_data_lc, tables, **extra_args)


def main(dask_client, drill, dir_data_lc, bc, nRals):
    print("==============================")
    print(queryType)
    print("==============================")

    start_mem = gpuMemory.capture_gpu_memory_usage()
    executionTest(dask_client, drill, dir_data_lc, bc, nRals)
    end_mem = gpuMemory.capture_gpu_memory_usage()
    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
