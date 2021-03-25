from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from Runner import runTest
from Utils import gpuMemory, skip_test
from EndToEndTests.tpchQueries import get_tpch_query

queryType = "Concurrent"
data_types = [
    DataType.DASK_CUDF,
    DataType.CSV,
    DataType.PARQUET,
]  # TODO json
tables = [
    "nation",
    "region",
    "customer",
    "lineitem",
    "orders",
    "supplier",
    "part",
    "partsupp",
]
tpch_queries = [
    "TEST_13",
    "TEST_07",
    "TEST_12",
    "TEST_04",
    "TEST_01",
]
# Parameter to indicate if its necessary to order
# the resulsets before compare them
worder = 1
use_percentage = False
acceptable_difference = 0.01

def start_query(bc, tokens, query, sampleId):
    print("==>> Start query for sample", sampleId)
    token = bc.sql(query, return_token = True)
    tokens[sampleId] = token


def fetch_result(bc, tokens, sampleId, drill, query, queryId, fileSchemaType):
    token = tokens[sampleId]
    if bc.status(token):
        print("==>> Fetch result for sample", sampleId)
        result_gdf = bc.fetch(token)
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
            blz_result=result_gdf,
        )
        return True
    return False


def datasources(dask_client, nRals, ds_types):
    for fileSchemaType in ds_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue
        yield fileSchemaType


def samples(bc, dask_client, nRals, **kwargs):
    init_tables = kwargs.get("init_tables", False)
    dir_data_lc = kwargs.get("dir_data_lc", "")
    table_names = kwargs.get("table_names", [])
    ds_types = kwargs.get("ds_types", [])
    sql_table_filter_map = kwargs.get("sql_table_filter_map", {})
    sql_table_batch_size_map = kwargs.get("sql_table_batch_size_map", {})

    # {csv: {tb1: tb1_csv, ...}, parquet: {tb1: tb1_parquet, ...}}
    ds_tables = dict((ds, dict((t, t+"_"+str(ds).split(".")[1]) for t in table_names)) for ds in ds_types)

    for fileSchemaType in datasources(dask_client, nRals, ds_types):
        dstables = ds_tables[fileSchemaType]
        if init_tables:
            print("Creating tables for", str(fileSchemaType))
            cs.create_tables(bc, dir_data_lc, fileSchemaType,
                tables = table_names,
                table_names = list(dstables.values()),
                sql_table_filter_map = sql_table_filter_map,
                sql_table_batch_size_map = sql_table_batch_size_map
            )
            print("All tables were created for", str(fileSchemaType))

        i = 0
        queries = [get_tpch_query(q, dstables) for q in tpch_queries]
        for query in queries:
            i = i + 1
            istr = str(i) if i > 10 else "0"+str(i)
            queryId = "TEST_" + istr
            sampleId = str(fileSchemaType) + "." + queryId
            yield sampleId, query, queryId, fileSchemaType


def start_queries(bc, dask_client, nRals, tokens, dir_data_lc, tables):
    print("######## Starting queries ...########")
    done = {} # sampleId -> True|False (fetch completed?)
    extra_args = {
        "dir_data_lc": dir_data_lc,
        "table_names": tables,
        "init_tables": True,
        "ds_types": data_types,
    }
    for sampleId, query, _, _ in samples(bc, dask_client, nRals, **extra_args):
        start_query(bc, tokens, query, sampleId)
        done[sampleId] = False
    return done


def fetch_results(bc, dask_client, nRals, tokens, drill, done):
    print("######## Fetching retults ...########")
    partial_done = dict((fileSchemaType, 0) for fileSchemaType in data_types)
    done_count = 0
    break_flag = False
    total_samples = len(done)
    while done_count < total_samples and not break_flag:
        for sampleId, query, queryId, fileSchemaType in samples(bc, dask_client, nRals, ds_types = data_types):
            if not done[sampleId]:
                if fetch_result(bc, tokens, sampleId, drill, query, queryId, fileSchemaType):
                    done[sampleId] = True
                    done_count = done_count + 1
                    partial_done[fileSchemaType] = partial_done[fileSchemaType] + 1
                    if len(partial_done) == len(tpch_queries):
                        if Settings.execution_mode == ExecutionMode.GENERATOR:
                            print("==============================")
                            break_flag = True
                            break


def executionTest(dask_client, drill, dir_data_lc, bc, nRals):
    tokens = {} # sampleId(fileSchemaType+"."+queryId) -> token
    done = start_queries(bc, dask_client, nRals, tokens, dir_data_lc, tables)
    fetch_results(bc, dask_client, nRals, tokens, drill, done)


def main(dask_client, drill, dir_data_lc, bc, nRals):
    print("==============================")
    print(queryType)
    print("==============================")

    start_mem = gpuMemory.capture_gpu_memory_usage()
    executionTest(dask_client, drill, dir_data_lc, bc, nRals)
    end_mem = gpuMemory.capture_gpu_memory_usage()
    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
