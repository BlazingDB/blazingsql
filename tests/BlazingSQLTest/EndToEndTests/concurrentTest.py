from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test
from EndToEndTests.tpchQueries import get_tpch_query

queryType = "Concurrent"
queries = [
    get_tpch_query("TEST_13"),
    get_tpch_query("TEST_07"),
    get_tpch_query("TEST_12"),
    get_tpch_query("TEST_04"),
    get_tpch_query("TEST_01"),
    "select * from nation"
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


def datasources(data_types, dask_client, nRals):
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue
        yield fileSchemaType


def samples(bc, data_types, dask_client, nRals, **kwargs):
    init_tables = kwargs.get("init_tables", False)
    dir_data_lc = kwargs.get("dir_data_lc", "")
    tables = kwargs.get("tables", [])
    for fileSchemaType in datasources(data_types, dask_client, nRals):
        if init_tables:
            print("Creating tables for", str(fileSchemaType))
            cs.create_tables(bc, dir_data_lc, fileSchemaType, tables=tables)
            print("All tables were created for", str(fileSchemaType))
        i = 0
        for query in queries:
            i = i + 1
            istr = str(i) if i > 10 else "0"+str(i)
            queryId = "TEST_" + istr
            sampleId = str(fileSchemaType) + "." + queryId
            yield sampleId, query, queryId, fileSchemaType


def start_queries(bc, data_types, dask_client, nRals, tokens, dir_data_lc, tables):
    print("######## Starting queries ...########")
    done = {} # sampleId -> True|False (fetch completed?)
    extra_args = {
        "dir_data_lc": dir_data_lc,
        "tables": tables,
        "init_tables": True
    }
    for sampleId, query, _, _ in samples(bc, data_types, dask_client, nRals, **extra_args):
        start_query(bc, tokens, query, sampleId)
        done[sampleId] = False
    return done


def fetch_results(bc, data_types, dask_client, nRals, tokens, drill, done):
    print("######## Fetching retults ...########")
    partial_done = dict((fileSchemaType, 0) for fileSchemaType in data_types)
    done_count = 0
    break_flag = False
    total_samples = len(done)
    while done_count < total_samples and not break_flag:
        for sampleId, query, queryId, fileSchemaType in samples(bc, data_types, dask_client, nRals):
            if not done[sampleId]:
                if fetch_result(bc, tokens, sampleId, drill, query, queryId, fileSchemaType):
                    done[sampleId] = True
                    done_count = done_count + 1
                    partial_done[fileSchemaType] = partial_done[fileSchemaType] + 1
                    if len(partial_done) == len(queries):
                        if Settings.execution_mode == ExecutionMode.GENERATOR:
                            print("==============================")
                            break_flag = True
                            break


def executionTest(dask_client, drill, dir_data_lc, bc, nRals):
    tokens = {} # sampleId(fileSchemaType+"."+queryId) -> token

    # Read Data TPCH------------------------------------------------------
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
    data_types = [
        DataType.DASK_CUDF,
        DataType.CSV,
        DataType.PARQUET,
    ]  # TODO json

    done = start_queries(bc, data_types, dask_client, nRals, tokens, dir_data_lc, tables)
    fetch_results(bc, data_types, dask_client, nRals, tokens, drill, done)


def main(dask_client, drill, dir_data_lc, bc, nRals):
    print("==============================")
    print(queryType)
    print("==============================")

    start_mem = gpuMemory.capture_gpu_memory_usage()
    executionTest(dask_client, drill, dir_data_lc, bc, nRals)
    end_mem = gpuMemory.capture_gpu_memory_usage()
    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


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
