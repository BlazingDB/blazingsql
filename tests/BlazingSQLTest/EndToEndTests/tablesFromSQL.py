from blazingsql import DataType
from DataBase import createSchema
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from Runner import runTest
from Utils import gpuMemory, skip_test
from EndToEndTests.tpchQueries import get_tpch_query

queryType = "TablesFromSQL"
data_types = [
    DataType.MYSQL,
    #DataType.POSTGRESQL,
    #DataType.SQLITE,
    # TODO percy c.gonzales
]

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

sql_table_filters = {
#    "lineitem": "l_quantity < 24",
}

# aprox. taken from parquet parts (tpch with nulls 2 parts)
sql_table_batch_sizes = {
    "nation": 30,
    "region": 10,
    "customer": 7000,
    "lineitem": 300000,
    "orders": 7500,
    "supplier": 600,
    "part": 10000,
    "partsupp": 40000,
}

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

# example: {csv: {tb1: tb1_csv, ...}, parquet: {tb1: tb1_parquet, ...}}
datasource_tables = dict((ds, dict((t, t+"_"+str(ds).split(".")[1]) for t in tables)) for ds in data_types)


def datasources(dask_client, nRals):
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue
        yield fileSchemaType


def samples(bc, dask_client, nRals, **kwargs):
    init_tables = kwargs.get("init_tables", False)
    sql_table_filter_map = kwargs.get("sql_table_filter_map", {})
    sql_table_batch_size_map = kwargs.get("sql_table_batch_size_map", {})
    sql = kwargs.get("sql_connection", None)
    for fileSchemaType in datasources(dask_client, nRals):
        dstables = datasource_tables[fileSchemaType]

        if init_tables:
            print("Creating tables for", str(fileSchemaType))
            table_names=list(dstables.values())
            createSchema.create_tables(bc, "", fileSchemaType,
                tables = tables,
                table_names=table_names,
                sql_table_filter_map = sql_table_filter_map,
                sql_table_batch_size_map = sql_table_batch_size_map,
                sql_connection = sql,
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


def run_queries(bc, dask_client, nRals, drill, dir_data_lc, tables, **kwargs):
    sql_table_filter_map = kwargs.get("sql_table_filter_map", {})
    sql_table_batch_size_map = kwargs.get("sql_table_batch_size_map", {})
    sql = kwargs.get("sql_connection", None)
    print("######## Starting queries ...########")
    extra_args = {
        "table_names": tables,
        "init_tables": True,
        "ds_types": data_types,
        "sql_table_filter_map": sql_table_filter_map,
        "sql_table_batch_size_map": sql_table_batch_size_map,
        "sql_connection": sql,
    }
    currrentFileSchemaType = data_types[0]
    for sampleId, query, queryId, fileSchemaType in samples(bc, dask_client, nRals, **extra_args):
        datasourceDone = (fileSchemaType != currrentFileSchemaType)
        if datasourceDone and Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break_flag = True
            break

        print("==>> Run query for sample", sampleId)
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
            print_result = True
        )
        currrentFileSchemaType = fileSchemaType


def setup_test() -> bool:
    sql = createSchema.get_sql_connection(DataType.MYSQL)
    if not sql:
        print("ERROR: You cannot run tablesFromSQL test, settup your SQL connection using env vars! See tests/README.md")
        return None

    from DataBase import mysqlSchema

    mysqlSchema.create_and_load_tpch_schema(sql)
    return sql


def executionTest(dask_client, drill, dir_data_lc, bc, nRals, sql):
    extra_args = {
        "sql_table_filter_map": sql_table_filters,
        "sql_table_batch_size_map": sql_table_batch_sizes,
        "sql_connection": sql,
    }
    run_queries(bc, dask_client, nRals, drill, dir_data_lc, tables, **extra_args)


def main(dask_client, drill, dir_data_lc, bc, nRals):
    print("==============================")
    print(queryType)
    print("==============================")

    sql = setup_test()
    if sql:
        start_mem = gpuMemory.capture_gpu_memory_usage()
        executionTest(dask_client, drill, dir_data_lc, bc, nRals, sql)
        end_mem = gpuMemory.capture_gpu_memory_usage()
        gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
