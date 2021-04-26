from collections import OrderedDict

from blazingsql import DataType
from DataBase import createSchema
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from Runner import runTest
from Utils import gpuMemory, skip_test
from EndToEndTests import tpchQueries


class Sample:
    def __init__(self, **kwargs):
        self.sample_id = kwargs.get("id", "")
        self.query = kwargs.get("query", "")
        self.table_mapper = kwargs.get("table_mapper", Sample.default_table_mapper)
        self.worder = kwargs.get("worder", 1)
        self.use_percentage = kwargs.get("use_percentage", False)
        self.acceptable_difference = kwargs.get("acceptable_difference", 0.01)
        self.use_pyspark = kwargs.get("use_pyspark", False) # we use drill by default

    def default_table_mapper(query, tables = {}):
        return query


# table_mapper if you want to apply the same global table_mapper for all the samples
def define_samples(sample_list: [Sample], table_mapper = None):
    ret = OrderedDict()
    i = 1
    for sample in sample_list:
        if table_mapper:
            sample.table_mapper = table_mapper # override with global table_mapper
        istr = str(i) if i > 10 else "0"+str(i)
        sampleId = sample.sample_id
        if not sampleId:
            sampleId = "TEST_" + istr
            i = i + 1
        ret[sampleId] = sample
    return ret


queryType = "TablesFromSQL"

samples = define_samples([
    Sample(query = tpchQueries.query_templates["TEST_13"]),
    Sample(query = tpchQueries.query_templates["TEST_07"]),
    Sample(query = tpchQueries.query_templates["TEST_12"]),
    Sample(query = tpchQueries.query_templates["TEST_04"]),
    Sample(query = tpchQueries.query_templates["TEST_01"]),
    Sample(query = "select * from {nation}", use_pyspark = True),
    Sample(query = tpchQueries.query_templates["TEST_08"], use_pyspark = True),
    Sample(query = """select c_custkey, c_nationkey, c_acctbal
                    from {customer} where c_custkey < 150 and c_nationkey = 5
                    or c_custkey = 200 or c_nationkey >= 10
                    or c_acctbal <= 500""")
], tpchQueries.map_tables)

data_types = [
    DataType.MYSQL,
    # TODO percy c.gonzales
    #DataType.POSTGRESQL,
    #DataType.SQLITE,
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


def datasources(dask_client, nRals):
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue
        yield fileSchemaType


def sample_items(bc, dask_client, nRals, **kwargs):
    # example: {csv: {tb1: tb1_csv, ...}, parquet: {tb1: tb1_parquet, ...}}
    dstables = dict((ds, dict((t, t+"_"+str(ds).split(".")[1]) for t in tables)) for ds in data_types)

    init_tables = kwargs.get("init_tables", False)
    sql_table_filter_map = kwargs.get("sql_table_filter_map", {})
    sql_table_batch_size_map = kwargs.get("sql_table_batch_size_map", {})
    sql = kwargs.get("sql_connection", None)
    dir_data_lc = kwargs.get("dir_data_lc", "")

    for fileSchemaType in datasources(dask_client, nRals):
        datasource_tables = dstables[fileSchemaType]

        if init_tables:
            print("Creating tables for", str(fileSchemaType))
            table_names = list(datasource_tables.values())
            if isinstance(sql, createSchema.sql_connection): # create sql tables
                createSchema.create_tables(bc, "", fileSchemaType,
                    tables = tables,
                    table_names=table_names,
                    sql_table_filter_map = sql_table_filter_map,
                    sql_table_batch_size_map = sql_table_batch_size_map,
                    sql_connection = sql,
                )
            else: # create in file tables (parquet, csv, etc)
                createSchema.create_tables(bc, dir_data_lc, fileSchemaType,
                    tables = tables,
                    table_names=table_names
                )

            print("All tables were created for", str(fileSchemaType))

        for sampleId, sample in samples.items():
            sampleUID = str(fileSchemaType) + "." + sampleId
            yield sampleUID, sampleId, fileSchemaType, datasource_tables


def run_queries(bc, dask_client, nRals, drill, spark, dir_data_lc, tables, **kwargs):
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
        "dir_data_lc": dir_data_lc,
    }
    currrentFileSchemaType = data_types[0]
    for sampleUID, sampleId, fileSchemaType, datasource_tables in sample_items(bc, dask_client, nRals, **extra_args):
        datasourceDone = (fileSchemaType != currrentFileSchemaType)
        if datasourceDone and Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break_flag = True
            break

        sample = samples[sampleId]

        query = sample.table_mapper(sample.query, datasource_tables) # map to tables with datasource info: order_csv, nation_csv ...
        worder = sample.worder
        use_percentage = sample.use_percentage
        acceptable_difference = sample.acceptable_difference
        use_pyspark = sample.use_pyspark
        engine = spark if use_pyspark else drill
        query_spark = sample.table_mapper(sample.query) # map to tables without datasource info: order, nation ...

        print("==>> Run query for sample", sampleId)
        print("PLAN:")
        print(bc.explain(query, True))
        runTest.run_query(
            bc,
            engine,
            query,
            sampleId,
            queryType,
            worder,
            "",
            acceptable_difference,
            use_percentage,
            fileSchemaType,
            query_spark = query_spark,
            print_result = True
        )
        currrentFileSchemaType = fileSchemaType


def setup_test(data_type: DataType) -> createSchema.sql_connection:
    sql = createSchema.get_sql_connection(data_type)
    if not sql:
        print(f"ERROR: You cannot run tablesFromSQL test, setup your SQL connection for {data_type}using env vars! See tests/README.md")
        return None

    if data_type is DataType.MYSQL:
      from DataBase import mysqlSchema
      mysqlSchema.create_and_load_tpch_schema(sql)
      return sql

    if data_type is DataType.SQLITE:
      from DataBase import sqliteSchema
      sqliteSchema.create_and_load_tpch_schema(sql)
      return sql


def executionTest(dask_client, drill, spark, dir_data_lc, bc, nRals, sql):
    extra_args = {
        "sql_table_filter_map": sql_table_filters,
        "sql_table_batch_size_map": sql_table_batch_sizes,
        "sql_connection": sql,
    }
    run_queries(bc, dask_client, nRals, drill, spark, dir_data_lc, tables, **extra_args)


def main(dask_client, drill, spark, dir_data_lc, bc, nRals):
    print("==============================")
    print(queryType)
    print("==============================")

    for data_type in data_types:
        sql = None
        is_file_ds = False
        # we can change the datatype for these tests and it should works just fine
        if data_type not in [DataType.MYSQL, DataType.POSTGRESQL, DataType.SQLITE]:
            is_file_ds = True
        else:
            sql = setup_test(data_type)
        if sql or is_file_ds:
            start_mem = gpuMemory.capture_gpu_memory_usage()
            executionTest(dask_client, drill, spark, dir_data_lc, bc, nRals, sql)
            end_mem = gpuMemory.capture_gpu_memory_usage()
            gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
