from blazingsql import DataType
from DataBase import createSchema
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from Runner import runTest
from Utils import gpuMemory, skip_test
from EndToEndTests.tpchQueries import get_tpch_query


class runner(object):

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
            

    def executionTest(dask_client, drill, dir_data_lc, bc, nRals, sql, queryType):
        
        lst_queries = get_queries(queryType)
        
        run_queries(bc, dask_client, nRals, drill, dir_data_lc, tables, **extra_args)

