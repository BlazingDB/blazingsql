from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Smiles Test"


def main(dask_client, spark, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["docked", "dcoids", "smiles", "split"]
        data_types = [
            DataType.PARQUET
        ]

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            # Run Query ------------------------------------------------------
            # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = 'SELECT COUNT(*) FROM docked'
            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_02"
            scores_query = """SELECT s.name,
                                s.idnumber,
                                s.score,
                                s.rf3,
                                s.vs_dude_v2,
                                1.0 - d.dcoid AS dcoid
                            FROM split as s
                            INNER JOIN dcoids as d ON s.name=d.name"""
            result_gdf = bc.sql(scores_query)
            bc.create_table('scores',result_gdf)

            runTest.run_query(
                bc,
                spark,
                scores_query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                nested_query=True,
                blz_result=result_gdf,
            )



            queryId = "TEST_03"
            top_6WQF_query = "SELECT * FROM scores ORDER BY vs_dude_v2 DESC LIMIT 100000"
            result_gdf = bc.sql(top_6WQF_query)

            bc.create_table('top_6WQF',result_gdf)

            top_6WQF_equiv_query = top_6WQF_query.replace('scores', "( " + scores_query + " ) as scores")
            runTest.run_query(
                bc,
                spark,
                top_6WQF_equiv_query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                nested_query=True,
                blz_result=result_gdf,
            )


            queryId = "TEST_04"
            bin_width = 0.01 # kcal/mol
            query = """
                        select bucket_floor,
                            count(*) as bincount
                        from (SELECT floor(vs_dude_v2/{:1.4E})*{:1.4E} AS bucket_floor FROM split )
                            GROUP BY bucket_floor
                            ORDER BY bucket_floor""".format(bin_width,bin_width)
            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )


            queryId = "TEST_05"
            bin_width_x = 0.01 # angsstrom
            bin_width_y = 0.01 # kcal/mol
            query = """
                    select floor_x,
                        floor_y,
                        count(*) as bincount
                    from (SELECT floor(rf3/{:1.4E})*{:1.4E} AS floor_x, 
                                floor(vs_dude_v2/{:1.4E})*{:1.4E} AS floor_y
                        FROM split )
                        GROUP BY floor_x, floor_y
                        ORDER BY floor_x, floor_y""".format(bin_width_x,bin_width_x,bin_width_y,bin_width_y)
            runTest.run_query(
                bc,
                spark,
                query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
            )

            queryId = "TEST_06"
            bsql_query = """SELECT t.name,
                t.idnumber,
                t.score,
                t.vs_dude_v2,
                t.rf3,
                t.dcoid,
                docked.conf
                FROM docked INNER JOIN top_6WQF as t ON (docked.name = t.name)"""
            result_gdf = bc.sql(bsql_query)
            bc.create_table('top_6WQF_conf',result_gdf)

            top_6WQF_conf_equiv_query = bsql_query.replace("top_6WQF", "( " + top_6WQF_equiv_query + " )")
            #TODO re enable this test once we have the new version of dask
            # https://github.com/dask/distributed/issues/4645
            # https://github.com/rapidsai/cudf/issues/7773
            #runTest.run_query(
                #bc,
                #spark,
                #top_6WQF_conf_equiv_query,
                #queryId,
                #queryType,
                #worder,
                #"",
                #acceptable_difference,
                #use_percentage,
                #fileSchemaType,
                #nested_query=True,
                #blz_result=result_gdf,
            #)


            queryId = "TEST_07"
            bsql_query = """SELECT top.name,
                top.idnumber,
                top.score,
                top.vs_dude_v2,
                top.rf3,
                top.dcoid,
                top.conf,
                smiles.smiles
            FROM top_6WQF_conf AS top
            INNER JOIN smiles ON (top.name = smiles.name)"""
            result_gdf = bc.sql(bsql_query)
            
            equiv_query = bsql_query.replace("top_6WQF_conf", "( " + top_6WQF_conf_equiv_query + " )")
            runTest.run_query(
                bc,
                spark,
                equiv_query,
                queryId,
                queryType,
                worder,
                "",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                nested_query=True,
                blz_result=result_gdf,
            )
            

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break

    executionTest()

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    spark = "spark"

    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if ((Settings.execution_mode == ExecutionMode.FULL and
         compareResults == "true") or
            Settings.execution_mode == ExecutionMode.GENERATOR):
       # Create Table Spark -------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(
                            spark,
                            Settings.data["TestSettings"]["dataDirectory"]
        )

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, spark, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
