# from blazingsql import BlazingContext
from Configuration import ExecutionMode
from Configuration import Settings as Settings

# from dask.distributed import Client
from DataBase import createSchema as createSchema

from EndToEndTests import (
    GroupByWitoutAggregations,
    aggregationsWithoutGroupByTest,
    bindableAliasTest,
    booleanTest,
    caseTest,
    castTest,
    countDistinctTest,
)
from EndToEndTests import coalesceTest as coalesceTest
from EndToEndTests import columnBasisTest as columnBasisTest
from EndToEndTests import (
    commonTableExpressionsTest,
    concatTest,
    configOptionsTest,
    countWithoutGroupByTest,
    dateTest,
    dayOfWeekTest,
    dirTest,
    fileSystemGSTest,
    fileSystemLocalTest,
    fileSystemS3Test,
)
from EndToEndTests import fullOuterJoinsTest as fullOuterJoinsTest
from EndToEndTests import groupByTest as groupByTest
from EndToEndTests import innerJoinsTest as innerJoinsTest
from EndToEndTests import crossJoinsTest as crossJoinsTest
from EndToEndTests import leftOuterJoinsTest as leftOuterJoinsTest
from EndToEndTests import (
    likeTest,
    literalTest,
    loggingTest,
    # loadDataTest,
    messageValidationTest,
    nestedQueriesTest,
    nonEquiJoinsTest,
)
from EndToEndTests import orderbyTest as orderbyTest
from EndToEndTests import (
    predicatesWithNulls,
    roundTest,
    stringTests,
    substringTest,
    stringCaseTest,
    tablesFromPandasTest,
    # timestampdiffTest,
    timestampTest,
    toTimestampTest,
    tpchQueriesTest,
)
from EndToEndTests import unaryOpsTest as unaryOpsTest
from EndToEndTests import unifyTablesTest
from EndToEndTests import unionTest as unionTest
from EndToEndTests import useLimitTest
from EndToEndTests import whereClauseTest as whereClauseTest
from EndToEndTests import wildCardTest
from EndToEndTests import smilesTest
from EndToEndTests import jsonTest
from EndToEndTests import windowFunctionTest
from EndToEndTests import windowNoPartitionTest
from EndToEndTests import concurrentTest
from EndToEndTests import tablesFromSQL
from EndToEndTests import hiveFileTest
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, init_context
from blazingsql import DataType


def main():
    print("**init end2end**")
    Execution.getArgs()
    nvmlInit()
    dir_data_file = Settings.data["TestSettings"]["dataDirectory"]
    nRals = Settings.data["RunSettings"]["nRals"]

    drill = "drill"
    spark = "spark"
    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if (
        Settings.execution_mode == ExecutionMode.FULL and compareResults == "true"
    ) or Settings.execution_mode == ExecutionMode.GENERATOR:

        # Create Table Drill -----------------------------------------
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        createSchema.init_drill_schema(
            drill, Settings.data["TestSettings"]["dataDirectory"], bool_test=True
        )
        createSchema.init_drill_schema(
            drill, Settings.data["TestSettings"]["dataDirectory"], smiles_test=True, fileSchemaType=DataType.PARQUET
        )

        # Create Table Spark -------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("allE2ETest").getOrCreate()
        createSchema.init_spark_schema(
            spark, Settings.data["TestSettings"]["dataDirectory"]
        )
        createSchema.init_spark_schema(
            spark, Settings.data["TestSettings"]["dataDirectory"], smiles_test=True, fileSchemaType=DataType.PARQUET
        )

    targetTestGroups = Settings.data["RunSettings"]["targetTestGroups"]

    # only innerJoinsTest will be with progress bar
    useProgressBar = False
    if "innerJoinsTest" in targetTestGroups:
        useProgressBar = True

    print("Using progress bar: ", useProgressBar)

    # Create Context For BlazingSQL
    bc, dask_client = init_context(useProgressBar = useProgressBar)

    runAllTests = (
        len(targetTestGroups) == 0
    )  # if targetTestGroups was empty the user wants to run all the tests

    if runAllTests or ("hiveFileTest" in targetTestGroups):
        hiveFileTest.main(dask_client, spark, dir_data_file, bc, nRals)

    if runAllTests or ("aggregationsWithoutGroupByTest" in targetTestGroups):
        aggregationsWithoutGroupByTest.main(
            dask_client, drill, dir_data_file, bc, nRals
        )

    if runAllTests or ("coalesceTest" in targetTestGroups):
        coalesceTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("columnBasisTest" in targetTestGroups):
        columnBasisTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("commonTableExpressionsTest" in targetTestGroups):
        commonTableExpressionsTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("countDistinctTest" in targetTestGroups):
        countDistinctTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("countWithoutGroupByTest" in targetTestGroups):
        countWithoutGroupByTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("dateTest" in targetTestGroups):
        dateTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("timestampTest" in targetTestGroups):
        timestampTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("toTimestampTest" in targetTestGroups):
        toTimestampTest.main(dask_client, spark, dir_data_file, bc, nRals)

    if runAllTests or ("dayOfWeekTest" in targetTestGroups):
        dayOfWeekTest.main(dask_client, spark, dir_data_file, bc, nRals)

    if runAllTests or ("fullOuterJoinsTest" in targetTestGroups):
        fullOuterJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("groupByTest" in targetTestGroups):
        groupByTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("GroupByWitoutAggregations" in targetTestGroups):
        GroupByWitoutAggregations.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("innerJoinsTest" in targetTestGroups):
        innerJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("crossJoinsTest" in targetTestGroups):
        crossJoinsTest.main(dask_client, spark, dir_data_file, bc, nRals)

    if runAllTests or ("leftOuterJoinsTest" in targetTestGroups):
        leftOuterJoinsTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("nonEquiJoinsTest" in targetTestGroups):
        nonEquiJoinsTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    # loadDataTest.main(dask_client, bc) #check this

    if runAllTests or ("nestedQueriesTest" in targetTestGroups):
        nestedQueriesTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("orderbyTest" in targetTestGroups):
        orderbyTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("predicatesWithNulls" in targetTestGroups):
        predicatesWithNulls.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("stringTests" in targetTestGroups):
        stringTests.main(dask_client, drill, spark, dir_data_file, bc, nRals)

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
        dirTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    # HDFS is not working yet
    # fileSystemHdfsTest.main(dask_client, drill, dir_data_file, bc)

    # HDFS is not working yet
    # mixedFileSystemTest.main(dask_client, drill, dir_data_file, bc)

    if runAllTests or ("likeTest" in targetTestGroups):
        likeTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("substringTest" in targetTestGroups):
        substringTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("stringCaseTest" in targetTestGroups):
        stringCaseTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("wildCardTest" in targetTestGroups):
        wildCardTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("tpchQueriesTest" in targetTestGroups):
        tpchQueriesTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("roundTest" in targetTestGroups):
        roundTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("fileSystemLocalTest" in targetTestGroups):
        fileSystemLocalTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("messageValidationTest" in targetTestGroups):
        messageValidationTest.main(dask_client, drill, dir_data_file, bc, nRals)

    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
    if testsWithNulls != "true":
        if Settings.execution_mode != ExecutionMode.GPUCI:
            if runAllTests or ("fileSystemS3Test" in targetTestGroups):
                fileSystemS3Test.main(dask_client, drill, dir_data_file, bc, nRals)

            if runAllTests or ("fileSystemGSTest" in targetTestGroups):
                fileSystemGSTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("loggingTest" in targetTestGroups):
        loggingTest.main(dask_client, dir_data_file, bc, nRals)

    # timestampdiffTest.main(dask_client, spark, dir_data_file, bc, nRals)

    #TODO re enable this test once we have the new version of dask
    # https://github.com/dask/distributed/issues/4645
    # https://github.com/rapidsai/cudf/issues/7773
    #if runAllTests or ("smilesTest" in targetTestGroups):
    #    smilesTest.main(dask_client, spark, dir_data_file, bc, nRals)

    if testsWithNulls != "true":
        if runAllTests or ("jsonTest" in targetTestGroups):
            jsonTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("windowFunctionTest" in targetTestGroups):
        windowFunctionTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("windowNoPartitionTest" in targetTestGroups):
        windowNoPartitionTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if testsWithNulls != "true":
        if runAllTests or ("concurrentTest" in targetTestGroups):
            concurrentTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if testsWithNulls == "true":
        if Settings.execution_mode != ExecutionMode.GPUCI:
            if runAllTests or ("tablesFromSQL" in targetTestGroups):
                tablesFromSQL.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    # WARNING!!! This Test must be the last one to test -------------------------------------------------------------------------------------------------------------------------------------------
    if runAllTests or ("configOptionsTest" in targetTestGroups):
        configOptionsTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:

        result, error_msgs = runTest.save_log(
            Settings.execution_mode == ExecutionMode.GPUCI
        )

        max = 0
        for i in range(0, len(Settings.memory_list)):
            if (Settings.memory_list[i].delta) > max:
                max = Settings.memory_list[i].delta

        print("MAX DELTA: " + str(max))
        print(
            """***********************************************************
              ********************"""
        )

        for i in range(0, len(Settings.memory_list)):
            print(
                Settings.memory_list[i].name
                + ":"
                + "   Start Mem: "
                + str(Settings.memory_list[i].start_mem)
                + "   End Mem: "
                + str(Settings.memory_list[i].end_mem)
                + "   Diff: "
                + str(Settings.memory_list[i].delta)
            )

        return result, error_msgs

    return True, []


if __name__ == "__main__":
    import time

    start = time.time()  # in seconds

    result, error_msgs = main()

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        # NOTE kahro william percy mario : here we tell to gpuci there was
        # an error comparing with historic results
        # TODO william kharoly felipe we should try to enable and
        # use this function in the future
        if result is False:
            for error_msg in error_msgs:
                print(error_msg)
            import sys

            end = time.time()  # in seconds
            elapsed = end - start  # in seconds

            time_delta_desc = (
                str(elapsed / 60)
                + " minutes and "
                + str(int(elapsed) % 60)
                + " seconds"
            )
            print(
                "==>> E2E FAILED against previous run, total time was: "
                + time_delta_desc
            )

            # TODO percy kharo willian: uncomment this line
            # when gpuci has all the env vars set
            # return error exit status to the command prompt (shell)
            sys.exit(1)
