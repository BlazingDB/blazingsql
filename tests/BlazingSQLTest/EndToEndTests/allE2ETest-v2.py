from Configuration import ExecutionMode
from Configuration import Settings

from DataBase import createSchema
from pynvml import nvmlInit
from Utils import Execution, init_context
from blazingsql import DataType
from Runner import runTest
import sys
import time

def initializeDrill():
    # Create Table Drill -----------------------------------------
    from pydrill.client import PyDrill

    drill = PyDrill(host="localhost", port=8047)
    createSchema.init_drill_schema(
        drill, Settings.data["TestSettings"]["dataDirectory"], bool_test=True
    )
    createSchema.init_drill_schema(
        drill, Settings.data["TestSettings"]["dataDirectory"], smiles_test=True, fileSchemaType=DataType.PARQUET
    )

    return drill

def initializeSpark():
    # Create Table Spark -------------------------------------------------
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("allE2ETest").getOrCreate()
    createSchema.init_spark_schema(
        spark, Settings.data["TestSettings"]["dataDirectory"]
    )
    createSchema.init_spark_schema(
        spark, Settings.data["TestSettings"]["dataDirectory"], smiles_test=True, fileSchemaType=DataType.PARQUET
    )

    return spark

def initializeComparators():
    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if (
            Settings.execution_mode == ExecutionMode.FULL and compareResults == "true"
    ) or Settings.execution_mode == ExecutionMode.GENERATOR:

        drill = initializeDrill()
        spark = initializeSpark()

        return drill, spark

    return "drill", "spark"

def E2EResults():
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

def checkErrors(result, error_msgs):
    if Settings.execution_mode != ExecutionMode.GENERATOR:
        # NOTE kahro william percy mario : here we tell to gpuci there was
        # an error comparing with historic results
        # TODO william kharoly felipe we should try to enable and
        # use this function in the future
        if result is False:
            for error_msg in error_msgs:
                print(error_msg)

            elapsed = endTest - startTest  # in seconds

            if elapsed < 60:
                time_delta_desc = str(elapsed) + " seconds"
            else:
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
            return True
    return False

def runE2ETest(bc, dask_client, drill, spark):
    targetTestGroups = Settings.data["RunSettings"]["targetTestGroups"]

    runAllTests = (
        len(targetTestGroups) == 0
    )  # if targetTestGroups was empty the user wants to run all the tests



    #Validate Test to init



    dir_data_file = Settings.data["TestSettings"]["dataDirectory"]
    nRals = Settings.data["RunSettings"]["nRals"]

    from EndToEndTests import aggregationsWithoutGroupByTest

    if runAllTests or ("aggregationsWithoutGroupByTest" in targetTestGroups):
        aggregationsWithoutGroupByTest.main(
            dask_client, drill, dir_data_file, bc, nRals
        )

def main():
    print("**init end2end**")
    Execution.getArgs()
    nvmlInit()

    targetTestGroups = Settings.data["RunSettings"]["targetTestGroups"]

    # only innerJoinsTest will be with progress bar
    useProgressBar = False
    if "innerJoinsTest" in targetTestGroups:
        useProgressBar = True

    print("Using progress bar: ", useProgressBar)

    drill, spark = initializeComparators()
    bc, dask_client = init_context(useProgressBar = useProgressBar)

    runE2ETest(bc, dask_client, drill, spark)

    return E2EResults()

if __name__ == "__main__":
    global startTest
    global endTest

    startTest = time.time()  # in seconds

    result, error_msgs = main()

    endTest = time.time() # in seconds

    if checkErrors(result, error_msgs):
        # TODO percy kharo willian: uncomment this line
        # when gpuci has all the env vars set
        # return error exit status to the command prompt (shell)
        sys.exit(1)
