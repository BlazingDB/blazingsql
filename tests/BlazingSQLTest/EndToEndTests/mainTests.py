from Configuration import ExecutionMode
from Configuration import Settings

from DataBase import createSchema
from pynvml import nvmlInit
from Utils import Execution, init_context, init_comparators, gpuMemory
from blazingsql import DataType
from Runner import runTest
from Runner import testSuites
import sys
import time


def test_results():

    if Settings.execution_mode != ExecutionMode.GENERATOR:

        result, error_msgs = runTest.save_log(
            Settings.execution_mode == ExecutionMode.GPUCI
        )

        gpuMemory.print_max_delta()

        return result, error_msgs

    return True, []


def check_errors(result, error_msgs):

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        # NOTE kharo william percy mario : here we tell to gpuci there was
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

            return time_delta_desc

            print(
                "==>> E2E FAILED against previous run, total time was: "
                + time_delta_desc
            )
            return True

    return False


def run_tests(bc, dask_client, drill, spark):
    
    runLegacyTest(bc, dask_client, drill, spark)

    runnerTest = testSuites.testSuites(bc, dask_client, drill, spark)
    runnerTest.setTargetTest(Settings.data["RunSettings"]["targetTestGroups"])
    
    runnerTest.runE2ETest()

    #TO D0: Add what it is necessary to run test suites with nulls

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

    drill, spark = init_Comparators()

    bc, dask_client = init_context(useProgressBar = useProgressBar)

    run_tests(bc, dask_client, drill, spark)

    return test_results()


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




# This code should be removed as soon as particular suite cases are implemented using templates

def runLegacyTest(bc, dask_client, drill, spark):
    targetTestGroups = Settings.data["RunSettings"]["targetTestGroups"]

    runAllTests = (
        len(targetTestGroups) == 0
    )  # if targetTestGroups was empty the user wants to run all the tests

    dir_data_file = Settings.data["TestSettings"]["dataDirectory"]
    nRals = Settings.data["RunSettings"]["nRals"]

    from LegacyTests import aggregationsWithoutGroupByTest
    from LegacyTests import fileSystemGSTest
    from LegacyTests import fileSystemLocalTest
    from LegacyTests import fileSystemS3Test
    from LegacyTests import columnBasisTest
    from LegacyTests import hiveFileTest
    from LegacyTests import concurrentTest

    if runAllTests or ("columnBasisTest" in targetTestGroups):
        columnBasisTest.main(dask_client, drill, dir_data_file, bc, nRals)

    if runAllTests or ("hiveFileTest" in targetTestGroups):
        hiveFileTest.main(dask_client, spark, dir_data_file, bc, nRals)

    if runAllTests or ("fileSystemLocalTest" in targetTestGroups):
        fileSystemLocalTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("fileSystemGSTest" in targetTestGroups):
        fileSystemGSTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("fileSystemS3Test" in targetTestGroups):
        fileSystemS3Test.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("concurrentTest" in targetTestGroups):
        concurrentTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    if runAllTests or ("aggregationsWithoutGroupByTest" in targetTestGroups):
        aggregationsWithoutGroupByTest.main(dask_client, drill, spark, dir_data_file, bc, nRals)

    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]
    if testsWithNulls != "true":
        if Settings.execution_mode != ExecutionMode.GPUCI:
            if runAllTests or ("fileSystemS3Test" in targetTestGroups):
                fileSystemS3Test.main(dask_client, drill, dir_data_file, bc, nRals)

            if runAllTests or ("fileSystemGSTest" in targetTestGroups):
                fileSystemGSTest.main(dask_client, drill, dir_data_file, bc, nRals)