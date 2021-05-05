from Configuration import ExecutionMode
from Configuration import Settings
from EndToEndTests.oldScripts import mainE2ELegacyTests

from DataBase import createSchema
from pynvml import nvmlInit
from Utils import Execution, init_context, init_comparators, gpuMemory
from blazingsql import DataType
from Runner import runTest
from Runner import TestSuites
import sys
import time


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

        gpuMemory.print_log_gpu_memory()

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

            def print_delta_time(startTest, endTest):
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
                + print_delta_time(startTest, endTest)
            )
            return True
    return False


def runE2ETest(bc, dask_client, drill, spark):
    mainE2ELegacyTests.runLegacyTest(bc, dask_client, drill, spark)

    runnerTest = TestSuites(bc, dask_client, drill, spark)
    runnerTest.setTargetTest(Settings.data["RunSettings"]["targetTestGroups"])
    runnerTest.runE2ETest()


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

    drill, spark = init_comparators()

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