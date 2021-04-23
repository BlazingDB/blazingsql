# from blazingsql import BlazingContext
from Configuration import ExecutionMode
from Configuration import Settings as Settings

# from dask.distributed import Client
from DataBase import createSchema as createSchema

from DataBase import createSchema as createSchema


from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, init_context
from blazingsql import DataType
from Runner import runner


def main():
    print("**init end2end**")
    Execution.getArgs()
    nvmlInit()
    dir_data_file = Settings.data["TestSettings"]["dataDirectory"]
    nRals = Settings.data["RunSettings"]["nRals"]

    bc, dask_client, drill, spark = runner.init_engines()
    
    targetTestGroups = Settings.data["RunSettings"]["targetTestGroups"]

    runAllTests = (
        len(targetTestGroups) == 0
    )  # if targetTestGroups was empty the user wants to run all the tests

    if runAllTests or ("caseSuit" in targetTestGroups):
        runner.executionTest(bc, dask_client, drill, spark, nRals, "caseSuit")

    if runAllTests or ("castSuite" in targetTestGroups):
        runner.executionTest(bc, dask_client, drill, spark, nRals, "castSuite")

    if runAllTests or ("timestampSuite" in targetTestGroups):
        runner.executionTest(bc, dask_client, drill, spark, nRals, "timestampSuite")
    
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
