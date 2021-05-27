import os
import sys
import yaml
from datetime import datetime

data = {}
memory_list = None

dateNow = None

execution_mode = "full"

compare_res = "false"

def initialize():
    global data
    global memory_list
    global dateNow
    global execution_mode
    global compare_res

    memory_list = []

    if data:
        return

    dateNow = datetime.now()

    create_json()

    execution_mode = data["RunSettings"]["executionMode"]

def readFile():
    cwd = os.path.dirname(os.path.realpath(__file__))
    if "-config_file" in sys.argv and len(sys.argv) >= 3:
        fileName = cwd + "/../Runner/" + sys.argv[2]
    else:
        fileName = cwd + "/../Runner/config.yaml"

    if os.path.isfile(fileName):
        with open(fileName, 'r') as stream:
            fileYaml = yaml.safe_load(stream)
    else:
        raise Exception("Error: " + fileName + " not exist")

    return fileYaml

def create_json():
    fileYaml     = readFile()
    conda_prefix = os.getenv("CONDA_PREFIX", "/tmp/")

    if "TEST_SETTINGS" in fileYaml:
        test_settings        = fileYaml["TEST_SETTINGS"]

      # dataDirectory        = test_settings.get("DATA_DIRECTORY"        , conda_prefix + "/blazingsql-testing-files/data/")
      # fileResultsDirectory = test_settings.get("FILE_RESULT_DIRECTORY" , conda_prefix + "/blazingsql-testing-files/results/")
        logDirectory         = test_settings.get("LOG_DIRECTORY"         , conda_prefix)
        dataSize             = test_settings.get("DATA_SIZE"             , "100MB2Part")
        executionEnv         = test_settings.get("EXECUTION_ENV"         , "local")
        daskConnection       = test_settings.get("DASK_CONNECTION"       , "local") # use 127.0.0.1:8786 for manual dask

      # dataDirectory        = os.path.expandvars(dataDirectory)
      # fileResultsDirectory = os.path.expandvars(fileResultsDirectory)
        logDirectory         = os.path.expandvars(logDirectory)

    # TestSettings
    dataDirectory = os.getenv(
        "BLAZINGSQL_E2E_DATA_DIRECTORY",
        conda_prefix + "/blazingsql-testing-files/data/",
    )
    fileResultsDirectory = os.getenv(
        "BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY",
        conda_prefix + "/blazingsql-testing-files/results/",
    )

    # AWS S3 env vars
    awsS3BucketName = os.getenv("BLAZINGSQL_E2E_AWS_S3_BUCKET_NAME", "")
    awsS3AccessKeyId = os.getenv("BLAZINGSQL_E2E_AWS_S3_ACCESS_KEY_ID", "")
    awsS3SecretKey = os.getenv("BLAZINGSQL_E2E_AWS_S3_SECRET_KEY", "")

    # Google Storage env vars
    googleStorageProjectId = os.getenv(
                            "BLAZINGSQL_E2E_GOOGLE_STORAGE_PROJECT_ID", "")
    googleStorageBucketName = os.getenv(
                            "BLAZINGSQL_E2E_GOOGLE_STORAGE_BUCKET_NAME", "")
    googleStorageAdcJsonFile = os.getenv(
        "BLAZINGSQL_E2E_GOOGLE_STORAGE_ADC_JSON_FILE", ""
    )


    if "RUN_SETTINGS" in fileYaml:
        run_settings = fileYaml["RUN_SETTINGS"]

        executionMode       = run_settings.get("EXEC_MODE"         , "gpuci")
        nGPUs               = run_settings.get("NGPUS"             , 1)
        networkInterface    = run_settings.get("NETWORK_INTERFACE" , "lo")
        # saveLog           = run_settings.get("SAVE_LOG"          , "false")
        worksheet           = run_settings.get("WORKSHEET"         , "BSQL Log Results")
        logInfo             = run_settings.get("LOG_INFO"          , "")
        compare_results     = run_settings.get("COMPARE_RESULTS"   , "true")
        concurrent          = run_settings.get("CONCURRENT"        , False)

    # RunSettings
    nRals = os.getenv("BLAZINGSQL_E2E_N_RALS", 1)
    saveLog = os.getenv("BLAZINGSQL_E2E_SAVE_LOG", "false")
    testsWithNulls = os.getenv("BLAZINGSQL_E2E_TEST_WITH_NULLS", "false")
    targetTestGroups = os.getenv(
        "BLAZINGSQL_E2E_TARGET_TEST_GROUPS", ""
    )  # comma separated values, if empty will run all the e2e tests
    gspreadCacheHint = os.getenv("BLAZINGSQL_E2E_GSPREAD_CACHE", "false")


    # trim all white spaces
    targetTestGroups = "".join(targetTestGroups.split())
    targetTestGroups = targetTestGroups.split(",")

    # when "a,b," -> ["a", "b", ""] we need to remove all those
    # empty string entries
    while "" in targetTestGroups:
        targetTestGroups.remove("")

    data["TestSettings"] = {
        "dataDirectory": dataDirectory,
        "logDirectory": logDirectory,
        # 'workspaceDirectory': workspaceDirectory,
        "fileResultsDirectory": fileResultsDirectory,
        "dataSize": dataSize,
        "executionEnv": executionEnv,
        "daskConnection": daskConnection,
        "awsS3BucketName": awsS3BucketName,
        "awsS3AccessKeyId": awsS3AccessKeyId,
        "awsS3SecretKey": awsS3SecretKey,
        "googleStorageProjectId": googleStorageProjectId,
        "googleStorageBucketName": googleStorageBucketName,
        "googleStorageAdcJsonFile": googleStorageAdcJsonFile,
    }

    data["RunSettings"] = {
        "executionMode": executionMode,
        "nGPUs": nGPUs,
        "networkInterface": networkInterface,
        "saveLog": saveLog,
        "worksheet": worksheet,
        "logInfo": logInfo,
        "gspreadCacheHint": gspreadCacheHint,
        "compare_results": compare_results,
        "targetTestGroups": targetTestGroups,
        "concurrent": concurrent,
        "nRals": int(nRals),
        "testsWithNulls": testsWithNulls,
    }

initialize()
