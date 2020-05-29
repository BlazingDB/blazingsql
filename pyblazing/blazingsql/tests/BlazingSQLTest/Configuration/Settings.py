import json
import os

from datetime import datetime

data = {}
memory_list = None

dateNow = None

execution_mode = "full_mode"

def initialize():
    global data
    global memory_list
    global dateNow
    global execution_mode

    memory_list = []
    
    if data:
        raise ValueError("Initialize must be called once")   

    dateNow=datetime.now()

    create_json()

    execution_mode = data['RunSettings']['executionMode']


def create_json():
    #TestSettings
    conda_prefix = os.getenv("CONDA_PREFIX", "/tmp/")
    dataDirectory = os.getenv("BLAZINGSQL_E2E_DATA_DIRECTORY", conda_prefix+"/blazingsql-testing-files/data/")
    logDirectory = os.getenv("BLAZINGSQL_E2E_LOG_DIRECTORY", conda_prefix)
    # DEPRECATED percy kharo use blazingsql.__version__ instead of WORKSPACE_DIRECTORY 
    #workspaceDirectory = os.getenv("WORKSPACE_DIRECTORY"]
    fileResultsDirectory = os.getenv("BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY", conda_prefix+"/blazingsql-testing-files/results/")
    dataSize = os.getenv("BLAZINGSQL_E2E_DATA_SIZE", "100MB2Part")
    executionEnv = os.getenv("BLAZINGSQL_E2E_EXECUTION_ENV", "local")
    daskConnection = os.getenv("BLAZINGSQL_E2E_DASK_CONNECTION", "local") # use 127.0.0.1:8786 for manual dask

    #RunSettings
    executionMode = os.getenv("BLAZINGSQL_E2E_EXEC_MODE", "GPU_CI")
    nRals = os.getenv("BLAZINGSQL_E2E_N_RALS", 1)
    nGPUs = os.getenv("BLAZINGSQL_E2E_N_GPUS", 1)
    networkInterface = os.getenv("BLAZINGSQL_E2E_NETWORK_INTERFACE", "lo")
    saveLog = os.getenv("BLAZINGSQL_E2E_SAVE_LOG", "false")
    worksheet = os.getenv("BLAZINGSQL_E2E_WORKSHEET", "BSQL Log Results")
    logInfo = os.getenv("BLAZINGSQL_E2E_LOG_INFO", "")
    gspreadCacheHint = os.getenv("BLAZINGSQL_E2E_GSPREAD_CACHE", "false")
    compare_results = os.getenv("BLAZINGSQL_E2E_COMPARE_RESULTS", "true")
    targetTestGroups = os.getenv("BLAZINGSQL_E2E_TARGET_TEST_GROUPS", "") # comma separated values, if empty will run all the e2e tests

    targetTestGroups = "".join(targetTestGroups.split()) # trim all white spaces
    targetTestGroups = targetTestGroups.split(",")
    
    # when "a,b," -> ["a", "b", ""] we need to remove all those empty string entries
    while("" in targetTestGroups): 
        targetTestGroups.remove("") 
    
    #ComparissonTest
    compareByPercentaje = os.getenv("BLAZINGSQL_E2E_COMPARE_BY_PERCENTAJE", "false")
    acceptableDifference = os.getenv("BLAZINGSQL_E2E_ACCEPTABLE_DIFERENCE", 0.01)
    
    data['TestSettings'] = {
    'dataDirectory': dataDirectory,
    'logDirectory': logDirectory,
    #'workspaceDirectory': workspaceDirectory,
    'fileResultsDirectory': fileResultsDirectory,
    'dataSize': dataSize,
    'executionEnv': executionEnv,
    'daskConnection': daskConnection
    }

    data['RunSettings'] = {
    'executionMode': executionMode,
    'nRals': int(nRals),
    'nGPUs': nGPUs,
    'networkInterface': networkInterface,
    'saveLog': saveLog,
    'worksheet': worksheet,
    'logInfo': logInfo,
    'gspreadCacheHint': gspreadCacheHint,
    'compare_results': compare_results,
    'targetTestGroups': targetTestGroups
    }

    data['ComparissonTest'] = {
    'compareByPercentaje': compareByPercentaje,
    'acceptableDifference': acceptableDifference
    }

