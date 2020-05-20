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
    dataDirectory = os.environ["DATA_DIRECTORY"]
    logDirectory = os.environ["LOG_DIRECTORY"]
    workspaceDirectory = os.environ["WORKSPACE_DIRECTORY"]
    fileResultsDirectory = os.environ["FILE_RESULT_DIRECTORY"]
    dataSize = os.environ["DATA_SIZE"]
    executionEnv = os.environ["EXECUTION_ENV"]
    daskConnection = os.environ["DASK_CONNECTION"]

    #RunSettings
    executionMode = os.environ["EXEC_MODE"]
    nRals = os.environ["N_RALS"]
    nGPUs = os.environ["N_GPUS"]
    networkInterface = os.environ["NETWORK_INTERFACE"]
    saveLog = os.environ["SAVE_LOG"]
    worksheet = os.environ["WORKSHEET"]
    logInfo = os.environ["LOG_INFO"]
    compare_results = os.environ["COMPARE_RESULTS"]

    #ComparissonTest
    compareByPercentaje = os.environ["COMPARE_BY_PERCENTAJE"]
    acceptableDifference = os.environ["ACCEPTABLE_DIFERENCE"]
    
    data['TestSettings'] = {
    'dataDirectory': dataDirectory,
    'logDirectory': logDirectory,
    'workspaceDirectory': workspaceDirectory,
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
    'compare_results': compare_results
    }

    data['ComparissonTest'] = {
    'compareByPercentaje': compareByPercentaje,
    'acceptableDifference': acceptableDifference
    }

# export DATA_DIRECTORY="/home/kharoly/blazingsql/DataSet100MB_2/"
# export LOG_DIRECTORY="/home/kharoly/blazingsql/logtest/"
# export WORKSPACE_DIRECTORY="$CONDA_PREFIX/"
# export FILE_RESULT_DIRECTORY="/home/kharoly/blazingsql/fileResult/"
# export DATA_SIZE="100MB"
# export EXECUTION_ENV="Local env"
# export DASK_CONNECTION="local"

# export N_RALS=1
# export N_GPUS=1
# export NETWORK_INTERFACE="lo"
# export SAVE_LOG=false
# export WORKSHEET="BSQL Performance Results"
# export COMPARE_RESULTS=true

# export COMPARE_BY_PERCENTAJE=false
# export ACCEPTABLE_DIFERENCE=0.01

