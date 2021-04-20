from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from Runner import runner
from Utils import gpuMemory, skip_test

queryType = "case"

# Parameter to indicate if its necessary to order
# the resulsets before compare them
worder = 1
use_percentage = False
acceptable_difference = 0.01

data_types = [
    DataType.DASK_CUDF,
    DataType.CSV,
    DataType.PARQUET,
]

tables = [
    "nation",
    "region",
    "customer",
    "lineitem",
    "orders",
    "supplier",
] 


def main(dask_client, drill, dir_data_lc, bc, nRals):
    print("==============================")
    print(queryType)
    print("==============================")

    start_mem = gpuMemory.capture_gpu_memory_usage()
    runner.executionTest(dask_client, drill, dir_data_lc, bc, nRals, queryType)
    end_mem = gpuMemory.capture_gpu_memory_usage()
    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)
    
if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()
    
    drill, spark = init_comparisson_engine ()

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(
        dask_client,
        drill,
        spark,
        Settings.data["TestSettings"]["dataDirectory"],
        bc,
        nRals,
    )

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()





#lst_queries = [
#    { "id":"TEST_01", "sql":"select * from nation", "order": worder, use_percentage, acceptable_difference },
 #   { "id":"TEST_02", "sql":"select * from region", "order": worder, use_percentage, acceptable_difference },
  #  { "id":"TEST_03", "sql":"select * from customer","order": worder, use_percentage, acceptable_difference },
   # { "id":"TEST_04", "sql":"select * from lineitem", "order": worder, use_percentage, acceptable_difference },
    #{ "id":"TEST_05", "sql":"select * from supplier", "order": worder, use_percentage, acceptable_difference },
#]
