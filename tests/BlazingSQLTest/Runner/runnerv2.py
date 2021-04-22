from blazingsql import DataType
# from DataBase import createSchema
# from Configuration import ExecutionMode, Settings
# from Runner import runTest
# from Utils import gpuMemory, skip_test
# from EndToEndTests.tpchQueries import get_tpch_query

class e2eTest():
    # compareEngine

    def __init__(self, bc, dask_client, drill, spark):
        self.bc = bc
        self.dask_client = dask_client
        self.drill = drill
        self.spark = spark

        self.worder = None
        self.use_percentage = None
        self.acceptable_difference = None
        self.orderby = None
        self.print_result = None

        self.setupTest()

    def setupTest(self):
        self.worder = 1
        self.use_percentage = False
        self.acceptable_difference = 0.01
        self.orderby = ""
        self.print_result = True





# def main(dask_client, drill, dir_data_lc, bc, nRals):
#     print("==============================")
#     print(queryType)
#     print("==============================")
#
#     sql = setup_test()
#     if sql:
#         start_mem = gpuMemory.capture_gpu_memory_usage()
#         executionTest(dask_client, drill, dir_data_lc, bc, nRals, sql)
#         end_mem = gpuMemory.capture_gpu_memory_usage()
#         gpuMemory.log_memory_usage(queryType, start_mem, end_mem)