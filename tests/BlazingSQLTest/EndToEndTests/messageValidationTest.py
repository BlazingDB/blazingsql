from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test

queryType = "Message Validation"


def main(dask_client, drill, dir_data_file, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():

        tables = ["customer", "orders", "nation"]
        data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.ORC,
            DataType.PARQUET,
        ]  # TODO json

        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            print(fileSchemaType)
            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            # Run Query ------------------------------------------------------
            # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

            queryId = "TEST_01"
            query = """select c_custkeynew, c_nationkey, c_acctbal 
                    from customer where c_custkey < 15"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                message_validation=True,
            )

            queryId = "TEST_02"
            query = """select c_custkey, c_nationkey, c_acctbal 
                    from customer1 where c_custkey < 150 
                    and c_nationkey = 5"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                message_validation=True,
            )

            queryId = "TEST_03"
            query = """select maxi(c_custkey), c_nationkey as nkey 
                    from customer where c_custkey < 0"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                message_validation=True,
            )

            queryId = "TEST_04"
            query = """select max(c_custkey) c_nationkey as nkey 
                    from customer where c_custkey < 0"""
            runTest.run_query(
                bc,
                drill,
                query,
                queryId,
                queryType,
                worder,
                "c_custkey",
                acceptable_difference,
                use_percentage,
                fileSchemaType,
                message_validation=True,
            )

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                print("==============================")
                break

    executionTest()

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    drill = "drill"

    # Create Context For BlazingSQL

    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
