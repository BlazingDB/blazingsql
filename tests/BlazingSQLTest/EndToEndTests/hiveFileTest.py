from blazingsql import DataType
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context, skip_test, utilityHive

queryType = "Hive File"


def executionTestAuto():
    tables = ["orders", "customer"]
    # data_types = [DataType.CSV, DataType.PARQUET, DataType.ORC]
    data_types = [DataType.PARQUET]

    # Create Tables -----------------------------------------------------
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue

        # Create hive partitions
        createPartitions(fileSchemaType, dir_data_file)
        return
        location = "/tmp/BlazingSQL/partitions/utilityHive"
        # cs.create_hive_partitions_tables(bc=bc,
        #                                  dir_partitions=location,
        #                                  fileSchemaType=fileSchemaType,
        #                                  createTableType=cs.HiveCreateTableType.AUTO,
        #                                  partitions={},
        #                                  partitions_schema=[],
        #                                  tables=tables)

        # cs.create_hive_partitions_tables(bc=bc,
        #                                  dir_partitions=location,
        #                                  fileSchemaType=fileSchemaType,
        #                                  createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
        #                                  partitions={
        #                                          'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'],
        #                                          'o_orderstatus': ['F', 'O', 'P'],
        #                                          'o_shippriority': [0]},
        #                                  partitions_schema=[('o_orderpriority', 'str'),
        #                                                     ('o_orderstatus', 'str'),
        #                                                     ('o_shippriority', 'int')],
        #                                  tables=tables)

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
        query = "select min(o_orderdate), max(o_orderdate) from orders where o_custkey between 10 and 20"
        query_spark = "select min(o_orderdate), max(o_orderdate) from orders where o_custkey between 10 and 20"
        runTest.run_query(
            bc,
            spark,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            use_percentage,
            fileSchemaType,
            query_spark=query_spark,
        )

        if Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break

def executionTestWithPartitions():
    tables = ["orders", "customer"]
    # data_types = [DataType.CSV, DataType.PARQUET, DataType.ORC]
    data_types = [DataType.PARQUET]

    # Create Tables -----------------------------------------------------
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue

        # Create hive partitions
        createPartitions(fileSchemaType, dir_data_file)
        return
        location = "/tmp/BlazingSQL/partitions/utilityHive"
        # cs.create_hive_partitions_tables(bc=bc,
        #                                  dir_partitions=location,
        #                                  fileSchemaType=fileSchemaType,
        #                                  createTableType=cs.HiveCreateTableType.AUTO,
        #                                  partitions={},
        #                                  partitions_schema=[],
        #                                  tables=tables)

        # cs.create_hive_partitions_tables(bc=bc,
        #                                  dir_partitions=location,
        #                                  fileSchemaType=fileSchemaType,
        #                                  createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
        #                                  partitions={
        #                                          'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'],
        #                                          'o_orderstatus': ['F', 'O', 'P'],
        #                                          'o_shippriority': [0]},
        #                                  partitions_schema=[('o_orderpriority', 'str'),
        #                                                     ('o_orderstatus', 'str'),
        #                                                     ('o_shippriority', 'int')],
        #                                  tables=tables)

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
        query = "select min(o_orderdate), max(o_orderdate) from orders where o_custkey between 10 and 20"
        query_spark = "select min(o_orderdate), max(o_orderdate) from orders where o_custkey between 10 and 20"
        runTest.run_query(
            bc,
            spark,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            use_percentage,
            fileSchemaType,
            query_spark=query_spark,
        )

        if Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break

def executionTestWithSomePartitions():
    tables = ["orders", "customer"]
    # data_types = [DataType.CSV, DataType.PARQUET, DataType.ORC]
    data_types = [DataType.PARQUET]

    # Create Tables -----------------------------------------------------
    for fileSchemaType in data_types:
        if skip_test(dask_client, nRals, fileSchemaType, queryType):
            continue

        # Create hive partitions
        createPartitions(fileSchemaType, dir_data_file)
        return
        location = "/tmp/BlazingSQL/partitions/utilityHive"
        # cs.create_hive_partitions_tables(bc=bc,
        #                                  dir_partitions=location,
        #                                  fileSchemaType=fileSchemaType,
        #                                  createTableType=cs.HiveCreateTableType.AUTO,
        #                                  partitions={},
        #                                  partitions_schema=[],
        #                                  tables=tables)

        # cs.create_hive_partitions_tables(bc=bc,
        #                                  dir_partitions=location,
        #                                  fileSchemaType=fileSchemaType,
        #                                  createTableType=cs.HiveCreateTableType.WITH_PARTITIONS,
        #                                  partitions={
        #                                          'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'],
        #                                          'o_orderstatus': ['F', 'O', 'P'],
        #                                          'o_shippriority': [0]},
        #                                  partitions_schema=[('o_orderpriority', 'str'),
        #                                                     ('o_orderstatus', 'str'),
        #                                                     ('o_shippriority', 'int')],
        #                                  tables=tables)

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
        query = "select min(o_orderdate), max(o_orderdate) from orders where o_custkey between 10 and 20"
        query_spark = "select min(o_orderdate), max(o_orderdate) from orders where o_custkey between 10 and 20"
        runTest.run_query(
            bc,
            spark,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            use_percentage,
            fileSchemaType,
            query_spark=query_spark,
        )

        if Settings.execution_mode == ExecutionMode.GENERATOR:
            print("==============================")
            break

def main(dask_client, spark, dir_data_file, bc, nRals):
    start_mem = gpuMemory.capture_gpu_memory_usage()

    def executionTest():
        tables = ["orders", "customer"]
        data_types = [DataType.CSV, DataType.PARQUET, DataType.ORC]

        # Create Tables -----------------------------------------------------
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue

            # Run Query ------------------------------------------------------
            # Parameter to indicate if its necessary to order
            # the resulsets before compare them
            worder = 1
            use_percentage = False
            acceptable_difference = 0.01

            print("==============================")
            print(queryType)
            print("==============================")

    executionTestAuto()
    executionTestWithPartitions()
    executionTestWithSomePartitions()

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)

    #remove paths


def createPartitions(fileSchemaType, dir_data_file):
    dir_data = dir_data_file + "/tpch"

    ext = cs.get_extension(fileSchemaType)

    # orders table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "orders", ext),
                                           table_name="orders",
                                           partitions={
                                               'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED',
                                                                   '5-LOW'],
                                               'o_orderstatus': ['F', 'O', 'P']},
                                           partitions_schema=[('o_orderpriority', 'str'),
                                                              ('o_orderstatus', 'str')],
                                           output='/tmp/BlazingSQL/partitions/' + ext + '/orders/',
                                           num_files=4)

    # customer table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "customer", ext),
                                           table_name="customer",
                                           partitions={
                                               'c_nationkey': [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24],
                                               'c_mktsegment': ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY']},
                                         partitions_schema=[('c_nationkey', 'int32'),
                                                            ('c_mktsegment', 'str')],
                                           output='/tmp/BlazingSQL/partitions/' + ext + '/customer/',
                                           num_files=4)

    # lineitem table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "lineitem", ext),
                                           table_name="lineitem",
                                           partitions={
                                                'l_shipmode':['AIR','FOB','MAIL','RAIL','REG','AIR','SHIP','TRUCK'],
                                                'l_linestatus':['F','O'],
                                                'l_returnflag':['A','N','R']
                                           },
                                         partitions_schema=[('l_shipmode', 'str'),
                                                            ('l_linestatus', 'str'),
                                                            ('l_returnflag', 'str')],
                                           output='/tmp/BlazingSQL/partitions/' + ext + '/lineitem/',
                                           num_files=4)

    # nation table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "nation", ext),
                                           table_name="nation",
                                           partitions={
                                               'n_regionkey': [0,1,2,3,4]},
                                         partitions_schema=[('n_regionkey', 'int32')],
                                           output='/tmp/BlazingSQL/partitions/' + ext + '/nation/',
                                           num_files=4)

    # part table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "part", ext),
                                           table_name="part",
                                           partitions={
                                               'p_container': ['JUMBO BAG', 'JUMBO BOX', 'JUMBO CAN', 'JUMBO CASE',
                                                               'JUMBO DRUM', 'JUMBO JAR', 'JUMBO PACK', 'JUMBO PKG',
                                                               'LG BAG', 'LG BOX', 'LG CAN', 'LG CASE', 'LG DRUM',
                                                               'LG JAR', 'LG PACK', 'LG PKG', 'MED BAG', 'MED BOX',
                                                               'MED CAN', 'MED CASE', 'MED DRUM', 'MED JAR', 'MED PACK',
                                                               'MED PKG', 'SM BAG', 'SM BOX', 'SM CAN', 'SM CASE',
                                                               'SM DRUM', 'SM JAR', 'SM PACK', 'SM PKG', 'WRAP BAG',
                                                               'WRAP BOX', 'WRAP CAN', 'WRAP CASE', 'WRAP DRUM',
                                                               'WRAP JAR', 'WRAP PACK', 'WRAP PKG']},
                                         partitions_schema=[('p_container', 'str')],
                                           output='/tmp/BlazingSQL/partitions/' + ext + '/part/',
                                           num_files=4)

    # supplier table
    utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "supplier", ext),
                                           table_name="supplier",
                                           partitions={
                                               's_nationkey': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]},
                                         partitions_schema=[('s_nationkey', 'int32')],
                                           output='/tmp/BlazingSQL/partitions/' + ext + '/supplier/',
                                           num_files=4)

    # order table with datetime
    # utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "orders", ext),
    #                                      table_name="orders",
    #                                      partitions={
    #                                          'o_orderdate': [694224000000, 694310400000, 694396800000, 694483200000, 694569600000,
    #                                                          694656000000, 694742400000, 694828800000, 694915200000, 695001600000]},
    #                                      partitions_schema=[('o_orderdate', 'timestamp')],
    #                                      output='/tmp/BlazingSQL/partitions/' + ext + '/ordersDatetime/',
    #                                      num_files=4)

    # '1992-01-01 00:00:00', '1992-01-02 00:00:00',
    # '1992-01-03 00:00:00', '1992-01-04 00:00:00',
    # '1992-01-05 00:00:00', '1992-01-06 00:00:00',
    # '1992-01-07 00:00:00', '1992-01-08 00:00:00',
    # '1992-01-09 00:00:00', '1992-01-10 00:00:00',
    # '1992-01-11 00:00:00', '1992-01-12 00:00:00',
    # '1992-01-13 00:00:00', '1992-01-14 00:00:00',
    # '1992-01-15 00:00:00', '1992-01-16 00:00:00',
    # '1992-01-17 00:00:00', '1992-01-18 00:00:00',
    # '1992-01-19 00:00:00', '1992-01-20 00:00:00',
    # '1992-01-21 00:00:00', '1992-01-22 00:00:00',
    # '1992-01-23 00:00:00', '1992-01-24 00:00:00',
    # '1992-01-25 00:00:00', '1992-01-26 00:00:00',
    # '1992-01-27 00:00:00', '1992-01-28 00:00:00',
    # '1992-01-29 00:00:00', '1992-01-30 00:00:00',
    # '1992-01-31 00:00:00'
# 694224000, 694310400, 694396800, 694483200, 694569600, 694656000, 694742400, 694828800, 694915200, 695001600
    # supplier table with Boolean
    # utilityHive.test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "supplier", ext),
    #                                      table_name="supplier",
    #                                      partitions={
    #                                          's_nationkey': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    #                                                          17, 18, 19, 20, 21, 22, 23, 24]},
    #                                      partitions_schema=[('s_nationkey', 'int32')],
    #                                      output='/tmp/BlazingSQL/partitions/' + ext + '/supplier/',
    #                                      num_files=4)

    return '/tmp/BlazingSQL/partitions/' + ext + '/'

if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    spark = "spark"

    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if (
        Settings.execution_mode == ExecutionMode.FULL and compareResults == "true"
    ) or Settings.execution_mode == ExecutionMode.GENERATOR:

        # Create Table Spark -------------------------------------------------
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("timestampTest").getOrCreate()
        cs.init_spark_schema(spark, Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL
    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(
        dask_client, spark, Settings.data["TestSettings"]["dataDirectory"], bc, nRals,
    )

    if Settings.execution_mode != ExecutionMode.GENERATOR:
        runTest.save_log()
        gpuMemory.print_log_gpu_memory()
