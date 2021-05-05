from Configuration import Settings
from Utils import gpuMemory
from Runner import runTest
from DataBase import createSchema
from Configuration import ExecutionMode, Settings

from os import listdir
from os.path import isfile, join
import sql_metadata
import os
import yaml
import re
import copy

__all__ = ["TestCase", "ConfigTest"]

class ConfigTest():
    apply_order = None
    use_percentage = None
    acceptable_difference = None
    order_by_col = None
    print_result = None
    data_types = None
    compare_with = None
    skip_with = []

class TestCase():
    def __init__(self, name, dataTargetTest, globalConfig):
        self.name = name
        self.dataTargetTest = dataTargetTest
        self.data = None
        self.configGlobal = copy.deepcopy(globalConfig)
        self.configLocal = copy.deepcopy(globalConfig)
        self.tables = set()

        self.bc = None
        self.dask_client = None
        self.drill = None
        self.spark = None

        self.nRals = Settings.data["RunSettings"]["nRals"]
        self.dir_data_file = Settings.data["TestSettings"]["dataDirectory"]

        self.__loadFileSuite()
        self.__loadConfigTest()
        self.__loadTables()

    def __loadFileSuite(self):
        fileName = "EndToEndTests/TestSuites/" + self.dataTargetTest["FILE"]
        with open(fileName, 'r') as stream:
            self.data = yaml.safe_load(stream)["TEST_SUITE"]

    def __loadConfigTest(self):
        if "SETUP" in self.data:
            setup = self.data["SETUP"]

            if setup.get("SKIP_WITH") is not None: self.configLocal.skip_with = setup.get("SKIP_WITH")
            if setup.get("APPLY_ORDER") is not None: self.configLocal.apply_order = setup.get("APPLY_ORDER")
            if setup.get("ORDER_BY_COL") is not None: self.configLocal.order_by_col = setup.get("ORDER_BY_COL")
            if setup.get("PRINT_RESULT") is not None: self.configLocal.print_result = setup.get("PRINT_RESULT")
            if setup.get("COMPARE_WITH") is not None: self.configLocal.compare_with = setup.get("COMPARE_WITH")
            if setup.get("USE_PERCENTAGE") is not None: self.configLocal.use_percentage = setup.get("USE_PERCENTAGE")
            if setup.get("ACCEPTABLE_DIFFERENCE") is not None: self.configLocal.acceptable_difference = setup.get("ACCEPTABLE_DIFFERENCE")

            self.data.pop("SETUP", None)

    def __loadTables(self):
        queries = self.__getAllQueries()
        for query in queries:
            self.tables.update(sql_metadata.get_query_tables(query))

    def __getAllQueries(self):
        listCase = list(self.data.keys())
        if "SETUP" in listCase: listCase.remove("SETUP")

        queries = []
        for testCase in listCase:
            if "SQL" in self.data[testCase]:
                sql = self.data[testCase]["SQL"]
                sql = re.sub('\s{2,}', ' ', sql)
                queries.append(sql)

        return queries

    def __loadDataTypes(self):
        if self.nRals > 1:
            self.configLocal.data_types.remove(DataType.DASK_CUDF)

    def __loadTestCaseConfig(self, test_name, fileSchemaType):
        config = copy.deepcopy(self.configLocal)
        if "SETUP" in self.data[test_name]:
            setup = self.data[test_name]["SETUP"]

            if setup.get("SKIP_WITH") is not None: config.skip_with = setup.get("SKIP_WITH")
            if setup.get("APPLY_ORDER") is not None: config.apply_order = setup.get("APPLY_ORDER")
            if setup.get("ORDER_BY_COL") is not None: config.order_by_col = setup.get("ORDER_BY_COL")
            if setup.get("PRINT_RESULT") is not None: config.print_result = setup.get("PRINT_RESULT")
            if setup.get("COMPARE_WITH") is not None: config.compare_with = setup.get("COMPARE_WITH")
            if setup.get("USE_PERCENTAGE") is not None: config.use_percentage = setup.get("USE_PERCENTAGE")
            if setup.get("ACCEPTABLE_DIFFERENCE") is not None: config.acceptable_difference = setup.get("ACCEPTABLE_DIFFERENCE")

        if isinstance(config.compare_with, dict):
            formatList = list(config.compare_with.keys())
            ext = createSchema.get_extension(fileSchemaType)
            if ext.upper() in formatList:
                config.compare_with = config.compare_with[ext.upper()]
            else:
                config.compare_with = config.compare_with["OTHER"]

        return config

    def __executionTest(self):
        listCase = list(self.data.keys())

        print("######## Starting queries ...########")

        self.__loadDataTypes()

        for n in range(0, len(self.configLocal.data_types)):

            fileSchemaType = self.configLocal.data_types[n]

            createSchema.create_tables(self.bc, self.dir_data_file, fileSchemaType, tables=list(self.tables))

            for test_name in listCase:
                test_case = self.data[test_name]

                if Settings.execution_mode == ExecutionMode.GENERATOR:
                    print("==============================")
                    break_flag = True
                    break

                configTest = self.__loadTestCaseConfig(test_name, fileSchemaType)

                ext = createSchema.get_extension(fileSchemaType)
                if ext.upper() in configTest.skip_with:
                    continue

                query = test_case["SQL"]
                engine = self.drill if configTest.compare_with == "drill" else self.spark

                print("==>> Run query for test case", self.name)
                print("PLAN:")
                print(self.bc.explain(query, True))
                runTest.run_query(
                    self.bc,
                    engine,
                    query,
                    test_name,
                    self.name,
                    configTest.apply_order,
                    configTest.order_by_col,
                    configTest.acceptable_difference,
                    configTest.use_percentage,
                    fileSchemaType,
                    print_result=configTest.print_result
                )

    def run(self, bc, dask_client, drill, spark):
        self.bc = bc
        self.dask_client = dask_client
        self.drill = drill
        self.spark = spark

        start_mem = gpuMemory.capture_gpu_memory_usage()

        self.__executionTest()

        end_mem = gpuMemory.capture_gpu_memory_usage()
        gpuMemory.log_memory_usage(self.name, start_mem, end_mem)


