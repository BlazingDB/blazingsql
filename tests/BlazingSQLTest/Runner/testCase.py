from Configuration import Settings
from Utils import gpuMemory
from Runner import runner

from os import listdir
from os.path import isfile, join
import sql_metadata
import os
import yaml

__all__ = ["TestCase", "ConfigTest"]

class ConfigTest():
    apply_order = None
    use_percentage = None
    acceptable_difference = None
    orderby = None
    print_result = None
    data_types = None

class TestCase():
    def __init__(self, name, dataTargetTest, globalConfig):
        self.name = name
        self.dataTargetTest = dataTargetTest
        self.data = None
        self.configGlobal = globalConfig

        self.bc = None
        self.dask_client = None
        self.drill = None
        self.spark = None

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

            if setup.get("ORDERBY") is not None: self.configGlobal.apply_order = setup.get("ORDERBY")
            if setup.get("SKIP_WITH") is not None: self.configGlobal.apply_order = setup.get("SKIP_WITH")
            if setup.get("APPLY_ORDER") is not None: self.configGlobal.apply_order = setup.get("APPLY_ORDER")
            if setup.get("COMPARE_WITH") is not None: self.configGlobal.apply_order = setup.get("COMPARE_WITH")
            if setup.get("USE_PERCENTAGE") is not None: self.configGlobal.apply_order = setup.get("USE_PERCENTAGE")
            if setup.get("ACCEPTABLE_DIFFERENCE") is not None: self.configGlobal.apply_order = setup.get("ACCEPTABLE_DIFFERENCE")

    def __loadTables(self):
        queries = __getAllQueries()
        for query in queries:
            self.tables.update(sql_metadata.get_query_tables(query))

    def __getAllQueries(self):
        dir = "EndToEndTests/TestSuites/"
        onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]
        print(onlyfiles)
        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                queriesYaml = yaml.safe_load(stream)

    def run(self, bc, dask_client, drill, spark):
        self.bc = bc
        self.dask_client = dask_client
        self.drill = drill
        self.spark = spark

        dir_data_file = Settings.data["TestSettings"]["dataDirectory"]
        nRals = Settings.data["RunSettings"]["nRals"]

        start_mem = gpuMemory.capture_gpu_memory_usage()
        runner.executionTest(bc, dask_client, drill, spark, nRals, dir_data_file, self.name)
        end_mem = gpuMemory.capture_gpu_memory_usage()
        gpuMemory.log_memory_usage(self.name, start_mem, end_mem)


