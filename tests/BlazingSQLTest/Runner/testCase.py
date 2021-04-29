from Configuration import Settings
from Utils import gpuMemory
from Runner import runner

import os
import yaml

__all__ = ["TestCase", "ConfigTest"]

class ConfigTest():
    worder = None
    use_percentage = None
    acceptable_difference = None
    orderby = None
    print_result = None
    data_types = None

class TestCase():
    def __init__(self, name, configFile, default):
        self.name = name
        self.configFile = configFile
        self.data = None
        self.defaultConfig = default
        self.config = ConfigTest()

        self.bc = None
        self.dask_client = None
        self.drill = None
        self.spark = None

        self.__loadConfig()

    def __loadConfig(self):
        if os.path.isfile(self.configFile):
            with open(self.configFile, 'r') as stream:
                self.data = yaml.safe_load(stream)[self.name]

        if "config" in self.data:
            if "worder" in self.data["config"]:
                self.config.apply_order = self.data["config"]["apply_order"]
            if "use_percentage" in self.data["config"]:
                self.config.use_percentage = self.data["config"]["use_percentage"]
            if "acceptable_difference" in self.data["config"]:
                self.config.acceptable_difference = self.data["config"]["acceptable_difference"]
            if "orderby" in self.data["config"]:
                self.config.orderby = self.data["config"]["orderby"]
            if "print_result" in self.data["config"]:
                self.config.print_result = self.data["config"]["print_result"]
            if "data_types" in self.data["config"]:
                self.config.data_types = self.data["config"]["data_types"]

    def __loadConfigQuery(self, name):
        configQuery = ConfigTest()
        if "config" in self.data["listTest"][name]:
            if "worder" in self.data["listTest"][name]["config"]:
                configQuery.apply_order = self.data["config"]["apply_order"]
            if "use_percentage" in self.data["listTest"][name]["config"]:
                configQuery.use_percentage = self.data["listTest"][name]["config"]["use_percentage"]
            if "acceptable_difference" in self.data["listTest"][name]["config"]:
                configQuery.acceptable_difference = self.data["listTest"][name]["config"]["acceptable_difference"]
            if "orderby" in self.data["listTest"][name]["config"]:
                configQuery.orderby = self.data["listTest"][name]["config"]["orderby"]
            if "print_result" in self.data["listTest"][name]["config"]:
                configQuery.print_result = self.data["listTest"][name]["config"]["print_result"]
            if "data_types" in self.data["listTest"][name]["config"]:
                configQuery.data_types = self.data["listTest"][name]["config"]["data_types"]

        return configQuery

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


