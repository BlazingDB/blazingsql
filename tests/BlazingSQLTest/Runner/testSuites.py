from blazingsql import DataType
from Runner import TestCase, ConfigTest

from os import listdir
from os.path import isfile, join
import yaml
import os
import sys

__all__ = ["TestSuites"]

class TestSuites():
    def __init__(self, bc, dask_client, drill, spark):
        self.bc = bc
        self.dask_client = dask_client
        self.drill = drill
        self.spark = spark
        self.targetTestList = []
        self.tables = set()

        self.globalConfig = ConfigTest()
        self.dataTestSuite = None

        self.__loadDefaultConfigFromFile()
        self.__loadTargetTestDataFromFile()

    def __loadDefaultConfigFromFile(self):
        cwd = os.path.dirname(os.path.realpath(__file__))
        if "-config_file" in sys.argv and len(sys.argv) >= 3:
            fileName = cwd + "/" + sys.argv[2]
        else:
            fileName = cwd + "/config.yaml"

        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                fileYaml = yaml.safe_load(stream)
        else:
            raise Exception("Error: " + fileName + " not exist")

        if "SETUP" in fileYaml:
            setup = fileYaml["SETUP"]

            if setup.get("COMPARING") is not None: self.globalConfig.comparing = setup.get("COMPARING")
            if setup.get("APPLY_ORDER") is not None: self.globalConfig.apply_order = setup.get("APPLY_ORDER")
            if setup.get("PRINT_RESULT") is not None: self.globalConfig.print_result = setup.get("PRINT_RESULT")
            if setup.get("COMPARE_WITH") is not None: self.globalConfig.compare_with = setup.get("COMPARE_WITH")
            if setup.get("USE_PERCENTAGE") is not None: self.globalConfig.use_percentage = setup.get("USE_PERCENTAGE")
            if setup.get("ACCEPTABLE_DIFFERENCE") is not None: self.globalConfig.acceptable_difference = setup.get("ACCEPTABLE_DIFFERENCE")
            if setup.get("DATA_TYPES") is not None: self.globalConfig.data_types = setup.get("DATA_TYPES")
            self.globalConfig.data_types = [DataType[item] for item in self.globalConfig.data_types]

    def __loadTargetTestDataFromFile(self):
        cwd = os.path.dirname(os.path.realpath(__file__))
        fileName = cwd + "/targetTest.yaml"
        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                fileYaml = yaml.safe_load(stream)

            self.dataTestSuite = fileYaml["LIST_TEST"]
            return

        raise RuntimeError("ERROR: Runner/targetTest.yaml not found")

    def __existTestData(self, test):
        cwd = os.path.dirname(os.path.realpath(__file__))
        fileName = cwd + "/../EndToEndTests/TestSuites/"
        if self.dataTestSuite is not None:
            if "FILE" in self.dataTestSuite[test] and self.dataTestSuite[test]["FILE"] is not None:
                fileName += self.dataTestSuite[test]["FILE"]
            else:
                print("ERROR: " + test + " configuration not found in targetTest.yaml, i.e. FILE: " + test + ".yaml")
                return False

        if os.path.isfile(fileName):
            return True

        print("ERROR: " + fileName + " file not found")
        return False

    def setTargetTest(self, testList):
        self.targetTestList = testList

    def runE2ETest(self):
        if len(self.targetTestList) == 0:
            self.targetTestList = list(self.dataTestSuite.keys())

        for testSuiteName in self.targetTestList:
            if self.__existTestData(testSuiteName):
                testCase = TestCase(testSuiteName, self.dataTestSuite[testSuiteName], self.globalConfig)
                testCase.run(self.bc, self.dask_client, self.drill, self.spark)