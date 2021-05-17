from blazingsql import DataType
from Runner import TestCase, ConfigTest

from os import listdir
from os.path import isfile, join
import yaml
import os

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

        self.__setupTest()
        self.__loadTargetTestDataFromFile()

    def __setupTest(self):
        self.globalConfig.apply_order = True
        self.globalConfig.use_percentage = False
        self.globalConfig.acceptable_difference = 0.01
        self.globalConfig.order_by_col = ""
        self.globalConfig.print_result = True
        self.globalConfig.compare_with = 'drill'
        self.globalConfig.spark_query = ""
        self.globalConfig.comparing = True
        self.globalConfig.message_validation = ""
        self.globalConfig.data_types = [
            DataType.DASK_CUDF,
            DataType.CUDF,
            DataType.CSV,
            DataType.PARQUET,
            DataType.ORC,
            DataType.JSON
        ]

    def __loadTargetTestDataFromFile(self):
        cwd = os.path.dirname(os.path.realpath(__file__))
        fileName = cwd + "/targetTest.yml"
        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                fileYaml = yaml.safe_load(stream)

            self.dataTestSuite = fileYaml["LIST_TEST"]
            return

        raise RuntimeError("ERROR: Runner/targetTest.yml not found")

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