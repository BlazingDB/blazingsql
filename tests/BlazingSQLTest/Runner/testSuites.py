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
        self.compare_engine = "spark"

        self.config = ConfigTest()
        self.dataTestSuite = None

        self.__setupTest()
        self.__loadTargetTestDataFromFile()

    def __setupTest(self):
        self.config.apply_order = True
        self.config.use_percentage = False
        self.config.acceptable_difference = 0.01
        self.config.orderby = ""
        self.config.print_result = True
        self.config.data_types = [
            DataType.CSV,
            DataType.PARQUET,
            DataType.ORC
        ]

    def __loadTargetTestDataFromFile(self):
        fileName = "Runner/targetTest.yml"
        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                fileYaml = yaml.safe_load(stream)

            self.dataTestSuite = fileYaml["LIST_TEST"]
            return

        raise RuntimeError("ERROR: Runner/targetTest.yml not found")

    def __existTestData(self, test):
        fileName = "EndToEndTests/TestSuites/"
        if self.dataTestSuite is not None:
            if "FILE" in self.dataTestSuite[test]:
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

        for testSuite in self.targetTestList:
            if self.__existTestData(testSuite):
                testCase = TestCase(test, "EndToEndTests/TestSuites/" + test + ".yaml", self.config)
                testCase.run(self.bc, self.dask_client, self.drill, self.spark)