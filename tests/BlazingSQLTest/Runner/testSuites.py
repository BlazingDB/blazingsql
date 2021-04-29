from blazingsql import DataType
from Runner import TestCase, ConfigTest

import sql_metadata
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

        self.config = ConfigTest()

        self.__setupTest()

    def __setupTest(self):
        self.config.worder = 1
        self.config.use_percentage = False
        self.config.acceptable_difference = 0.01
        self.config.orderby = ""
        self.config.print_result = True
        self.config.data_types = [
            DataType.CSV,
            DataType.PARQUET,
            DataType.ORC
        ]

    def __loadTargetTestFromFile(self):
        fileName = "Runner/targetTest.yml"
        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                testListYaml = yaml.safe_load(stream)

            return testListYaml["test"]
        return []

    def __existTestData(self, test):
        fileName = "EndToEndTests/TestSuites/" + test + ".yaml"
        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                queriesYaml = yaml.safe_load(stream)

            return True

        return False

    def setTargetTest(self, testList):
        self.targetTestList = testList

    def runE2ETest(self):
        if len(self.targetTestList) == 0:
            self.targetTestList = self.__loadTargetTestFromFile()

        for test in self.targetTestList:
            if self.__existTestData(test):
                testCase = TestCase(test, "EndToEndTests/TestSuites/" + test + ".yaml", self.config)
                testCase.run(self.bc, self.dask_client, self.drill, self.spark)