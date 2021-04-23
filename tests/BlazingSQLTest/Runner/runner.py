import yaml

from DataBase import createSchema as cs
from Runner import runTest
from blazingsql import DataType

class TestCase:

    def __init__(self, testcase_id, sql, acceptable_difference, compare_with, order, **kwargs):
        self.testcase_id = testcase_id
        self.sql = sql
        self.acceptable_difference = acceptable_difference
        self.compare_with = compare_with
        self.order = order
        self.del_dtype = kwargs.get("del_dtype", "")

class Runner:

    def __init__(self, testSuite, dir_data, nrals, **kwargs):
        self.testSuite = testSuite
        self.dir_data = dir_data
        self.nrals = nrals
        self.drill = kwargs.get("drill", "")
        self.spark = kwargs.get("spark", "")
        
    def get_test_cases(testSuite):
        with open(r'../EndToEndTests/TestSuites/'+testSuite+'.yaml') as file:
            yaml_dict = yaml.load(file, Loader=yaml.FullLoader)
        
        testSuite_dict = yaml_dict["TEST_SUITE"]

        testCases_list = []
        
        for testCase_id in testSuite_dict:
            testcase_dict = testSuite_dict[key]
            testCases_list.append(TestCase(testCase_id,
                                  testcase_dict["SQL"],
                                  testcase_dict["ACCEPTABLE_DIFFERENCE"],
                                  testcase_dict["COMPARE_WITH"],
                                  testcase_dict["ORDER"]))

        return testCases_list

    def datasources(dask_client, nRals):
        for fileSchemaType in data_types:
            if skip_test(dask_client, nRals, fileSchemaType, queryType):
                continue
            yield fileSchemaType

    
    def datasources(nrals, del_dtype):
        data_types = [
                DataType.DASK_CUDF,
                DataType.CUDF,
                DataType.CSV,
                DataType.ORC,
                DataType.PARQUET,
            ]

        if nrals > 1 and not del_dtype =="": 
            data_types.remove(del_dtype)
        
        return data_types

    def run_queries(bc, dask_client, drill, spark, queryType, **kwargs):

        test_cases_list = get_test_cases(self.testSuite)

        print("######## Starting queries ...########")

        data_types = datasources(sel.nrals, self.del_dtype)

        for n in range(0,len(data_types)):

            fileSchemaType = data_types[n]

            cs.create_tables(bc, dir_data_file, fileSchemaType, tables=tables)

            for x in range(0,len(test_cases_list)):
                test_case = test_cases_list[x]

                if Settings.execution_mode == ExecutionMode.GENERATOR:
                    print("==============================")
                    break_flag = True
                    break

                query = test_case.sql
                worder = test_case.order
                use_percentage = test_case.use_percentage
                acceptable_difference = test_case.acceptable_difference
                engine = test_case.compare_with
                test_suite = self.testSuite

                print("==>> Run query for test case", testcase_id)
                print("PLAN:")
                print(bc.explain(query, True))
                runTest.run_query(
                    bc,
                    engine,
                    query,
                    test_case.testCase_id,
                    test_suite,
                    worder,
                    "",
                    acceptable_difference,
                    use_percentage,
                    fileSchemaType,
                    print_result = True
                )







def executionTest(bc, dask_client, drill, spark, nRals, dir_data, testSuite):

    runner = Runner(nRals, testSuite, dir_data)

    runner.run_queries(bc, dask_client, drill, spark, testSuite)
