from blazingsql import DataType
# from DataBase import createSchema
# from Configuration import ExecutionMode, Settings
# from Runner import runTest
# from Utils import gpuMemory, skip_test
# from EndToEndTests.tpchQueries import get_tpch_query
import sql_metadata
import yaml
import os

def getAllQueries():
    return [
        "select case when o_custkey > 20 then o_orderkey else o_custkey - 20 end from orders where o_orderkey <= 50",
        "select count(p_partkey), sum(p_partkey),avg(CAST(p_partkey AS DOUBLE)), max(p_partkey), min(p_partkey) from part",
        "select * from part, orders",
        "select * from part, orders where part.p_partkey = 1",
        "select * from part p, orders as o",
        "select * from part as p, orders o",
        "select * from part as p, orders as o",
        "select * from part p, orders o",
        "select avg(CAST(c.c_custkey AS DOUBLE)),avg(c.c_acctbal), n.n_nationkey,r.r_regionkey from customer as c inner join nation as n on c.c_nationkey = n.n_nationkey inner join region as r on r.r_regionkey = n.n_regionkey group by n.n_nationkey, r.r_regionkey",
        "select maxPrice,avgSize from ( select max(p_retailprice) as maxPrice, max(p_retailprice) as avgSize from part) as partAnalysis order by maxPrice, avgSize",
        "select maxPrice from ( select max(p_retailprice) as maxPrice from part) as partAnalysis, orders order by maxPrice, avgSize",
        "select o_orderkey, o_totalprice,l_linenumber, l_shipmode from orders cross join lineitem where o_orderkey < 6 and l_receiptdate > date '1996-07-12' and l_linenumber > 5 and o_totalprice < 74029.55 and o_clerk = 'Clerk#000000880' and l_shipmode IN ('FOB', 'RAIL') order by o_orderkey, o_totalprice, l_linenumber",
        "select COUNT(DISTINCT(n.n_nationkey)), AVG(r.r_regionkey) from nation as n left outer join region as r on n.n_nationkey = r.r_regionkey",
        "select maxPrice, avgSize from ( select avg(CAST(p_size AS DOUBLE)) as avgSize, max(p_retailprice) as maxPrice, min(p_retailprice) as minPrice from part ) as partAnalysis order by maxPrice, avgSize",
        "with ordersTemp as (select min(o_orderkey) as priorityKey, o_custkey from orders group by o_custkey ), ordersjoin as( select orders.o_custkey from orders inner join ordersTemp on ordersTemp.priorityKey = orders.o_orderkey) select customer.c_custkey, customer.c_nationkey from customer inner join ordersjoin on ordersjoin.o_custkey =  customer.c_custkey",
        "select l.l_orderkey, l.l_linenumber from lineitem as l inner join orders as o on l.l_orderkey = o.o_orderkey and l.l_commitdate < o.o_orderdate and l.l_receiptdate > o.o_orderdate"
    ]

class e2eTest():
    # compareEngine

    def __init__(self, bc, dask_client, drill, spark):
        self.bc = bc
        self.dask_client = dask_client
        self.drill = drill
        self.spark = spark
        self.targetTestList = []

        self.worder = None
        self.use_percentage = None
        self.acceptable_difference = None
        self.orderby = None
        self.print_result = None
        self.tables = None
        self.data_types = None

        self.__setupTest()
        self.__loadTables()

    def __setupTest(self):
        self.worder = 1
        self.use_percentage = False
        self.acceptable_difference = 0.01
        self.orderby = ""
        self.print_result = True
        self.tables = set()
        self.data_types = [
            DataType.CSV,
            DataType.PARQUET,
            DataType.ORC
        ]

    def __loadTables(self):
        queries = getAllQueries()
        for query in queries:
            self.tables.update(sql_metadata.get_query_tables(query))

    def __loadTargetTestFromFile(self):
        fileName = "Runner/targetTest.yml"
        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                testListYaml = yaml.load(stream)

            return testListYaml["test"]
        return []

    def __existTestData(self, test):
        fileName = "Runner/queries.yml"
        if os.path.isfile(fileName):
            with open(fileName, 'r') as stream:
                queriesYaml = yaml.load(stream)

            if test in queriesYaml:
                return True

        return False

    def setTargetTest(self, testList):
        self.targetTestList = testList

    def runE2ETest(self):
        if len(self.targetTestList) == 0:
            self.targetTestList = self.__loadTargetTestFromFile()

        for test in self.targetTestList:
            if self.__existTestData(test):
                a = 10




# def main(dask_client, drill, dir_data_lc, bc, nRals):
#     print("==============================")
#     print(queryType)
#     print("==============================")
#
#     sql = setup_test()
#     if sql:
#         start_mem = gpuMemory.capture_gpu_memory_usage()
#         executionTest(dask_client, drill, dir_data_lc, bc, nRals, sql)
#         end_mem = gpuMemory.capture_gpu_memory_usage()
#         gpuMemory.log_memory_usage(queryType, start_mem, end_mem)