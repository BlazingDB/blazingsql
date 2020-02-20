import time
import pprint
from blazingsql import BlazingContext
from dask.distributed import Client

from collections import OrderedDict 
from blazingsql import BlazingContext


client = Client('127.0.0.1:8786')
bc = BlazingContext(dask_client=client, network_interface="lo")

bc.create_table('customer', '/home/aocsa/tpch/100MB2Part/tpch/customer_0_0.parquet')
bc.create_table('orders', '/home/aocsa/tpch/100MB2Part/tpch/orders_*.parquet')

algebra = """LogicalProject(c_custkey=[$0], c_nationkey=[$3])
  LogicalFilter(condition=[<($0, 10)])
    LogicalTableScan(table=[[main, customer]])
"""

plan = """
{
		'expr': 'LogicalProject(c_custkey=[$0], c_nationkey=[$3])',
		'children': [
			{
				'expr': 'LogicalFilter(condition=[<($0, 10)])',
				'children': [
					{
						'expr': 'LogicalTableScan(table=[[main, customer]])',
						'children': []
					}
				]
			}
		]
}
"""

ddf = bc.execute('select c_custkey, c_nationkey from customer where c_custkey < 10', algebra)

print(ddf.compute())