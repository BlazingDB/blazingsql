import time
import pprint
from blazingsql import BlazingContext
from dask.distributed import Client

from collections import OrderedDict 
from blazingsql import BlazingContext

import json
import collections

def visit (lines): 
	stack = collections.deque()
	root_level = 0
	dicc = {
		"expr": lines[root_level][1],
		"children": []
	}
	processed = set()
	for index in range(len(lines)): 
		child_level, expr = lines[index]
		if child_level == root_level + 1:
			new_dicc = {
				"expr": expr,
				"children": []
			}
			if len(dicc["children"]) == 0:
				dicc["children"] = [new_dicc]
			else:
				dicc["children"].append(new_dicc)
			stack.append( (index, child_level, expr, new_dicc) )
			processed.add(index)

	for index in processed:
		lines[index][0] = -1 

	while len(stack) > 0: 
		curr_index, curr_level, curr_expr, curr_dicc = stack.pop()
		processed = set()

		if curr_index < len(lines)-1: # is brother
			child_level, expr = lines[curr_index+1]
			if child_level == curr_level:
				continue
			elif child_level == curr_level + 1:
				index = curr_index + 1				
				while index < len(lines) and child_level == curr_level + 1: 
					child_level, expr = lines[index]
					if child_level != curr_level + 1:
						continue
					first = index
					new_dicc = {
						"expr": expr,
						"children": []
					}
					if len(curr_dicc["children"]) == 0:
						curr_dicc["children"] = [new_dicc]
					else:
						curr_dicc["children"].append(new_dicc)
					processed.add(index)
					stack.append( (index, child_level, expr, new_dicc) )
					index += 1
		for index in processed:
			lines[index][0] = -1 
	return json.dumps(dicc)


def get_plan(algebra):
	algebra = algebra.replace("  ", "\t")
	lines = algebra.split("\n")
	new_lines = [] 
	for i in range(len(lines) - 1):
		line = lines[i]
		level = line.count("\t")
		new_lines.append( [level, line.replace("\t", "")] )

	return visit(new_lines)


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

print(get_plan(algebra))

ddf = bc.execute('select c_custkey, c_nationkey from customer where c_custkey < 10', algebra, get_plan(algebra))

print(ddf.compute())