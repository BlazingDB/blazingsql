# Detectar diferentes tipos de datos
# deteccion automatica
# Recibir varios formatos de archivo

# /root_path/t_year=2017/t_company_id=2/region=asia/
# /root_path/t_year=2017/t_company_id=4/region=asia/
# /root_path/t_year=2017/t_company_id=6/region=asia/
# /root_path/t_year=2018/t_company_id=2/region=asia/
# /root_path/t_year=2018/t_company_id=4/region=asia/
# /root_path/t_year=2018/t_company_id=6/region=asia/

from blazingsql import BlazingContext
import os
import math
from itertools import combinations

__all__ = ["create_hive_partition_data", "test_hive_partition_data"]

def _select_columns(columns, remove_columns):
	for item in remove_columns:
		columns.remove(item)

	return ','.join(columns)

def _get_partition_values(data_partition_array_dict):
	values = []
	for item in data_partition_array_dict:
		values.append(list(item)[0])

	return values

def _get_columns_names(bc, table_name):
	columns = []
	columns_info = bc.describe_table(table_name)
	for key in columns_info:
		columns.append(key)

	return columns

def _save_parquet(bc, query, output, partition, partition_value, filename):
	result = bc.sql(query)

	path = '{}/{}={}'.format(output, partition, partition_value)
	if not os.path.exists(path):
		os.mkdir(path)

	pdf = result.to_pandas()
	output_name = path + "/" + filename + '.parquet'
	pdf.to_parquet(output_name, compression="GZIP")

def _save_partition_files(bc, table_name, data_partition_array_dict, output, num_files_per_parquet):
	values_partitions = _get_partition_values(data_partition_array_dict)
	columns = _get_columns_names(bc, table_name)
	view_columns = _select_columns(columns, values_partitions)

	# for partition in values_partitions:

	if num_files_per_parquet == 1:

		arr = ['one=1', 'two=D', 'three=T', 'one=2', 'two=E', 'three=U']

		for comb in (combinations(arr, 3)):
			print (comb)

		for i in list(comb):
			print(i)

		query = 'select {} from {} where {} = {}'.format(view_columns, table_name, partition, partition_value)
		_save_parquet(bc, query, output, partition, partition_value, table_name)

	elif num_files_per_parquet >= 1:
		print('Unsupported')

		# TODO: Enable up to OFFSET clause support.
		# query = 'select count(*) from {} where {} = {}'.format(table_name, partition, partition_value)
		# result = bc.sql(query)
		# total_registers = result.values.tolist()[0][0]
		# registers_per_parquet = math.ceil(total_registers / num_files_per_parquet)
		#
		# index = 0
		# for i in range(0, total_registers, registers_per_parquet):
		# 	query = 'select {} from {} where {} = {} limit {} offset {}'.format(view_columns, table_name, partition, partition_value, registers_per_parquet, i)
		#
		# 	_save_parquet(bc, query, output, partition, partition_value, table_name + '_' + str(index))
		# 	index += 1

	else:
		print('num_files_per_parquet must be greater than 1.')


def create_hive_partition_data(input, table_name, partitions, output, num_files_per_parquet):
	if not os.path.exists(output):
		os.makedirs(output)

	bc = BlazingContext()
	bc.create_table(table_name, input)

	# Temporal
	total = bc.sql('select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from orders order by o_orderkey')
	total.to_csv(output + "/origin.csv", index=False)

	columns = bc.describe_table(table_name)
	data_partition_array_dict = []
	for partition in partitions:
		if partition in columns:
			values = bc.sql(f'select distinct({partition}) from {table_name}')
			data_partition_array_dict.append(values.to_pandas().to_dict())
		else:
			print('Column "' + partition + '" not exist')

	_save_partition_files(bc, table_name, data_partition_array_dict, output, num_files_per_parquet)

def testing_load_hive_table(table_name, location, partitions, partitions_schema):
	bc = BlazingContext()

	bc.create_table(table_name, location,
					file_format='parquet',
					hive_table_name=table_name,
					partitions=partitions,
					partitions_schema=partitions_schema)

	# bc.create_table(table_name, location, file_format='parquet')

	# Temporal
	total = bc.sql('select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from orders order by o_orderkey')
	total.to_csv(location + "/dest.csv", index=False)


def test_hive_partition_data(input, table_name, partitions, partitions_schema, output, num_files_per_parquet=1):
	create_hive_partition_data(input, table_name, partitions, output, num_files_per_parquet)
	testing_load_hive_table(table_name, output, partitions, partitions_schema)


def main():
	dir_data = '/home/diegodfrf/tpch'
	ext = "parquet"

	test_hive_partition_data(input = ("%s/%s_[0-9]*.%s") % (dir_data, "orders", ext),
							 table_name = 'orders',
							 partitions={'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'],
										 'o_orderstatus': ['F', 'O', 'P'],
										 'o_shippriority': [0]},
							 partitions_schema=[('o_orderpriority', 'str'),
												('o_orderstatus', 'str'),
												('o_shippriority', 'int')],
							 output='/home/diegodfrf/BlazingSQL/partitions',
							 num_files_per_parquet = 1)

if __name__ == "__main__":
	main()