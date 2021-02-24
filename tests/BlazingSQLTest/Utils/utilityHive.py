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

def _save_parquet_from_pandas(pdf, output, partition, filename):
	partition_folder = '/'.join(partition)
	partition_folder = partition_folder.replace("'","")

	path = '{}/{}'.format(output, partition_folder)
	if not os.path.exists(path):
		os.makedirs(path)

	output_name = path + "/" + filename + '.parquet'
	pdf.to_parquet(output_name, compression="GZIP")

def _save_parquet_from_blazing(bc, query, output, partition, filename):
	result = bc.sql(query)
	_save_parquet_from_pandas(result.to_pandas(), output, partition, filename)

def _are_repeats_partitions(items):
	comp = set()
	for i in items:
		comp.add( i.split('=')[0] )

	if len(comp) == len(items):
		return False
	else:
		return True

def _concatenate_partitions_with_values(data_partition_array_dict):
	res = []
	for partition_dict in data_partition_array_dict:
		for item_array in partition_dict:
			temp = partition_dict[item_array]
			for i in temp:
				if type(temp[i]) == str:
					res.append(item_array + "='" + temp[i] + "'")
				else:
					res.append(item_array + "=" + str(temp[i]))

	return res

def _combine_partitions(data_partition_array_dict):
	res = _concatenate_partitions_with_values(data_partition_array_dict)

	comb = combinations(res, len(data_partition_array_dict))
	comb = list(comb)
	copy_comb = []

	for i in comb:
		if not _are_repeats_partitions(i):
			copy_comb.append(i)

	return copy_comb

def _save_partition_files(bc, table_name, data_partition_array_dict, output, num_files_per_parquet):
	values_partitions = _get_partition_values(data_partition_array_dict)
	columns = _get_columns_names(bc, table_name)
	view_columns = _select_columns(columns, values_partitions)

	combination_partition = _combine_partitions(data_partition_array_dict)

	if num_files_per_parquet == 1:

		for partition in combination_partition:
			where_clause = ' and '.join(partition)

			query = 'select {} from {} where {}'.format(view_columns, table_name, where_clause)
			_save_parquet_from_blazing(bc, query, output, partition, table_name)

	elif num_files_per_parquet >= 1:

		for partition in combination_partition:
			where_clause = ' and '.join(partition)

			query = 'select count(*) from {} where {}'.format(table_name, where_clause)
			result = bc.sql(query)
			total_registers = result.values.tolist()[0][0]
			registers_per_parquet = math.ceil(total_registers / num_files_per_parquet)

			index = 0
			df = bc.sql('select {} from {} where {}'.format(view_columns, table_name, where_clause))
			for i in range(0, total_registers, registers_per_parquet):
				pf = df.iloc[i:i+registers_per_parquet]

				_save_parquet_from_pandas(pf.to_pandas(), output, partition, table_name + '_' + str(index))
				index += 1

	else:
		print('num_files_per_parquet must be greater than 1.')


def create_hive_partition_data(input, table_name, partitions, output, num_files_per_parquet):
	if not os.path.exists(output):
		os.makedirs(output)

	bc = BlazingContext()
	bc.create_table(table_name, input)

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

def test_hive_partition_data(input, table_name, partitions, partitions_schema, output, num_files_per_parquet=1):
	create_hive_partition_data(input, table_name, partitions, output, num_files_per_parquet)
	testing_load_hive_table(table_name, output, partitions, partitions_schema)


def main():
	dir_data = '/input_path'
	ext = "parquet"

	test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "orders", ext),
							 table_name='orders',
							 partitions={
								 'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'],
								 'o_orderstatus': ['F', 'O', 'P'],
								 'o_shippriority': [0]},
							 partitions_schema=[('o_orderpriority', 'str'),
												('o_orderstatus', 'str'),
												('o_shippriority', 'int')],
							 output='/output_path',
							 num_files_per_parquet=4)

if __name__ == "__main__":
	main()