from blazingsql import BlazingContext
import os
import math
from itertools import combinations

__all__ = ["create_hive_partition_data", "test_hive_partition_data"]

def get_dtypes(table_name, bool_column=False):
    switcher = {
        "customer": [
            "int32",
            "str",
            "str",
            "int32",
            "str",
            "float64",
            "str",
            "str"
        ],
        "region": ["int32", "str", "str"],
        "nation": ["int32", "str", "int32", "str"],
        "lineitem": [
            "int64",
            "int64",
            "int64",
            "int32",
            "float64",
            "float64",
            "float64",
            "float64",
            "str",
            "str",
            "date64",
            "date64",
            "date64",
            "str",
            "str",
            "str",
        ],
        "orders": [
            "int64",
            "int32",
            "str",
            "float64",
            "date64",
            "str",
            "str",
            "int32",
            "str",
        ],
        "supplier": ["int64", "str", "str", "int32", "str", "float64", "str"],
        "part": [
            "int64",
            "str",
            "str",
            "str",
            "str",
            "int64",
            "str",
            "float32",
            "str"
        ],
        "partsupp": ["int64", "int64", "int64", "float32", "str"],
    }

    if bool_column:
        switcher.update(
            {
                "bool_orders": [
                    "int64",
                    "int32",
                    "str",
                    "float64",
                    "date64",
                    "str",
                    "str",
                    "str",
                    "str",
                    "boolean",
                ]
            }
        )

    # Get the function from switcher dictionary
    func = switcher.get(table_name, "nothing")
    # Execute the function
    return func

def get_column_names(table_name, bool_column=False):
    switcher = {
        "customer": [
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment",
        ],
        "region": ["r_regionkey", "r_name", "r_comment"],
        "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
        "lineitem": [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment",
        ],
        "orders": [
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment",
        ],
        "supplier": [
            "s_suppkey",
            "s_name",
            "s_address",
            "s_nationkey",
            "s_phone",
            "s_acctbal",
            "s_comment",
        ],
        "part": [
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment",
        ],
        "partsupp": [
            "ps_partkey",
            "ps_suppkey",
            "ps_availqty",
            "ps_supplycost",
            "ps_comment",
        ],
    }

    if bool_column:
        switcher.update(
            {
                "bool_orders": [
                    "o_orderkey",
                    "o_custkey",
                    "o_orderstatus",
                    "o_totalprice",
                    "o_orderdate",
                    "o_orderpriority",
                    "o_clerk",
                    "o_shippriority",
                    "o_comment",
                    "o_confirmed",
                ]
            }
        )

    # Get the function from switcher dictionary
    func = switcher.get(table_name, "nothing")
    # Execute the function
    return func

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

def _save(df, output, partition, filename, file_format, table_name):
	partition_folder = '/'.join(partition)
	partition_folder = partition_folder.replace("'","")

	path = '{}/{}'.format(output, partition_folder)
	if not os.path.exists(path):
		os.makedirs(path)

	if file_format == 'parquet':
		output_name = path + "/" + filename + '.parquet'
		df.to_pandas().to_parquet(output_name, compression="GZIP")
	elif file_format == 'orc':
		output_name = path + "/" + filename + '.orc'
		df.to_orc(output_name)
	elif file_format == 'psv':
		output_name = path + "/" + filename + '.psv'
		col_names = get_column_names(table_name)
		df.to_pandas().to_csv(output_name, sep="|", header=False, index=False, encoding='utf-8')

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

def _save_partition_files(bc, table_name, data_partition_array_dict, output, file_format, num_files):
	values_partitions = _get_partition_values(data_partition_array_dict)
	columns = _get_columns_names(bc, table_name)
	view_columns = _select_columns(columns, values_partitions)

	combination_partition = _combine_partitions(data_partition_array_dict)

	if num_files == 1:

		for partition in combination_partition:
			where_clause = ' and '.join(partition)

			query = 'select {} from {} where {}'.format(view_columns, table_name, where_clause)
			_save(bc.sql(query), output, partition, table_name, file_format, table_name)

	elif num_files >= 1:

		for partition in combination_partition:
			where_clause = ' and '.join(partition)

			query = 'select count(*) from {} where {}'.format(table_name, where_clause)
			result = bc.sql(query)
			total_registers = result.values.tolist()[0][0]

			if total_registers == 0:
				continue

			registers_per_parquet = math.ceil(total_registers / num_files)

			index = 0
			df = bc.sql('select {} from {} where {}'.format(view_columns, table_name, where_clause))
			for i in range(0, total_registers, registers_per_parquet):
				pf = df.iloc[i:i+registers_per_parquet]

				_save(pf, output, partition, table_name + '_' + str(index), file_format, table_name)
				index += 1

	else:
		print('num_files must be greater than 1.')


def create_hive_partition_data(input, file_format, table_name, partitions, output, num_files):
	if not os.path.exists(output):
		os.makedirs(output)

	bc = BlazingContext()
	if file_format == 'psv':
		dtypes = get_dtypes(table_name)
		col_names = get_column_names(table_name)
		bc.create_table(table_name, input, file_format=file_format, delimiter="|", dtype=dtypes,
						names=col_names)
	else:
		bc.create_table(table_name, input)

	columns = bc.describe_table(table_name)
	data_partition_array_dict = []
	for partition in partitions:
		if partition in columns:
			valuesPartition = bc.sql(f'select distinct({partition}) from {table_name}').to_pandas().to_dict()
			finalValues = list(set(valuesPartition[partition].values()) & set(partitions[partition]))
			dictOfvalues = {i: finalValues[i] for i in range(0, len(finalValues))}
			valuesPartition[partition] = dictOfvalues
			data_partition_array_dict.append(valuesPartition)
		else:
			print('Column "' + partition + '" not exist')

	_save_partition_files(bc, table_name, data_partition_array_dict, output, file_format, num_files)

def testing_load_hive_table(table_name, file_format, location, partitions, partitions_schema):
	bc2 = BlazingContext()

	if file_format == 'psv':
		dtypes = get_dtypes(table_name)
		col_names = get_column_names(table_name)
		bc2.create_table(table_name, location,
						file_format='csv',
						hive_table_name=table_name,
						partitions=partitions,
						partitions_schema=partitions_schema,
						delimiter="|",
						dtype=dtypes,
						names=col_names)
	else:
		bc2.create_table(table_name, location,
						file_format=file_format,
						hive_table_name=table_name,
						partitions=partitions,
						partitions_schema=partitions_schema)

	# bc.create_table(table_name, location, file_format='parquet')

def test_hive_partition_data(input, file_format, table_name, partitions, partitions_schema, output, num_files=1):
	create_hive_partition_data(input, file_format, table_name, partitions, output, num_files)
	testing_load_hive_table(table_name, file_format, output, partitions, partitions_schema)


def main():
	condaPath = os.environ['CONDA_PREFIX']
	dir_data = condaPath + '/blazingsql-testing-files/data/tpch'
	ext = "parquet"

	test_hive_partition_data(input=("%s/%s_[0-9]*.%s") % (dir_data, "orders", ext),
							 file_format=ext,
							 table_name='orders',
							 partitions={
								 'o_orderpriority': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'],
								 'o_orderstatus': ['F', 'O', 'P'],
								 'o_shippriority': [0]},
							 partitions_schema=[('o_orderpriority', 'str'),
												('o_orderstatus', 'str'),
												('o_shippriority', 'int')],
							 output='/tmp/BlazingSQL/partitions/utilityHive',
							 num_files=4)

if __name__ == "__main__":
	main()