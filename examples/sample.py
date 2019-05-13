import pyarrow as pa
import pyblazing
from pyblazing import SchemaFrom
from pyblazing import DriverType, FileSystemType, EncryptionType
from collections import OrderedDict
from libgdf_cffi import ffi, libgdf

def get_dtype_values(dtypes):
    values = []
    def gdf_type(type_name):
        dicc = {
            'str': libgdf.GDF_STRING,
            'date': libgdf.GDF_DATE64,
            'date64': libgdf.GDF_DATE64,
            'date32': libgdf.GDF_DATE32,
            'timestamp': libgdf.GDF_TIMESTAMP,
            'category': libgdf.GDF_CATEGORY,
            'float': libgdf.GDF_FLOAT32,
            'double': libgdf.GDF_FLOAT64,
            'float32': libgdf.GDF_FLOAT32,
            'float64': libgdf.GDF_FLOAT64,
            'short': libgdf.GDF_INT16,
            'long': libgdf.GDF_INT64,
            'int': libgdf.GDF_INT32,
            'int32': libgdf.GDF_INT32,
            'int64': libgdf.GDF_INT64,
        }
        if dicc.get(type_name):
            return dicc[type_name]
        return libgdf.GDF_INT64

    for key in dtypes:
        values.append( gdf_type(key))

#     print('>>>> dtyps for', dtypes.values())
#     print(values)
    return values

print('*** Register a POSIX File System ***')
fs_status = pyblazing.register_file_system(
    authority="tpch",
    type=FileSystemType.POSIX,
    root="/"
)
print(fs_status)


column_names = ["c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment"]
column_types = ["int32","int64","int64","int32","int64","float64","int64","int64"]
customer_schema = pyblazing.register_table_schema(table_name='customer_csv', type=SchemaFrom.CsvFile, path='/home/aocsa/blazingsql/tpch/data/customer_0.psv', delimiter='|', dtypes=get_dtype_values(column_types), names=column_names)

column_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comments']
column_types = ['int32', 'int64', 'int32', 'int64']
nation_schema = pyblazing.register_table_schema(table_name='nation_csv', type=SchemaFrom.CsvFile, path='/home/aocsa/blazingsql/tpch/data/nation_0.psv', delimiter='|', dtypes=get_dtype_values(column_types), names=column_names)

column_names = ['r_regionkey', 'r_name', 'r_comment']
column_types = ['int32', 'int64', 'int64']
region_schema = pyblazing.register_table_schema(table_name='region_csv', type=SchemaFrom.CsvFile, path='/home/aocsa/blazingsql/tpch/data/region_0.psv', delimiter='|', dtypes=get_dtype_values(column_types), names=column_names)

sql_data = {
    customer_schema: ['/home/aocsa/blazingsql/tpch/data/customer_0.psv',
                     '/home/aocsa/blazingsql/tpch/data/customer_1.psv'],
    nation_schema: ['/home/aocsa/blazingsql/tpch/data/nation_0.psv',
                    '/home/aocsa/blazingsql/tpch/data/nation_1.psv'],
    region_schema: ['/home/aocsa/blazingsql/tpch/data/region_0.psv',
                    '/home/aocsa/blazingsql/tpch/data/region_1.psv']
}

sql = "select c_custkey, c_nationkey from main.customer_csv order by c_nationkey"
sql = "select c_custkey, n_nationkey, n_regionkey from main.customer_csv as c inner join main.nation_csv as n on c.c_nationkey = n.n_nationkey"
# sql = "select c.n_nationkey, c.n_regionkey from main.nation_csv as c order by c.n_regionkey"

result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
# result_gdf = [result_gdf +  result_gdf]
print(sql)
print('result size: ', len(result_gdf))
print(result_gdf)

fs_status = pyblazing.deregister_file_system(authority="tpch")
print(fs_status)
