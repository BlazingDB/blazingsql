import numpy as np
import pandas as pd
import cudf as cudf
import pyblazing
from pyblazing import DriverType, FileSystemType, EncryptionType
from pyblazing import SchemaFrom


def register_hdfs():
    print('*** Register a HDFS File System ***')
    fs_status = pyblazing.register_file_system(
        authority="localhost",
        type=FileSystemType.HDFS,
        root="/",
        params={
            "host": "localhost",
            "port": 54310,
            "user": "hadoop",
            "driverType": DriverType.LIBHDFS3,
            "kerberosTicket": ""
        }
    )
    print(fs_status)

 
def main():
    register_hdfs()

    names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
    dtypes = [3, 4, 3, 4]

    nation_schema = pyblazing.register_table_schema(table_name='nation_csv', type=SchemaFrom.CsvFile, path='hdfs://localhost/tpch/nation.psv', delimiter='|', dtypes=dtypes, names=names)
    sql_data = {
        nation_schema: ['hdfs://localhost/tpch/nation.psv']
    }
    for i in range(1):
        sql = 'select n_nationkey, n_regionkey + n_nationkey as addition from main.nation_csv'

        result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
        print(sql)
        print(result_gdf)


    fs_status = pyblazing.deregister_file_system(authority="tpch")
    print(fs_status)

if __name__ == '__main__':
    main()
