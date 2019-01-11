import numpy as np
import pandas as pd
import cudf as cudf
import pyblazing
from pyblazing import DriverType, FileSystemType, EncryptionType
from pyblazing import SchemaFrom


def register_hdfs():
    print('*** Register a HDFS File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch_hdfs",
        type=FileSystemType.HDFS,
        root="/",
        params={
            "host": "localhost",
            "port": 54310,
            "user": "aocsa",
            "driverType": DriverType.LIBHDFS3,
            "kerberosTicket": ""
        }
    )
    print(fs_status)

def main():
    register_hdfs()

    names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
    dtypes = [3, 4, 3, 4]

    nation_table = pyblazing.create_table(table_name='nation_csv', type=SchemaFrom.CsvFile, path='hdfs://tpch_hdfs/Data1Mb/nation_0_0.psv', delimiter='|', dtypes=dtypes, names=names)
    sql = 'select n_nationkey, n_regionkey + n_nationkey as addition from main.nation_csv'
    print(nation_table.name)
    print(nation_table.columns)
    print(nation_table.columns.dtypes)

    result_gdf = pyblazing.run_query(sql, {nation_table.name: nation_table.columns})
    print(sql)
    print(result_gdf.columns)

if __name__ == '__main__':
    main()
