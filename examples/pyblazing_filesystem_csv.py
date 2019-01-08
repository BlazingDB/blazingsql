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
            "host": "192.168.0.1",
            "port": 8080,
            "user": "aocsa",
            "driverType": DriverType.LIBHDFS3,
            "kerberosTicket": ""
        }
    )
    print(fs_status)


def register_s3():
    print('*** Register a S3 File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch_s3",
        type=FileSystemType.S3,
        root="/",
        params={
            "fs_name": "company",
            "bucket_name": "company_bucket",
            "encryption_type": EncryptionType.NONE,
            "kms_key_amazon_resource_name": "",
            "access_key_id": "accessKeyIddsf3",
            "secret_key": "secretKey234",
            "session_token": ""
        }
    )
    print(fs_status)


def main():
    print('*** Register a POSIX File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch",
        type=FileSystemType.POSIX,
        root="/"
    )
    print(fs_status)

    names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
    dtypes = [3, 4, 3, 4]

    nation_schema = pyblazing.register_table_schema(table_name='nation_csv', type=SchemaFrom.CsvFile, path='/tmp/tpch/1mb/nation.psv', delimiter='|', dtypes=dtypes, names=names)
    sql_data = {
        nation_schema: ['/tmp/tpch/1mb/nation.psv']
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
