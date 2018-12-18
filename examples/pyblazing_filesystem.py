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

    # register schema
    orders_schema = pyblazing.register_table_schema(table_name='customer_parquet', type=SchemaFrom.ParquetFile, path='/tmp/DataSet50mb/customer_0_0.parquet')

    # query using multiples files
    sql_data = {
        orders_schema: ['/tmp/DataSet50mb/customer_0_0.parquet',
                        '/tmp/DataSet50mb/customer_0_1.parquet']
    }
    sql = 'select c_custkey, c_acctbal, c_acctbal + c_nationkey as addition from main.customer_parquet'
    result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
    print(sql)
    print(result_gdf)

    fs_status = pyblazing.deregister_file_system(authority="tpch")
    print(fs_status)

if __name__ == '__main__':
    main()
