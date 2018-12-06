import pyarrow as pa
import pyblazing
from pyblazing import DriverType, FileSystemType, EncryptionType


def register_hdfs():
    print('*** Register a HDFS File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch2",
        type=FileSystemType.HDFS,
        root="/tmp/tpch/1mb/",
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
        authority="tpch2",
        type=FileSystemType.S3,
        root="/tmp/tpch/1mb/",
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

import numpy as np

def main():
    print('*** Register a POSIX File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch0",
        type=FileSystemType.POSIX,
        root="/tmp/tpch/1mb/"
    )
    print(fs_status)

    print('*** Create a table from an CSV file schema ***')
    column_names = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
    column_types = [np.int32, np.str, np.int32, np.str]

    orders_table_ref = pyblazing.create_table(table_name='orders',
                                          path_list=['/tpch/order.psv'],
                                          delimiter='|',
                                          dtype=column_types,
                                          names=column_names)
    
    # customer_table_ref = pyblazing.create_table('customer', ['/tmp/customer.parquet'])
    # print(orders_table_ref)
    # print(customer_table_ref)

    print('*** Define inputs for a SQL query and run query ***')
    tables = [orders_table_ref]
    sql = """select sum(orders.o_totalprice) from orders;"""
    result_gdf = pyblazing.run_query_filesystem(sql, tables)
    print(sql)
    print(result_gdf)

    fs_status = pyblazing.deregister_file_system(authority="tpch0")
    print(fs_status)


if __name__ == '__main__':
    main()
