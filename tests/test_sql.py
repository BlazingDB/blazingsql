import unittest

import pyblazing
from pyblazing.apiv2 import sql
from pyblazing.apiv2 import datasource

from pyblazing import DriverType, FileSystemType, EncryptionType
from pyblazing import SchemaFrom

from cudf import DataFrame


class TestCreateTableFromGDF(unittest.TestCase):

    def setUp(self):
        self.sql = sql.SQL()

        # TODO percy check this we needto use mocks
        connection = '/tmp/orchestrator.socket'
        self.client = pyblazing._get_client_internal(connection, 8890)

        # TODO clean this stuff

        print('*** Register a POSIX File System ***')
        fs_status = pyblazing.register_file_system(
            self.client,
            authority = "tpch",
            type = FileSystemType.POSIX,
            root = "/"
        )

    def test_simple_cudf(self):
        cudf_df = DataFrame()
        cudf_df['key'] = [1, 2, 3, 4, 5]
        cudf_df['val'] = [float(i + 10) for i in range(5)]

        ds = datasource.from_cudf(cudf_df)

        table = self.sql.create_table('holas', ds)

        print("PRINTTTTTT RAWWWWWWW")
        print(cudf_df)

        print("PRINTTTTTT TABLE DATASOURCE")
        print(table)

        print("RUN QUERYYYYYYYYY")

        result = self.sql.run_query(self.client, "select * from main.holas", ['holas'])
        now = result.get()

        print(now)

    def test_simple_parquet(self):
        # TODO this is ugly ... seems ds concept se mete en el camino
        table_name = 'holas_parquet'

        ds = datasource.from_parquet(self.client, table_name, '/home/percy/Blazing/Parquet/tpch-1gb/nation/0_0_0.parquet')

        table = self.sql.create_table(table_name, ds)

        print("PRINTTTTTT TABLE PARQUETTT DATASOURCE")
        print(table)

        print("RUN PARQUETTTTT QUERYYYYYYYYY")

        result = self.sql.run_query(self.client, "select * from main.holas_parquet", [table_name])
        now = result.get()

        print(now)


if __name__ == '__main__':
    unittest.main()

