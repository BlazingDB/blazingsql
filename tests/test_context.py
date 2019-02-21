import unittest

import pyblazing
from pyblazing.apiv2 import context
from pyblazing.apiv2 import datasource

from pyblazing import DriverType, FileSystemType, EncryptionType
from pyblazing import SchemaFrom

from cudf import DataFrame


class TestCreateTableFromGDF(unittest.TestCase):

    def setUp(self):
        # TODO percy check this we needto use mocks
        self.context = context.make_context()

        # TODO clean this stuff

        print('*** Register a POSIX File System ***')
        fs_status = pyblazing.register_file_system(
            self.context.client,
            authority = "tpch",
            type = FileSystemType.POSIX,
            root = "/"
        )

    def test_simple_cudf(self):
        cudf_df = DataFrame()
        cudf_df['key'] = [1, 2, 3, 4, 5]
        cudf_df['val'] = [float(i + 10) for i in range(5)]

        ds = datasource.from_cudf(cudf_df)

        table = self.context.create_table('holas', ds)

        print("PRINTTTTTT RAWWWWWWW")
        print(cudf_df)

        print("PRINTTTTTT TABLE DATASOURCE")
        print(table)

        print("RUN QUERYYYYYYYYY")

        result = self.context.run_query("select * from main.holas", ['holas'])
        now = result.get()

        print(now)

    def test_simple_parquet(self):
        # TODO this is ugly ... seems ds concept se mete en el camino
        table_name = 'holas_parquet'

        ds = datasource.from_parquet(self.context.client, table_name, '/home/percy/Blazing/Parquet/tpch-1gb/nation/0_0_0.parquet')

        table = self.context.create_table(table_name, ds)

        print("PRINTTTTTT TABLE PARQUETTT DATASOURCE")
        print(table)

        print("RUN PARQUETTTTT QUERYYYYYYYYY")

        result = self.context.run_query("select * from main.holas_parquet", [table_name])
        now = result.get()

        print(now)

    def test_simple_csv(self):
        # TODO this is ugly ... seems ds concept se mete en el camino

        table_name = 'holas_csv'
        path = '/home/percy/Blazing/TPCH/files_50mb/nation.tbl'
        column_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
        column_types = [3, 4, 3, 4]
        delimiter = '|'
        skip_rows = 0

        ds = datasource.from_csv(
            self.context.client,
            table_name,
            path,
            column_names,
            column_types,
            delimiter,
            skip_rows
        )

        table = self.context.create_table(table_name, ds)

        print("PRINTTTTTT TABLE CCCCCCCCCCCCCCCCSSSSSSSSSSVVVVVVVVVVVVVVVV DATASOURCE")
        print(table)

        print("RUN CCCCCCCSSSSVVVV QUERYYYYYYYYY")

        result = self.run_query("select * from main.holas_csv", [table_name])
        now = result.get()

        print(now)


if __name__ == '__main__':
    unittest.main()

