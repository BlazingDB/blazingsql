import unittest

from pyblazing.apiv2 import context

from cudf import DataFrame
import pandas as pd
import pyarrow as pa


class TestCreateTableFromGDF(unittest.TestCase):

    def setUp(self):
        # TODO percy check this we needto use mocks
        self.context = context.make_context()
        self.context.localfs("tpch")

    def test_simple_cudf(self):
        cudf_df = DataFrame()
        cudf_df['key'] = [1, 2, 3, 4, 5]
        cudf_df['val'] = [float(i + 10) for i in range(5)]

        # ds = datasource.from_cudf(cudf_df)
        # table = self.context.create_table('holas', ds)

        table = self.context.create_table('holas', cudf_df)

        print("PRINTTTTTT RAWWWWWWW")
        print(cudf_df)

        print("PRINTTTTTT TABLE DATASOURCE")
        print(table)

        print("RUN QUERYYYYYYYYY")

        result = self.context.run_query("select * from main.holas", ['holas'])
        now = result.get()

        print(now)

    def test_simple_pandas(self):
        column_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comments']
        column_types = {'n_nationkey': 'int32', 'n_regionkey': 'int64'}
        nation_df = pd.read_csv("/home/percy/Blazing/projects/demos/data/nation.psv", delimiter = '|', dtype = column_types, names = column_names)
        pandas_df = nation_df[['n_nationkey', 'n_regionkey']]

        table = self.context.create_table('holas2', pandas_df)

        print("PRINTTTTTT RAWWWWWWW")
        print(pandas_df)

        print("PRINTTTTTT TABLE DATASOURCE")
        print(table)

        print("RUN QUERYYYYYYYYY")

        result = self.context.run_query("select * from main.holas2", ['holas2'])
        now = result.get()

        print(now)

    def test_simple_arrow(self):
        arrow_table = pa.RecordBatchStreamReader('/home/percy/Blazing/projects/demos/data/gpu.arrow').read_all()
        table = self.context.create_table('holas3', arrow_table)

        print("PRINTTTTTT RAWWWWWWW")
        print(arrow_table)

        print("PRINTTTTTT TABLE DATASOURCE")
        print(table)

        print("RUN QUERYYYYYYYYY")

        result = self.context.run_query("select * from main.holas3", ['holas3'])
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

        result = self.context.run_query("select * from main.holas_csv", [table_name])
        now = result.get()

        print(now)


if __name__ == '__main__':
    unittest.main()

