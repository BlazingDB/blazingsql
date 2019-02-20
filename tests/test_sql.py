import unittest

from pyblazing.apiv2 import sql
from pyblazing.apiv2 import datasource

from cudf import DataFrame


class TestCreateTableFromGDF(unittest.TestCase):

    def setUp(self):
        self.sql = sql.SQL()

    def test_simple(self):
        cudf_df = DataFrame()
        cudf_df['key'] = [1, 2, 3, 4, 5]
        cudf_df['val'] = [float(i + 10) for i in range(5)]

        ds = datasource.from_cudf(cudf_df)

        table = self.sql.create_table('holas', ds)

        print("PRINTTTTTT RAWWWWWWW")
        print(cudf_df)

        print("PRINTTTTTT TABLE DATASOURCES")
        print(table)
        
        print("RUN QUERYYYYYYYYY")
        
        self.run_query()



if __name__ == '__main__':
    unittest.main()

