import unittest

import pyblazing
from pyblazing.apiv2 import sql
from pyblazing.apiv2 import datasource

from cudf import DataFrame


class TestCreateTableFromGDF(unittest.TestCase):

    def setUp(self):
        self.sql = sql.SQL()

        # TODO percy check this we needto use mocks
        connection = '/tmp/orchestrator.socket'
        self.client = pyblazing._get_client_internal(connection, 8890)

    def test_simple(self):
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


if __name__ == '__main__':
    unittest.main()

