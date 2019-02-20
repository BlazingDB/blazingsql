from collections import OrderedDict

import pyblazing


# Maintains the resulset and the token after the run_query
class ResultSet:

    # this will call the get_result api
    def get(self):
        pass

    # TODO see Rodriugo proposal for interesting actions/operations here


class SQL(object):

    def __init__(self):
        pass

    # TODO percy
    def create_database(self, database_name):
        pass

    # ds is the DataSource object
    def create_table(self, table_name, datasource):
        pass

    # TODO percy this is to save materialized tables avoid reading from the data source
    def create_view(self, view_name, sql):
        pass

    # TODO percy
    def drop_database(self, database_name):
        pass

    # TODO percy drops should be here but this will be later (see Felipe proposal free)
    def drop_table(self, table_name):
        pass

    # TODO percy
    def drop_view(self, view_name):
        pass

    # TODO percy think about William proposal, launch, token split and distribution use case
    # return result obj ... by default is async
    def run_query(self, sql, async_opts = 'TODO'):
        rs = ResultSet()
        # TODO percy
        return rs
