from collections import OrderedDict

import pyblazing


# TODO we need to deal here with this metatokens stuff and many rals
# Maintains the resulset and the token after the run_query
class ResultSet:

    def __init__(self, client, metaToken):
        self.client = client
        self.metaToken = metaToken

    # this will call the get_result api
    def get(self):
        temp = pyblazing.run_query_get_results(self.client, self.metaToken)

        return temp

    # TODO see Rodriugo proposal for interesting actions/operations here


class SQL(object):

    def __init__(self):
        self.tables = OrderedDict()

    # TODO percy
    def create_database(self, database_name):
        pass

    # ds is the DataSource object
    def create_table(self, table_name, datasource):
        self._verify_table_name(table_name)

        # TODO verify cuda ipc ownership or reuse resources here

        self.tables[table_name] = datasource

        # TODO percy create table result
        output = OrderedDict()
        output['name'] = table_name
        output['datasource'] = str(datasource)

        return str(output)

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
    # table_names is an array of strings
    # return result obj ... by default is async
    def run_query(self, client, sql, table_names):
        tables = {}

        for table_name in table_names:
            tables[table_name] = self.tables[table_name].dataframe()

        metaToken = pyblazing.run_query_get_token(client, sql, tables)

        rs = ResultSet(client, metaToken)

        # TODO percy
        return rs

    def _verify_table_name(self, table_name):
        # TODO percy throw exception
        if table_name in self.tables:
            # TODO percy improve this one add the fs type so we can raise a nice exeption
            raise Exception('Fail add table_name already exists')

