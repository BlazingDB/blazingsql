from collections import OrderedDict

# from .bridge import internal_api

import time
import dask.dataframe as dd


class Table:

    def __init__(self, datasource):
        self._datasource = datasource

    def __str__(self):
        if not self.is_valid():
            return 'Invalid BlazingSQL table'

        ident = "  "  # or can be a tab "\t"
        output = "BlazingSQL table type: %s\n" % self.type().value
        output += "%sTable name: %s\n" % (ident, self.name())

        if len(self.datasource_files()) > 0:
            output += "%sData source URI: %s\n" % (ident, self.datasource_uri())
            if len(self.datasource_files()) == 1:
                output += "%sData source file:\n" % ident
            else:
                output += "%sData source files:\n" % ident
            for file in self.datasource_files():
                output += "%s%s%s\n" % (ident, ident, file)

        return output

    def is_valid(self):
        return self._datasource.is_valid()

    def name(self):
        return self._datasource.table_name()

    def type(self):
        return self._datasource.descriptor().type()

    def datasource_uri(self):
        return self._datasource.descriptor().uri()

    def datasource_files(self):
        return self._datasource.descriptor().files()


# TODO we need to deal here with this metatokens stuff and many rals
# Maintains the resulset and the token after the run_query
# class ResultSet:

#     def __init__(self, client, metaToken, startTime, dask_client):
#         self.client = client
#         self.metaToken = metaToken
#         self.startTime = startTime
#         self.dask_client = dask_client

#     # this will call the get_result api
#     def get(self):
#         if(self.dask_client is None):
#             temp = internal_api.run_query_get_results(self.client, self.metaToken, self.startTime)
#         else:
#             dask_futures = []
#             for worker in list(self.dask_client.scheduler_info()["workers"]):
#                 dask_futures.append(self.dask_client.submit(internal_api.convert_to_dask, self.metaToken, self.client, workers = [worker]))
#             temp = dd.from_delayed(dask_futures)

#         return temp

#     # this assumes all ral are local. It will get all results and concatenamte them and only return the gdf.
#     # It will not return a result object, therefore it will need to make a copy
#     def get_all(self):
#         return internal_api.run_query_get_concat_results(self.client, self.metaToken, self.startTime)


# class SQL(object):

#     def __init__(self):
#         self.tables = OrderedDict()

#     def __del__(self):
#         all_table_names = list(self.tables.keys())
#         for table_name in all_table_names:
#             self.drop_table(table_name)

#     # TODO percy
#     def create_database(self, database_name):
#         pass

#     # ds is the DataSource object
#     def create_table(self, datasource):

#         self.tables[datasource.table_name()] = datasource

#         return Table(datasource)

#     # TODO percy this is to save materialized tables avoid reading from the data source
#     def create_view(self, view_name, sql):
#         pass

#     # TODO percy
#     def drop_database(self, database_name):
#         pass

#     # TODO percy drops should be here but this will be later (see Felipe proposal free)
#     def drop_table(self, table_name):
#         if table_name in self.tables:
#             del self.tables[table_name]

#     # TODO percy
#     def drop_view(self, view_name):
#         pass

#     def run_query(self, client, sql, dask_client):
#         startTime = time.time()
#         metaToken = internal_api.run_query_get_token(client, sql)
#         return ResultSet(client, metaToken, startTime, dask_client)
