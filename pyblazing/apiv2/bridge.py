import pyblazing


# TODO percy remove this one once we have the full port ready
# this object should not have state and ultimately should stop existing
class internal_api:

    # Type wrappers

    SchemaFrom = pyblazing.SchemaFrom
    DriverType = pyblazing.DriverType
    S3EncryptionType = pyblazing.EncryptionType
    FileSystemType = pyblazing.FileSystemType

    ResultSetHandle = pyblazing.ResultSetHandle

    # Function/Method wrappers

    _get_client = pyblazing._get_client

    # Decorators
    # Todo Rommel remove deprecated methods

    @staticmethod
    def register_file_system(client, authority, type, root, params = None):
        return pyblazing.register_file_system(authority, type, root, params)

    @staticmethod
    def new_create_table(client, table_name, **kwargs):
        return pyblazing.new_create_table(table_name, **kwargs)

    @staticmethod
    def register_table_schema(client, table_name, **kwargs):
        return pyblazing.register_table_schema(table_name, **kwargs)
        
    @staticmethod
    def create_table(client, table_name, **kwargs):
        return pyblazing.create_table(table_name, **kwargs)

    @staticmethod
    def run_query_get_token(client, sql, tables):
        return pyblazing.run_query_get_token(sql, tables)

    @staticmethod
    def run_query_filesystem_get_token(client, sql):
        return pyblazing.run_query_filesystem_get_token(sql)

    @staticmethod
    def run_query_get_results(client, metaToken):
        return pyblazing.run_query_get_results(metaToken)
