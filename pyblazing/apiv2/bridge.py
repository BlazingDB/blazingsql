import pyblazing


# TODO percy remove this one once we have the full port ready
# this object should not have state and ultimately should stop existing
class internal_api:

    # Type wrappers

    SchemaFrom = pyblazing.SchemaFrom
    DriverType = pyblazing.DriverType.LIBHDFS3,
    S3EncryptionType = pyblazing.EncryptionType
    FileSystemType = pyblazing.FileSystemType

    ResultSetHandle = pyblazing.ResultSetHandle

    # Function/Method wrappers

    _get_client_internal = pyblazing._get_client_internal

    # Decorators

    @staticmethod
    def register_file_system(client, authority, type, root, params = None):
        return pyblazing.register_file_system(authority, type, root, params)

    @staticmethod
    def create_table(client, table_name, **kwargs):
        return pyblazing.create_table(table_name, **kwargs)

    @staticmethod
    def run_query_get_token(client, sql, tables):
        return pyblazing.run_query_get_token(sql, tables)

    @staticmethod
    def run_query_get_results(client, metaToken):
        return pyblazing.run_query_get_results(metaToken)
