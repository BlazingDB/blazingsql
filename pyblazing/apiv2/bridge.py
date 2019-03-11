import pyblazing


# TODO percy remove this one once we have the full port ready
# this object should not have state and ultimately should stop existing
class internal_api:

    # Type wrappers

    SchemaFrom = pyblazing.SchemaFrom
    DriverType = pyblazing.DriverType.LIBHDFS3,
    S3EncryptionType = pyblazing.EncryptionType
    FileSystemType = pyblazing.FileSystemType

    # Function/Method wrappers

    _get_client_internal = pyblazing._get_client_internal

    # Decorators

    @staticmethod
    def register_file_system(client, authority, type, root, params = None):
        return pyblazing.register_file_system(authority, type, root, params)

    @staticmethod
    def register_table_schema(client, table_name, **kwargs):
        return pyblazing.register_table_schema(table_name, **kwargs)
