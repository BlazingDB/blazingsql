import pyblazing


# TODO percy remove this one once we have the full port ready
# this object should not have state and ultimately should stop existing
class internal_api:

    # Type wrappers
    Status = pyblazing.Status

    FileSchemaType = pyblazing.FileSchemaType
    DriverType = pyblazing.DriverType
    S3EncryptionType = pyblazing.EncryptionType
    FileSystemType = pyblazing.FileSystemType

    ResultSetHandle = pyblazing.ResultSetHandle
    gdf_dtype = pyblazing.gdf_dtype
    get_dtype_values = pyblazing.get_dtype_values
    get_np_dtype_to_gdf_dtype = pyblazing.get_np_dtype_to_gdf_dtype

    _CsvArgs = pyblazing._CsvArgs
    _OrcArgs = pyblazing._OrcArgs

    # Function/Method wrappers

    _get_client = pyblazing._get_client
    SetupOrchestratorConnection = pyblazing.SetupOrchestratorConnection

    @staticmethod
    def register_file_system(client, authority, type, root, params = None):
        return pyblazing.register_file_system(authority, type, root, params)

    @staticmethod
    def create_table(client, table_name, **kwargs):
        return pyblazing.create_table(table_name, **kwargs)

    @staticmethod
    def run_query_get_token(client, sql):
        return pyblazing.run_query_get_token(sql)

    @staticmethod
    def run_query_get_results(client, metaToken, startTime):
        return pyblazing.run_query_get_results(metaToken, startTime)

    @staticmethod
    def run_query_get_concat_results(client, metaToken, startTime):
        return pyblazing.run_query_get_concat_results(metaToken, startTime)

    @staticmethod
    def convert_to_dask(metaToken, connection,workerPort):
        return pyblazing.convert_to_dask(metaToken, connection,workerPort)

    @staticmethod
    def scan_datasource(client, directory, wildcard):
        return pyblazing.scan_datasource(directory, wildcard)
