from collections import OrderedDict
from enum import Enum

import pyblazing


class S3(Enum):
    NONE = 'NONE'
    AES_256 = 'AES_256'
    AWS_KMS = 'AWS_KMS'


class DataSource:

    def __init__(self):
        pass

    def is_valid(self):
        pass

    # cudf.DataFrame in-gpu-memory
    def is_cudf(self):
        pass

    # pandas.DataFrame in-memory
    def is_pandas(self):
        pass

    # arrow file on filesystem or arrow in-memory
    def is_arrow(self):
        pass

    # csv file on filesystem
    def is_csv(self):
        pass

    # parquet file on filesystem
    def is_parquet(self):
        pass

    # dir which contains any supported file on file system (csv, parquet or arrow)
    def is_dir(self):
        pass

# BEGIN DataSource builders


def from_cudf():
    pass


def from_pandas():
    pass


def from_arrow():
    pass


# path is file system transparent
def from_csv(path):
    pass


# path is file system transparent
def from_parquet(path):
    pass


# path is file system transparent
def from_dir(path, hint = 'auto can be parquet arrow csv'):
    pass

# END DataSource builders


# Maintains the resulset and the token after the run_query
class ResultSet:

    # this will call the get_result api
    def get(self):
        pass

    # TODO see Rodriugo proposal for interesting actions/operations here


class SQL(object):

    def __init__(self):
        pass

    # ds is the DataSource object
    def table(self, name, ds):
        pass

    # TODO percy this is to save materialized tables avoid reading from the data source
    def view(self, name, sql):
        pass

    # TODO percy drops should be here but this will be later (see Felipe proposal free)
    def drop(self, name):
        pass

    # TODO percy think about William proposal, launch, token split and distribution use case
    # return result obj ... by default is async
    def run_query(self, sql, async_opts = 'TODO'):
        rs = ResultSet()
        # TODO percy
        return rs


class FileSystem(object):

    def __init__(self):
        self.file_systems = OrderedDict()

    def __repr__(self):
        return "TODO"

    def __str__(self):
        pass

    def hdfs(self, client, prefix, **kwargs):
        self._verify_prefix(prefix)

        root = kwargs.get('root', '/')

        host = kwargs.get('host', '127.0.0.1')
        port = kwargs.get('port', 8020)
        user = kwargs.get('user', '')
        kerberos_ticket = kwargs.get('kerberos_ticket', '')

        fs = OrderedDict()
        fs['type'] = 'hdfs'
        fs['host'] = host
        fs['port'] = port
        fs['user'] = user
        fs['kerberos_ticket'] = kerberos_ticket

        # TODO percy manage exceptions here ?
        self._register_hdfs(client, prefix, root, fs)

        return fs

    def s3(self, client, prefix, **kwargs):
        self._verify_prefix(prefix)

        root = kwargs.get('root', '/')

        bucket_name = kwargs.get('bucket_name', '')
        access_key_id = kwargs.get('access_key_id', '')
        secret_key = kwargs.get('secret_key', '')
        session_token = kwargs.get('session_token', '')
        encryption_type = kwargs.get('encryption_type', S3.NONE)
        kms_key_amazon_resource_name = kwargs.get('kms_key_amazon_resource_name', '')

        fs = OrderedDict()
        fs['type'] = 's3'
        fs['bucket_name'] = bucket_name
        fs['access_key_id'] = access_key_id
        fs['secret_key'] = secret_key
        fs['session_token'] = session_token
        fs['encryption_type'] = encryption_type
        fs['kms_key_amazon_resource_name'] = kms_key_amazon_resource_name

        self.file_systems[prefix] = fs

        # TODO percy connect here with low level api

        return fs

    def show(self):
        for fs in self.file_systems:
            type = fs['type']
            prefix = fs['prefix']
            fs_str = '%s (%s)' % (prefix, type)
            print(fs_str)

    def _verify_prefix(self, prefix):
        # TODO percy throw exception
        if prefix in self.file_systems:
            # TODO percy improve this one add the fs type so we can raise a nice exeption
            raise Exception('Fail add fs')

    def _register_hdfs(self, client, prefix, root, fs):
        fs_status = pyblazing.register_file_system(
            client,
            authority = prefix,
            type = pyblazing.FileSystemType.HDFS,
            root = root,
            params = {
                'host': fs['host'],
                'port': fs['port'],
                'user': fs['user'],
                'driverType': pyblazing.DriverType.LIBHDFS3,
                'kerberosTicket': fs['kerberos_ticket']
            }
        )

        if fs_status != 1:
            # TODO percy better error  message
            raise Exception("coud not register the hdfs")

        self.file_systems[prefix] = fs


class BlazingContext(object):

    # connection (string) can be the unix socket path or the tcp host:port
    def __init__(self, connection):
        self.connection = connection
        self.client = pyblazing._get_client_internal(connection, 8890)
        self.fs = FileSystem()

    def __repr__(self):
        return "BlazingContext('%s')" % (self.connection)

    def __str__(self):
        return self.connection

    def hdfs(self, prefix, **kwargs):
        return self.fs.hdfs(self.client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        return self.fs.s3(self.client, prefix, kwargs)

    def show_filesystems(self):
        self.fs.show()


def make_context():
    # TODO percy we hardcode here becouse we know current ral has hardcoded this
    connection = '/tmp/orchestrator.socket'
    bc = BlazingContext(connection)
    return bc
