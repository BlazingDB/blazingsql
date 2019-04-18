from collections import OrderedDict
from enum import Enum

from .bridge import internal_api


class FileSystem(object):

    def __init__(self):
        self.file_systems = OrderedDict()

    def __repr__(self):
        return "TODO"

    def __str__(self):
        # TODO percy use string builder here (generators?)
        for fs in self.file_systems:
            type = fs['type']
            prefix = fs['prefix']
            fs_str = '%s (%s)' % (prefix, type)
            print(fs_str)

    def localfs(self, client, prefix, **kwargs):
        self._verify_prefix(prefix)

        root = kwargs.get('root', '/')

        fs = OrderedDict()
        fs['type'] = 'local'

        # TODO percy manage exceptions here ?
        self._register_localfs(client, prefix, root, fs)

        return fs

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
        encryption_type = kwargs.get('encryption_type', internal_api.S3EncryptionType.NONE)
        kms_key_amazon_resource_name = kwargs.get('kms_key_amazon_resource_name', '')

        fs = OrderedDict()
        fs['type'] = 's3'
        fs['bucket_name'] = bucket_name
        fs['access_key_id'] = access_key_id
        fs['secret_key'] = secret_key
        fs['session_token'] = session_token
        fs['encryption_type'] = encryption_type
        fs['kms_key_amazon_resource_name'] = kms_key_amazon_resource_name

        # TODO percy manage exceptions here ?
        self._register_s3(client, prefix, root, fs)

        return fs

    def _verify_prefix(self, prefix):
        # TODO percy throw exception
        if prefix in self.file_systems:
            # TODO percy improve this one add the fs type so we can raise a nice exeption
            raise Exception('Fail add fs')

    def _register_localfs(self, client, prefix, root, fs):
        fs_status = internal_api.register_file_system(
            client,
            authority = prefix,
            type = internal_api.FileSystemType.POSIX,
            root = root
        )

        self._verify_filesystem(prefix, fs, fs_status)

    def _register_hdfs(self, client, prefix, root, fs):
        fs_status = internal_api.register_file_system(
            client,
            authority = prefix,
            type = internal_api.FileSystemType.HDFS,
            root = root,
            params = {
                'host': fs['host'],
                'port': fs['port'],
                'user': fs['user'],
                'driverType': internal_api.DriverType.LIBHDFS3,
                'kerberosTicket': fs['kerberos_ticket']
            }
        )

        self._verify_filesystem(prefix, fs, fs_status)

    def _register_s3(self, client, prefix, root, fs):
        fs_status = internal_api.register_file_system(
            client,
            authority = prefix,
            type = internal_api.FileSystemType.S3,
            root = root,
            params = {
                "bucketName": fs['bucket_name'],
                "accessKeyId": fs['access_key_id'],
                "secretKey": fs['secret_key'],
                "sessionToken": fs['session_token'],
                "encryptionType": fs['encryption_type'],
                "kmsKeyAmazonResourceName": fs['kms_key_amazon_resource_name']
            }
        )

        self._verify_filesystem(prefix, fs, fs_status)

    def _verify_filesystem(self, prefix, fs, fs_status):
        if fs_status != 1:
            # TODO percy better error  message
            raise Exception("coud not register the s3")

        self.file_systems[prefix] = fs

