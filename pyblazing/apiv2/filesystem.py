from collections import OrderedDict
from enum import Enum

from .bridge import internal_api
import cio

def registerFileSystem(client,fs,root,prefix):
    if client is None:
        ok = cio.registerFileSystemCaller(fs,root,prefix)
        if ok == False:
            print("Could not register filesystem")
    else:
        dask_futures = []
        i = 0
        for worker in list(client.scheduler_info()["workers"]):
            dask_futures.append(client.submit(cio.registerFileSystemCaller, fs, root, prefix, workers = [worker]))
            i = i + 1
        for connection in dask_futures:
            ok = connection.result()
            if ok == False:
                print("Could not register filesystem with worker ")
                print(worker)
    return fs


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
        return registerFileSystem(client,fs,root,prefix)


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
        return registerFileSystem(client,fs,root,prefix)

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
        return registerFileSystem(client,fs,root,prefix)

    def gs(self, client, prefix, **kwargs):
        self._verify_prefix(prefix)

        root = kwargs.get('root', '/')

        project_id = kwargs.get('project_id', '')
        bucket_name = kwargs.get('bucket_name', '')
        use_default_adc_json_file = kwargs.get('use_default_adc_json_file', True)
        adc_json_file = kwargs.get('adc_json_file', '')

        fs = OrderedDict()
        fs['type'] = 'gs'
        fs['project_id'] = project_id
        fs['bucket_name'] = bucket_name
        fs['use_default_adc_json_file'] = use_default_adc_json_file
        fs['adc_json_file'] = adc_json_file
        return registerFileSystem(client,fs,root,prefix)

    def _verify_prefix(self, prefix):
        # TODO percy throw exception
        if prefix in self.file_systems:
            # TODO percy improve this one add the fs type so we can raise a nice exeption
            raise Exception('Fail add fs')
