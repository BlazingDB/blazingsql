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
        result, error_msg = self._verify_prefix(prefix)
        
        if result == False:
            return (result, error_msg)

        root = kwargs.get('root', '/')

        fs = OrderedDict()
        fs['type'] = 'local'

        result, error_msg = self._register_localfs(client, prefix, root, fs)

        return result, error_msg, fs

    def hdfs(self, client, prefix, **kwargs):
        result, error_msg = self._verify_prefix(prefix)
        
        if result == False:
            return (result, error_msg)

        root = kwargs.get('root', '/')

        host = kwargs.get('host', '127.0.0.1')
        port = kwargs.get('port', 8020)
        user = kwargs.get('user', '')
        driver = kwargs.get('driver', 'libhdfs')
        kerberos_ticket = kwargs.get('kerberos_ticket', '')

        fs = OrderedDict()
        fs['type'] = 'hdfs'
        fs['host'] = host
        fs['port'] = port
        fs['user'] = user
        fs['driver'] = driver
        fs['kerberos_ticket'] = kerberos_ticket

        result, error_msg = self._register_hdfs(client, prefix, root, fs)

        if result == False:
            fs = None 

        return result, error_msg, fs

    def s3(self, client, prefix, **kwargs):
        result, error_msg = self._verify_prefix(prefix)
        
        if result == False:
            return (result, error_msg)

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

        result, error_msg = self._register_s3(client, prefix, root, fs)

        if result == False:
            fs = None

        return result, error_msg, fs

    def gcs(self, client, prefix, **kwargs):
        result, error_msg = self._verify_prefix(prefix)
        
        if result == False:
            return (result, error_msg)

        root = kwargs.get('root', '/')

        project_id = kwargs.get('project_id', '')
        bucket_name = kwargs.get('bucket_name', '')
        use_default_adc_json_file = kwargs.get('use_default_adc_json_file', True)
        adc_json_file = kwargs.get('adc_json_file', '')

        fs = OrderedDict()
        fs['type'] = 'gcs'
        fs['project_id'] = project_id
        fs['bucket_name'] = bucket_name
        fs['use_default_adc_json_file'] = use_default_adc_json_file
        fs['adc_json_file'] = adc_json_file

        result, error_msg = self._register_gcs(client, prefix, root, fs)

        if result == False:
            fs = None

        return result, error_msg, fs

    def _verify_prefix(self, prefix):
        result = True
        error_msg = ""
        if prefix in self.file_systems:
            result = False
            error_msg = "File system %s already exists!" % prefix
            return error_msg
        
        return result, error_msg

    def _register_localfs(self, client, prefix, root, fs):
        result, error_msg = internal_api.register_file_system(
            client,
            authority = prefix,
            type = internal_api.FileSystemType.POSIX,
            root = root
        )

        if result == True:
            self.file_systems[prefix] = fs

        return (result, error_msg) 

    def _register_hdfs(self, client, prefix, root, fs):
        if(fs['driver']=='libhdfs3'):
            driver = internal_api.DriverType.LIBHDFS3
        elif(fs['driver']=='libhdfs'):
            driver = internal_api.DriverType.LIBHDFS

        result, error_msg = internal_api.register_file_system(
            client,
            authority = prefix,
            type = internal_api.FileSystemType.HDFS,
            root = root,
            params = {
                'host': fs['host'],
                'port': fs['port'],
                'user': fs['user'],
                'driverType': driver,
                'kerberosTicket': fs['kerberos_ticket']
            }
        )

        if result == True:
            self.file_systems[prefix] = fs

        return (result, error_msg) 

    def _register_s3(self, client, prefix, root, fs):
        result, error_msg = internal_api.register_file_system(
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

        if result == True:
            self.file_systems[prefix] = fs

        return (result, error_msg) 

    def _register_gcs(self, client, prefix, root, fs):
        result, error_msg = internal_api.register_file_system(
            client,
            authority = prefix,
            type = internal_api.FileSystemType.GCS,
            root = root,
            params = {
                "projectId": fs['project_id'],
                "bucketName": fs['bucket_name'],
                "useDefaultAdcJsonFile": fs['use_default_adc_json_file'],
                "adcJsonFile": fs['adc_json_file']
            }
        )

        if result == True:
            self.file_systems[prefix] = fs

        return (result, error_msg) 
