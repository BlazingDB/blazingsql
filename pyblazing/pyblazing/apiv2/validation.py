import sys

def kwargs_validation(kwargs, bc_api_str):
    """
    Validation of kwargs params when a bc API is called
    """
    # csv, parquet, orc, json params
    # TODO wait for issue #1080 (stripe, skip_rows, kms_key_amazon_resource)
    if bc_api_str == "create_table":
        full_kwargs = ['file_format', 'names', 'dtype', 'delimiter', 'nrows', 'header', 'skiprows',
                       'skipfooter', 'lineterminator', 'delim_whitespace', 'skipinitialspace',
                       'skip_blank_lines', 'use_cols_indexes', 'use_cols_names', 'true_values',
                       'false_values', 'na_values', 'keep_default_na', 'na_filter', 'decimal',
                       'quotechar', 'doublequote', 'byte_range_offset', 'byte_range_size',
                       'compression', 'lines', 'stripes', 'skiprows', 'num_rows', 'use_index']
        params_info = "https://docs.blazingdb.com/docs/create_table"

    elif bc_api_str == "s3":
        full_kwargs = ['name', 'bucket_name', 'access_key_id', 'secret_key', 'encryption_type',
                       'session_token', 'root', 'kms_key_amazon_resource_name', 'endpoint_override', 'region']
        params_info = "https://docs.blazingdb.com/docs/s3"

    elif bc_api_str == "hdfs":
        full_kwargs = ['name', 'host', 'port', 'user', 'kerb_ticket']
        params_info = "https://docs.blazingdb.com/docs/hdfs"

    elif bc_api_str == "gs":
        full_kwargs = ['name', 'project_id', 'bucket_name', 'use_default_adc_json_file', 'adc_json_file']
        params_info = "https://docs.blazingdb.com/docs/google-storage"
    
    for arg_i in kwargs.keys():
        if arg_i not in full_kwargs:
            print("The parameter \'" + arg_i + "\' does not exists. Please make sure you are using the correct parameter:")
            print("To get the correct parameters, check:  " + params_info)
            sys.exit()
