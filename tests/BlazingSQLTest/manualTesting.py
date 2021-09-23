from blazingsql import BlazingContext
from blazingsql import DataType, S3EncryptionType
import numpy as np

import pandas as pd
import os
import subprocess
import sys

from EndToEndTests import tpchQueries as tpch

drill = "drill"
spark = "spark"
conda_prefix = os.getenv("CONDA_PREFIX")

bucket_name = os.getenv("BLAZINGSQL_E2E_AWS_S3_BUCKET_NAME")
access_key_id = os.getenv("BLAZINGSQL_E2E_AWS_S3_ACCESS_KEY_ID")
secret_key = os.getenv("BLAZINGSQL_E2E_AWS_S3_SECRET_KEY")

gs_project_id=os.getenv("BLAZINGSQL_E2E_GS_PROJECT_ID")
gs_bucket_name=os.getenv("BLAZINGSQL_E2E_GS_BUCKET_NAME")

file_results_dir = conda_prefix + "/blazingsql-testing-files/results/"
data_dir = conda_prefix + "/blazingsql-testing-files/data/"

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def git_clone():
    import git
    git.Git("$CONDA_PREFIX").clone("https://github.com/rapidsai/blazingsql-testing-files.git")

def git_pull():
    import git
    os.chdir(conda_prefix + "/blazingsql-testing-files")

def unzip():

    import tarfile

    os.chdir(conda_prefix + "/blazingsql-testing-files/")
    tar = tarfile.open('data.tar.gz', "r:gz")
    tar.extractall()
    tar.close()

def run_query(bc, engine, query, queryId, queryType, worder, orderBy,  acceptable_difference, use_percentage, input_type, **kwargs):

    result_gdf = bc.sql(query)

    filename = str("TPCH").upper() + "-" + str(queryId) + ".parquet"

    result_file = file_results_dir + "/" + str(engine) + "/" + filename

    pdf2 = pd.read_parquet(result_file)

    stringResult = ""

    if result_gdf is not None:
        if result_gdf.columns is not None:
            import dask_cudf

            if type(result_gdf) is dask_cudf.core.DataFrame:
                result_gdf = result_gdf.compute()

            expected_dtypes = result_gdf.dtypes.to_list()

            pdf = upcast_to_float(result_gdf).fillna(get_null_constants(result_gdf)).to_pandas()

            if worder == 1 and pdf.size != 0:
                pdf.sort_values([orderBy] if orderBy else pdf.columns.to_list(), inplace = True)

            stringResult = print_query_results(pdf, pdf2,  acceptable_difference, use_percentage, engine)

    return stringResult


def print_query_results(pdf1, pdf2,  acceptable_difference, use_percentage, engine):

    columnNamesComparison = compare_column_names(pdf1, pdf2)
    if columnNamesComparison != True:
        error_message = "Column names are not the same"

    resultComparisson = compare_results(pdf1, pdf2,  acceptable_difference, use_percentage, engine)
    if resultComparisson != "Success":
        error_message = resultComparisson[6:]

    stringResult = resultComparisson
    if resultComparisson != "Success" or columnNamesComparison == False:
        stringResult = "Fail"

    return stringResult

def compare_column_names(pdf1, pdf2):
    if len(pdf1.columns) != len(pdf2.columns):
        if pdf1.values.size == 0 and pdf2.values.size == 0:
            return True
        print("Different set of columns")
        return False
    for blzCol, drillCol in zip(pdf1.columns.values.tolist(), pdf2.columns.values.tolist()):
        if blzCol != drillCol:
            if begins_with(drillCol, blzCol, "EXPR")==False and begins_with(drillCol, blzCol, "count(")==False:
                print("Different columns")
                return False
    return True

def begins_with(col1, col2, exp):
    return col1.startswith(exp) or col2.startswith(exp)

def compare_results(vdf1, vdf2,  acceptable_difference, use_percentage, engine):
    if vdf1.size == 0 and vdf2.size == 0:
        return 'Success'
    elif pre_compare_results(vdf1.values, vdf2.values):
        return 'Success'
    else:
        res = assert_equal(vdf1, vdf2, acceptable_difference, use_percentage, engine)
        return res

def upcast_to_float(df):
    for name in df.columns:
        if np.issubdtype(df[name].dtype, np.bool_):
            df[name] = df[name].astype(np.float32)
        elif np.issubdtype(df[name].dtype, np.integer):
            df[name] = df[name].astype(np.float64)
    return df

def get_null_constants(df):
    null_values = {}
    for col, dtype in df.dtypes.to_dict().items():
        if np.issubdtype(dtype, np.datetime64):
            null_values[col] = np.datetime64('nat')
        elif np.issubdtype(dtype, np.number):
            null_values[col] = np.nan
    return null_values

def pre_compare_results(vdf1, vdf2):
    try:
        np.testing.assert_equal(vdf1, vdf2)
        return True
    except (AssertionError, ValueError, TypeError) as e:
        return False

def assert_equal(pdf1, pdf2, acceptable_difference, use_percentage, engine):
    np.warnings.filterwarnings('ignore')
    if pdf1.shape[0] == pdf2.shape[0]:
        if pdf1.shape[1] == pdf2.shape[1]:

            pdf1.reset_index(drop=True, inplace=True)
            pdf2.reset_index(drop=True, inplace=True)

            exac_comp = pdf1.select_dtypes(exclude=np.inexact).equals(pdf2.select_dtypes(exclude=np.inexact))

            tmp_pdf1 = pdf1.select_dtypes(include=np.inexact)
            tmp_pdf2 = pdf2.select_dtypes(include=np.inexact)

            res = np.all(exac_comp) and np.allclose(tmp_pdf1.values, tmp_pdf2.values, acceptable_difference, equal_nan=True)
            if res:
                return  'Success'
            else:
                return  'Fail: Different values'
        else:
            return  'Fail: Different number of columns blzSQLresult: ' + str(pdf1.shape[1]) + ' ' + (engine) + ' result: ' + str(pdf2.shape[1])
    else:
        return  'Fail: Different number of rows blzSQLresult: ' + str(pdf1.shape[0]) + ' ' + (engine) + ' result: '+ str(pdf2.shape[0])

def create_tables(bc, dir_data_lc, fileSchemaType, **kwargs):

    ext = "parquet"

    tpchTables = ['customer','orders','supplier','lineitem','part','partsupp','nation','region']

    tables = kwargs.get('tables', tpchTables)

    dir_data_lc = dir_data_lc + "tpch/"

    for i, table in enumerate(tables):
        table_files = ("%s/%s_[0-9]*.%s") % (dir_data_lc, table, ext)
        t = None
        t = bc.create_table(table, table_files)

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def main():

    install("gitpython")

    if not os.path.exists(conda_prefix + "/blazingsql-testing-files/"):
        git_clone()
    else:
        git_pull()

    unzip()

    queryType = ' Local Tests '

    if len(sys.argv) == 2:
        n_nodos = sys.argv[1]
    else:
        n_nodos = "1"

    if n_nodos == "1":
        print("Executing test in Single Node")
        bc = BlazingContext()
    else:
        print("Executing test in Distributed Mode")
        from dask.distributed import Client
        client = Client('127.0.0.1:8786')
        print("Dask client ready!")
        bc = BlazingContext(dask_client = client, network_interface='lo')

    log_dict = {}

    def executionLocalTest(queryType):

        #Read Data TPCH------------------------------------------------------------------------------------------------------------

        tables = ['nation', 'region', 'supplier','customer','lineitem','orders', 'part', 'partsupp']

        data_types =  [DataType.PARQUET] # TODO json

        for fileSchemaType in data_types:
            create_tables(bc, data_dir, fileSchemaType, tables=tables)

        #   Run Query -----------------------------------------------------------------------------
            worder = 1 # Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.01

            print('==============================')
            print(queryType)
            print('==============================')

            queryId = 'TEST_01'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_02'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_03'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, spark, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_04'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_05'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_06'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, spark, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_07'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

    executionLocalTest(queryType)

    queryType = ' S3 Tests '

    def executionS3Test(queryType):

        #Read Data TPCH------------------------------------------------------------------------------------------------------------

        authority = "tpch_s3"

        print(authority)
        print(bucket_name)
        hola = S3EncryptionType.NONE
        print(hola)
        print(access_key_id)
        print(secret_key)

        bc.s3(authority, bucket_name=bucket_name, encryption_type=S3EncryptionType.NONE,
              access_key_id=access_key_id, secret_key=secret_key)

        dir_data_lc = "s3://" + authority + "/" + "DataSet100Mb2part/"

        tables = ['nation', 'region', 'supplier','customer','lineitem','orders', 'part', 'partsupp']
        data_types =  [DataType.PARQUET] # TODO json

        for fileSchemaType in data_types:
            create_tables(bc, data_dir, fileSchemaType, tables=tables)

        #   Run Query -----------------------------------------------------------------------------
            worder = 1 # Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.01

            print('==============================')
            print(queryType)
            print('==============================')

            queryId = 'TEST_08'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_09'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_10'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_11'
            #print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            #result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_12'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_13'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_14'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

    executionS3Test(queryType)

    queryType = ' GS Tests '

    def executionGSTest(queryType):

        authority = "tpch_gs"

        bc.gs(authority,
        project_id=gs_project_id,
        bucket_name=gs_bucket_name,
        use_default_adc_json_file=True,
        adc_json_file='')

        dir_data_lc = 'gcs://'+ authority +'/100MB2Part/'

        tables = ['nation', 'region', 'supplier','customer','lineitem','orders', 'part', 'partsupp']
        data_types =  [DataType.PARQUET]

        for fileSchemaType in data_types:
            create_tables(bc, data_dir, fileSchemaType, tables=tables)

        #   Run Query -----------------------------------------------------------------------------
            worder = 1 # Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.01

            print('==============================')
            print(queryType)
            print('==============================')

            queryId = 'TEST_15'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, spark, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_16'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            queryId = 'TEST_17'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, spark, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_18'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_19'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_20'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, spark, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_21'
            print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            result = run_query(bc, spark, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

            queryId = 'TEST_22'
            #print("Executing " + queryId + " ... ")
            query = tpch.get_tpch_query(queryId)
            #result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

    executionGSTest(queryType)

    green = bcolors.OKGREEN
    endc = bcolors.ENDC

    print(green + "=======================================")
    print("SUMMARY TESTS")
    print("=======================================" + endc)

    for key, value in log_dict.items():
	    print('"{}" : {} '.format(key, value))

    print (green + "=======================================" + endc)

main()
