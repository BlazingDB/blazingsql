# Cast column to f64 before convert it to pandas
# This is a hack, use the assert_equal comparator when nulls is
# fully supported on cudf.sort_values
import json
import logging
import os
import re
import time

import blazingsql

# import git
import gspread
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pydrill.client import PyDrill
from pyspark.sql.session import SparkSession

from BlazingLogging import loggingHandler as lhandler
from Configuration import ExecutionMode
from Configuration import Settings as Settings
from DataBase import createSchema as cs


class Result:
    def __init__(self, columns, resultSet, resultBlz):
        self.columns = columns
        self.resultSet = resultSet
        self.resultBlz = resultBlz


name = "blzlogging"

HANDLER = lhandler.logging_handler()


class loggerblz:
    def __init__(self, query, error, totaltime):
        self.query = query
        self.error = error
        self.totaltime = totaltime


class result:
    def __init__(self, res_execution, error):
        self.res_execution = res_execution
        self.error = error


def logginghelper(name):
    # logging.basicConfig(filename='example.txt',level=logging.DEBUG)
    logging._defaultFormatter = logging.Formatter()
    logger = logging.getLogger(name)
    logger.handlers = []
    logger.setLevel(logging.DEBUG)
    logger.addHandler(HANDLER)
    return logger


def loggingClose(name):
    HANDLER.log = []


def upcast_to_float(df):
    for name in df.columns:
        if np.issubdtype(df[name].dtype, np.bool_):
            df[name] = df[name].astype(np.float32)
        elif np.issubdtype(df[name].dtype, np.integer):
            df[name] = df[name].astype(np.float64)
    return df


def to_pandas_f64_engine(df, expected_types_list):
    count = 0
    for col in df.columns:
        if count >= len(expected_types_list):
            break

        if expected_types_list[count] != np.dtype(object):
            if df.shape[0] > 0:
                if not np.issubdtype(df[col].dtype, np.number) and not np.issubdtype(
                    df[col].dtype, np.datetime64
                ):
                    if np.issubdtype(expected_types_list[count], np.bool_):
                        df[col] = (
                            df[col].map({"true": 1.0, "false": 0.0}).astype(np.float32)
                        )
                    elif np.issubdtype(expected_types_list[count], np.datetime64):
                        df[col] = df[col].astype(expected_types_list[count])
                    else:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
        count = count + 1

    return df


def get_null_constants(df):
    null_values = {}
    for col, dtype in df.dtypes.to_dict().items():
        if np.issubdtype(dtype, np.datetime64):
            null_values[col] = np.datetime64("nat")
        elif np.issubdtype(dtype, np.number):
            null_values[col] = np.nan
    return null_values


def pre_compare_results(vdf1, vdf2):
    try:
        np.testing.assert_equal(vdf1, vdf2)
        return True
    except (AssertionError, ValueError, TypeError) as e:
        print(e)
        return False


def assert_equal(pdf1, pdf2, acceptable_difference, use_percentage, engine):
    np.warnings.filterwarnings("ignore")
    if pdf1.shape[0] == pdf2.shape[0]:
        if pdf1.shape[1] == pdf2.shape[1]:

            # Removing indexes, because those are considered when
            # comparing with equals()
            pdf1.reset_index(drop=True, inplace=True)
            pdf2.reset_index(drop=True, inplace=True)

            exac_comp = pdf1.select_dtypes(exclude=np.inexact).equals(
                pdf2.select_dtypes(exclude=np.inexact)
            )

            tmp_pdf1 = pdf1.select_dtypes(include=np.inexact)
            tmp_pdf2 = pdf2.select_dtypes(include=np.inexact)

            # inexac_comp = tmp_pdf1.values == tmp_pdf2.values

            # if use_percentage:
            #     delta_comp =
            #           np.absolute(1 - (tmp_pdf1.values/tmp_pdf2.values)) <=
            #                                       acceptable_difference
            # else:
            #     delta_comp =
            #           np.absolute(tmp_pdf1.values - tmp_pdf2.values) <=
            #                                       acceptable_difference

            res = np.all(exac_comp) and np.allclose(
                tmp_pdf1.values, tmp_pdf2.values, acceptable_difference, equal_nan=True
            )
            if res:
                return "Success"
            else:
                return "Fail: Different values"
        else:
            return (
                "Fail: Different number of columns blzSQLresult: "
                + str(pdf1.shape[1])
                + " "
                + ("PyDrill" if isinstance(engine, PyDrill) else "PySpark")
                + " result: "
                + str(pdf2.shape[1])
            )
    else:
        return (
            "Fail: Different number of rows blzSQLresult: "
            + str(pdf1.shape[0])
            + " "
            + ("PyDrill" if isinstance(engine, PyDrill) else "PySpark")
            + " result: "
            + str(pdf2.shape[0])
        )


def begins_with(col1, col2, exp):
    return col1.startswith(exp) or col2.startswith(exp)


def compare_column_names(pdf1, pdf2):
    if len(pdf1.columns) != len(pdf2.columns):
        if pdf1.values.size == 0 and pdf2.values.size == 0:
            return True
        print("Different set of columns")
        return False
    for blzCol, drillCol in zip(
        pdf1.columns.values.tolist(), pdf2.columns.values.tolist()
    ):
        if blzCol != drillCol:
            if (
                begins_with(drillCol, blzCol, "EXPR") is False
                and begins_with(drillCol, blzCol, "count(") is False
            ):
                print("Different columns")
                return False
    return True


def compare_results(vdf1, vdf2, acceptable_difference, use_percentage, engine):
    if vdf1.size == 0 and vdf2.size == 0:
        return "Success"
    elif pre_compare_results(vdf1.values, vdf2.values):
        return "Success"
    else:
        res = assert_equal(vdf1, vdf2, acceptable_difference, use_percentage, engine)
        return res


# NOTE kharoly percy william: NEVER CHANGE THE ORDER of these
# lines (the logger logic depends that we log first queryType and then queryId
# WARNING DO NOT CHANGE THE CALL ORDER IN THIS FUCTION!


def get_Branch():
    branch = blazingsql.__branch_name__
    return branch


def get_CommitHash():
    commit = blazingsql.__version__
    return commit


def get_QueryId(input_type, test_name, test_id):
    query_id = (
        str(input_type).upper()
        + "-"
        + str(get_codTest(test_name)).upper()
        + "-"
        + str(test_id)
    )
    return query_id


def get_resultId(resultComparisson):
    result_id = 1
    if resultComparisson != "Success":
        result_id = 0
    return result_id


def get_codTest(test_name):
    switcher = {
        "Aggregations without group by": "AGGWOGRBY",
        "Coalesce": "COALESCE",
        "Column Basis": "COLBAS",
        "Bindable Alias": "BALIAS",
        "Boolean": "BOOL",
        "Case": "CASE",
        "Cast": "CAST",
        "Common Table Expressions": "COMTABLEX",
        "Concat": "CONCAT",
        "Count Distinct": "COUNTD",
        "Count without group by": "COUNTWOGRBY",
        "Cross join": "CROSSJOIN",
        "Date": "DATE",
        "Dir": "DIR",
        "File System Google Storage": "FSGS",
        "Hdfs FileSystem": "FSHDFS",
        "Hive FileSystem": "FSHIVE",
        "File System Local": "FSLOCAL",
        "File System S3": "FSS3",
        "Full outer join": "FOUTJOIN",
        "Group by": "GROUPBY",
        "Group by without aggregations": "GRBYWOAGG",
        "Inner join": "INNERJOIN",
        "Left outer join": "LOUTJOIN",
        "Like": "LIKE",
        "Literal": "LITERAL",
        "Nested Queries": "NESTEDQ",
        "Non-EquiJoin Queries": "NEQUIJOIN",
        "Order by": "ORDERBY",
        "Predicates With Nulls": "PREDWNULLS",
        "Round": "ROUND",
        "Simple Distribution From Local": "SIMPLEDIST",
        "Substring": "SUBSTRING",
        "Tables from Pandas": "TBLPANDAS",
        "Timestampdiff": "TIMESTAMPD",
        "Timestamp": "TIMESTAMP",
        "TPCH Queries": "TPCH",
        "Unary ops": "UNARYOPS",
        "Unify Tables": "UNIFYTBL",
        "Union": "UNION",
        "Limit": "LIMIT",
        "Where clause": "WHERE",
        "Wild Card": "WILDCARD",
        "Simple String": "SSTRING",
    }

    return switcher.get(test_name)


def print_fixed_log(
    logger,
    test_name,
    input_type,
    test_id,
    sql,
    resultComparisson,
    error_message,
    load_time,
    engine_time,
    total_time,
):
    commitHash = get_CommitHash()
    branchName = get_Branch()
    # dateNow=datetime.now()
    inputType = cs.get_extension(input_type)

    logger.info(get_QueryId(inputType, test_name, test_id))  # QueryID
    logger.info(Settings.dateNow)  # TimeStamp
    logger.info(test_name)  # TestGroup
    logger.info(inputType)  # InputType
    logger.info(sql)  # Query
    logger.info(get_resultId(resultComparisson))  # Result
    logger.info(error_message)  # Error
    logger.info(branchName)  # PR
    logger.info(commitHash)  # CommitHash
    logger.info(Settings.data["RunSettings"]["nRals"])
    logger.info(Settings.data["RunSettings"]["nGPUs"])
    logger.info(Settings.data["TestSettings"]["dataDirectory"])
    logger.info(test_id)
    logger.info(load_time)
    logger.info(engine_time)
    logger.info(total_time)


def print_query_results(
    sql,
    queryId,
    queryType,
    pdf1,
    pdf2,
    resultgdf,
    acceptable_difference,
    use_percentage,
    print_result,
    engine,
    input_type,
    load_time,
    engine_time,
    total_time,
):
    if print_result:
        print("#BLZ:")
        print(pdf1)
        if isinstance(engine, PyDrill):
            print("#DRILL:")
        else:
            print("#PYSPARK:")
        print(pdf2)
    data_type = cs.get_extension(input_type)
    print(str(queryId) + " Test " + queryType + " - " + data_type)
    print("#QUERY:")
    print(sql)
    print("RESULT:")

    error_message = ""
    stringResult = ""

    compareResults = True
    if "compare_results" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_results"]

    if compareResults:
        columnNamesComparison = compare_column_names(pdf1, pdf2)
        if columnNamesComparison is not True:
            print("Columns:")
            print(pdf1.columns)
            print(pdf2.columns)

            error_message = "Column names are not the same"
            print("ERROR:")
            print(error_message)

        resultComparisson = compare_results(
            pdf1, pdf2, acceptable_difference, use_percentage, engine
        )
        if resultComparisson != "Success":
            error_message = resultComparisson[6:]
            print("ERROR:")
            print(error_message)

        stringResult = resultComparisson
        if resultComparisson != "Success" or columnNamesComparison is False:
            stringResult = "Fail"
    else:
        stringResult = "Success"
    print(stringResult)

    print("TOTAL TIME: ")
    print(total_time)
    print("CRASHED NODES: ")
    # print(resultgdf.n_crashed_nodes)
    print("TOTAL NODES: ")
    # print(resultgdf.total_nodes)
    print("===================================================")

    logger = logginghelper(name)

    # TODO percy kharoly bindings we need to get the number from internal api
    # print_fixed_log(logger, queryType, queryId, sql, stringResult,
    #                                          error_message, 1, 1, 2)
    print_fixed_log(
        logger,
        queryType,
        input_type,
        queryId,
        sql,
        stringResult,
        error_message,
        load_time,
        engine_time,
        total_time,
    )


def print_query_results2(sql, queryId, queryType, error_message):
    print(queryId)
    print("#QUERY:")
    print(sql)
    print("RESULT:")
    print("Crash")
    print("ERROR:")
    print(error_message)
    print("CALCITE TIME: ")
    print("-")
    print("RAL TIME: ")
    print("-")
    print("EXECUTION TIME: ")
    print("-")

    print("===================================================")

    logger = logginghelper(name)
    print_fixed_log(
        logger, queryType, queryId, sql, "Crash", error_message, None, None, None
    )


def print_query_results_performance(sql, queryId, queryType, resultgdf):
    print(queryId)
    print("#QUERY:")
    print(sql)
    print("RESULT:")
    resultComparisson = "Success"
    print("CALCITE TIME: ")
    print(resultgdf.calciteTime)
    print("RAL TIME: ")
    print(resultgdf.ralTime)
    print("EXECUTION TIME: ")
    print(resultgdf.totalTime)

    print("===================================================")

    logger = logginghelper(name)

    print_fixed_log(
        logger,
        queryType,
        queryId,
        sql,
        resultComparisson,
        " ",
        resultgdf.calciteTime,
        resultgdf.ralTime,
        resultgdf.totalTime,
    )


def print_query_results_dist(
    sql,
    queryId,
    queryType,
    pdf1,
    pdf2,
    resultgdf,
    acceptable_difference,
    use_percentage,
    print_result,
):
    if print_result:
        print("#BLZ:")
        print(pdf1)
        print("#DRILL:")
        print(pdf2)
    print(queryId)
    print("#QUERY:")
    print(sql)
    print("RESULT:")
    resultComparisson = compare_results(
        pdf1.values, pdf2.values, acceptable_difference, use_percentage
    )
    error_message = ""
    if resultComparisson != "Success":
        error_message = resultComparisson[6:]
        resultComparisson = "Fail"
        print(resultComparisson)
        print("ERROR:")
        print(error_message)
    else:
        print(resultComparisson)
    print("CALCITE TIME: ")
    print(resultgdf.calciteTime)
    print("RAL TIME: ")
    print(resultgdf.ralTime)
    print("EXECUTION TIME: ")
    print(resultgdf.totalTime)

    print("===================================================")

    logger = logginghelper(name)

    print_fixed_log(
        logger,
        queryType,
        queryId,
        sql,
        resultComparisson,
        error_message,
        None,
        None,
        None,
    )


class Test:
    def __init__(self, test_name):
        self.test_name = test_name
        self.total = 0
        self.success = 0
        self.fail_ids = []


def save_log(gpu_ci_mode):

    c = 1
    cadena = []
    subcadena = []
    countPass = 0
    countCrash = 0

    for x in HANDLER.log:
        if c < 17:
            subcadena.append(x.msg)
            c = c + 1
        else:
            c = 1
            cadena.append(subcadena)
            subcadena = []
            subcadena.append(x.msg)
            c = c + 1
    print()
    cadena.append(subcadena)

    # If it didn't run any test (probably some were skipped)
    # then return success
    if cadena == [[]]:
        return True, []

    df = pd.DataFrame(
        cadena,
        columns=[
            "QueryID",
            "TimeStamp",
            "TestGroup",
            "InputType",
            "Query",
            "Result",
            "Error",
            "Branch",
            "CommitHash",
            "nRals",
            "nGPUs",
            "DataDirectory",
            "TestId",
            "LoadingTime",
            "EngineTotalTime",
            "TotalTime",
        ],
    )

    total = df.shape[0]

    countPass = df[df.Result == 1].count()["Result"]

    df1 = df[
        [
            "QueryID",
            "TimeStamp",
            "TestGroup",
            "InputType",
            "Query",
            "Result",
            "Error",
            "Branch",
            "CommitHash",
            "nRals",
            "nGPUs",
            "DataDirectory",
            "LoadingTime",
            "EngineTotalTime",
            "TotalTime",
        ]
    ].copy()

    create_summary_detail(df, gpu_ci_mode)

    printSummary(countPass, countCrash, total, gpu_ci_mode)

    if not gpu_ci_mode:
        saveLogInFile(df1)

        saveLog = False
        if "saveLog" in Settings.data["RunSettings"]:
            saveLog = Settings.data["RunSettings"]["saveLog"]

        print("saveLog = " + str(saveLog))

        # TODO william kharoly felipe we should try to enable and use
        # this function in the future
        # result, error_msgs = verify_prev_google_sheet_results(df1)
        result, error_msgs = True, []

        if result is True and saveLog == "true":
            saving_google_sheet_results(df1)
    else:
        result, error_msgs = True, []

    loggingClose(name)
    return result, error_msgs


def create_summary_detail(df, no_color):
    pdf = df
    pdf["Result"] = df["Result"].replace(1, "Success")
    pdf["Result"] = df["Result"].replace(0, "Fail")

    # making boolean series for a team name
    filter_fail = pdf["Result"] == "Fail"

    # filtering data
    pdf2 = pdf.where(filter_fail)
    pdf_fail = pdf2.dropna()

    if no_color:
        green = ""
        yellow = ""
        # red = ""
        endc = ""
    else:
        green = bcolors.OKGREEN
        yellow = bcolors.WARNING
        # red = bcolors.FAIL
        endc = bcolors.ENDC

    # display
    print(green + "========================================================")
    print("DETAILED SUMMARY TESTS")
    print("========================================================" + endc)
    pd.set_option("max_rows", 1500)
    print(pdf.groupby(["TestGroup", "InputType"])["Result"].value_counts())
    print(yellow + "========================================================")
    print("FAILED TESTS" + yellow)
    print("========================================================" + endc)
    # pd.set_option('max_columns', 5)
    # pd.set_option('max_colwidth', 1000)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 2000)
    pd.set_option("display.float_format", "{:20,.2f}".format)
    pd.set_option("display.max_colwidth", -1)
    print(
        pdf_fail.groupby(["TestGroup", "InputType", "Result"])["TestId"]
        .apply(",".join)
        .reset_index()
    )


# This function use the google spreadsheet to compare the current results
# against historic ones
# Returns a tuple with 2 entries:
# 1st element: False in case gpuci should be fail, True otherwise
# 2nd element: A list of error messages (in case 1st element is False)
# Example:
# result, error_msgs = verify_prev_google_sheet_results(log_pdf)
# if result == False:
#     exits the python process and do not move to next steps
# TODO william kharoly felipe we should try to enable and use
# this function in the future
def _verify_prev_google_sheet_results(log_pdf):
    def get_the_data_from_sheet():
        # Use creds to create a client to interact with the Google Drive API
        scope = [
            "https://www.googleapis.com/auth/drive",
            "https://spreadsheets.google.com/feeds",
        ]
        # Using credentials from BlazingSQL
        # os.getcwd() #Settings.data['TestSettings']['workspaceDirectory']
        # # #/home/kharoly/blazingsql/blazingdb-testing/BlazingSQLTest
        # current_dir = "/home/ubuntu/.conda/envs/e2e"

        log_info = Settings.data["RunSettings"]["logInfo"]

        if log_info == "":
            print(
                """####### ======= >>>>>>> WARNING this test run will not
                   be compared against old results from Google Docs. Define
                   the env var BLAZINGSQL_E2E_LOG_INFO"""
            )
            return None

        log_info = json.loads(log_info)
        creds_blazing = ServiceAccountCredentials.from_json_keyfile_dict(
            log_info, scope
        )
        client_blazing = gspread.authorize(creds_blazing)
        # Find a Locally workbook by name and open a sheet
        work_sheet = "BSQL Log Results"

        if "worksheet" in Settings.data["RunSettings"]:
            work_sheet = Settings.data["RunSettings"]["worksheet"]

        sheet_blazing = client_blazing.open("BSQL End-to-End Tests").worksheet(
            work_sheet
        )
        # Writing log results into Blazing sheet
        ret = pd.DataFrame(sheet_blazing.get_all_records())
        # NOTE percy kharo william we need to patch these columns
        # before convert to parquet
        ret["LoadingTime"] = ret["LoadingTime"].astype(str)
        ret["EngineTotalTime"] = ret["EngineTotalTime"].astype(str)
        ret["TotalTime"] = ret["TotalTime"].astype(str)
        return ret

    dir_log = Settings.data["TestSettings"]["logDirectory"]
    gspreadCacheHint = Settings.data["RunSettings"]["gspreadCacheHint"]
    gspread_e2e_cache_path = dir_log + "/e2e-gspread-cache.parquet"

    gspread_df = None

    if gspreadCacheHint == "false":
        gspread_df = get_the_data_from_sheet()
        if gspread_df is not None:
            # Always save a cache (so when gspreadCacheHint
            # is false will refresh the cache)
            gspread_df.to_parquet(gspread_e2e_cache_path)
    elif gspreadCacheHint == "true":
        if os.path.isfile(gspread_e2e_cache_path):
            gspread_df = pd.read_parquet(gspread_e2e_cache_path)
        else:
            gspread_df = get_the_data_from_sheet()
            if gspread_df is not None:
                gspread_df.to_parquet(gspread_e2e_cache_path)

    if gspread_df is None:
        error_msg = """ERROR: This test run could not be compared
                     against old results from Google Docs"""
        return False, [error_msg]

    log_pdf_copy = log_pdf.copy()
    prev_nrals = gspread_df["nRALS"][0]
    curr_nrals = Settings.data["RunSettings"]["nRals"]

    # Assume prev_nrals == curr_nrals
    last_e2e_run_id = gspread_df["Timestamp"][0]
    # NOTE If prev_nrals != curr_nrals we need to search the first
    # Timestamp (a.k.a ID) for the current nRals target
    if prev_nrals != curr_nrals:
        gspread_df_uniques = gspread_df.drop_duplicates()
        gspread_df_uniques_target_nrals = gspread_df_uniques.loc[
            gspread_df_uniques["nRALS"] == curr_nrals
        ]
        last_e2e_run_id = gspread_df_uniques_target_nrals.iloc[
            0, 1
        ]  # select the first Timestamp from the unique values

    print(
        "####### ======= >>>>>>> E2E INFO: We will compare the"
        + " current run against the ID (Timestamp): "
        + last_e2e_run_id
    )

    last_e2e_run_df = gspread_df.loc[gspread_df["Timestamp"] == last_e2e_run_id]

    # NOTE percy kharo william we need to rename some columns to use our dfs
    log_pdf_copy = log_pdf_copy.rename(
        columns={
            "TestGroup": "Test Group",
            "InputType": "Input Type",
            "nRals": "nRALS",
            "DataDirectory": "data_dir",
        }
    )

    # NOTE For debugging
    # log_pdf_copy['TimeStamp'] = log_pdf_copy['TimeStamp'].astype(str)
    # log_pdf_copy.to_parquet('/home/percy/workspace/logtest/ultimo.parquet',
    #                                                    compression='GZIP')
    # log_pdf_copy = pd.read_parquet('/home/user/last_run_log_df.parquet')

    error_msgs = []

    prev_summary = last_e2e_run_df.groupby("Test Group").count()
    curr_summary = log_pdf_copy.groupby("Test Group").count()

    prev_test_groups = prev_summary.index.tolist()
    curr_test_groups = curr_summary.index.tolist()

    has_less_test_groups = len(prev_test_groups) > len(curr_test_groups)

    # Check if someone deleted some tests
    # (there more test groups in the sheet)
    if has_less_test_groups:
        list_difference = [
            item for item in prev_test_groups if item not in curr_test_groups
        ]
        error_msg = (
            "ERROR: current e2e has less test groups than"
            + " previous run, delta is %s" % list_difference
        )
        error_msgs.append(error_msg)

    # Just check the common test groups
    if has_less_test_groups:
        test_groups = curr_test_groups
    else:
        test_groups = prev_test_groups

    for test_group in test_groups:
        prev_test_group_df = last_e2e_run_df.loc[
            last_e2e_run_df["Test Group"] == test_group
        ]
        prev_input_types = (
            prev_test_group_df.groupby("Input Type").count().index.tolist()
        )

        curr_test_group_df = log_pdf_copy.loc[log_pdf_copy["Test Group"] == test_group]
        cur_input_typ = curr_test_group_df.groupby("Input Type").count().index.tolist()

        has_less_input_types = len(prev_input_types) > len(cur_input_typ)

        if has_less_input_types is True:
            list_difference = [
                item for item in prev_input_types if item not in cur_input_typ
            ]
            error_msg = """ERROR: current test group %s has less
                         input types cases, delta is %s""" % (
                test_group,
                list_difference,
            )
            error_msgs.append(error_msg)

        for input_type in prev_input_types:
            prev_tests_df = prev_test_group_df.loc[
                prev_test_group_df["Input Type"] == input_type
            ]
            prev_tests_df.sort_values(by=["QueryID"])

            curr_tests_df = curr_test_group_df.loc[
                curr_test_group_df["Input Type"] == input_type
            ]
            curr_tests_df.sort_values(by=["QueryID"])

            # We need to make a copy since we are going to drop some row
            prev_tests_df = prev_tests_df.copy()
            curr_tests_df = curr_tests_df.copy()

            # NOTE for debugging
            # print("============================================PREV!")
            # print(prev_tests_df.head())
            # print(len(prev_tests_df))
            # print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxCURR!")
            # print(curr_tests_df.head())
            # print(len(curr_tests_df))

            # Check if current run has less tests than previous run
            len_prev_tests_df = len(prev_tests_df)
            len_curr_tests_df = len(curr_tests_df)
            has_less_tests = len_prev_tests_df > len_curr_tests_df

            # NOTE for debugging
            # print("====== PREV TESTS ======")
            # print(prev_tests_df)
            # print("====== CURR TESTS ======")
            # print(curr_tests_df)

            if has_less_tests:
                prev_tests = prev_tests_df["QueryID"].tolist()
                curr_tests = curr_tests_df["QueryID"].tolist()
                list_difference = [
                    item for item in prev_tests if item not in curr_tests
                ]
                error_msg = """ERROR: The test group %s has less tests than
                              previous run for input type %s, delta is %s""" % (
                    test_group,
                    input_type,
                    list_difference,
                )
                error_msgs.append(error_msg)

                n = len_prev_tests_df - len_curr_tests_df
                prev_tests_df.drop(prev_tests_df.tail(n).index, inplace=True)
            elif len_prev_tests_df < len_curr_tests_df:
                n = len_curr_tests_df - len_prev_tests_df
                curr_tests_df.drop(curr_tests_df.tail(n).index, inplace=True)

            prev_tests_results = prev_tests_df["Result"].to_list()
            curr_tests_results = curr_tests_df["Result"].to_list()

            for i in range(0, len(prev_tests_results)):
                prev_test_result = prev_tests_results[i]
                curr_test_result = curr_tests_results[i]

                if prev_test_result == 1 and curr_test_result == 0:
                    error_msg = """ERROR: Test %d for %s (%s) is now failing
                                  but before was ok!""" % (
                        i + 1,
                        test_group,
                        input_type,
                    )
                    error_msgs.append(error_msg)

    succs = len(error_msgs) == 0
    return succs, error_msgs


def saving_google_sheet_results(log_pdf):
    log_info = Settings.data["RunSettings"]["logInfo"]

    if log_info == "":
        print(
            """####### ======= >>>>>>> WARNING this test run will
             not save its results into the Google spreadsheet."""
        )
        return

    # Create an empty list
    log_list = []

    # Iterate over each row
    for index, rows in log_pdf.iterrows():
        # Create a list for the current row (ADDS)
        current_list = [
            rows.QueryID,
            str(rows.TimeStamp),
            str(rows.TestGroup),
            rows.InputType,
            rows.Query,
            rows.Result,
            rows.Error,
            rows.Branch,
            str(rows.CommitHash),
            rows.nRals,
            rows.nGPUs,
            rows.DataDirectory,
            rows.LoadingTime,
            rows.EngineTotalTime,
            rows.TotalTime,
        ]

        # append the list to the final list
        log_list.append(current_list)
    # Use creds to create a client to interact with the Google Drive API
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://spreadsheets.google.com/feeds",
    ]
    # === 1. BlazingSQL =====
    # Using credentials from BlazingSQL
    # os.getcwd() #Settings.data['TestSettings']['workspaceDirectory']
    # # #/home/kharoly/blazingsql/blazingdb-testing/BlazingSQLTest
    current_dir = "/home/ubuntu/.conda/envs/e2e"
    print(current_dir)

    log_info = json.loads(log_info)
    creds_blazing = ServiceAccountCredentials.from_json_keyfile_dict(log_info, scope)
    client_blazing = gspread.authorize(creds_blazing)
    # Find a Locally workbook by name and open a sheet
    work_sheet = "BSQL Log Results"
    if "worksheet" in Settings.data["RunSettings"]:
        work_sheet = Settings.data["RunSettings"]["worksheet"]
    blaz_googlesheat = client_blazing.open("BSQL End-to-End Tests")
    sheet_blazing = blaz_googlesheat.worksheet(work_sheet)
    # Writing log results into Blazing sheet
    total_queries = len(log_list)
    for i in range(0, total_queries):
        sheet_blazing.append_row(log_list[i])
        time.sleep(1)

    print("\nTable was uptdated into Blazing Google SpreadSheet")


def saveLogInFile(df):
    dir_log = Settings.data["TestSettings"]["logDirectory"]
    filepath = getFileName(dir_log)
    df.to_excel(filepath, index=False)


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def on_jenkins():
    # NOTE For more env vars see
    # https://wiki.jenkins.io/display/JENKINS/Building+a+software+project
    jenkins_job = os.environ.get("JOB_NAME")
    if jenkins_job is not None:
        return True

    return False


def print_tests(tests, onlyFails=False):
    print(
        """************************************************************
          *******************"""
    )

    tab = "    "

    failedPrefix = ""
    if onlyFails:
        failedPrefix = "FAILED"

    # TODO percy check None
    for extension in tests:
        if onlyFails:
            if extension == "parquet":
                print(
                    "!!!!!!!!!!!!!!!! "
                    + failedPrefix
                    + " "
                    + extension
                    + " TESTS !!!!!!!!!!!!"
                )
            else:
                print(
                    "!!!!!!!!!!!!!!!! "
                    + failedPrefix
                    + " "
                    + extension
                    + " TESTS !!!!!!!!!!!!!!!!"
                )
        else:
            if extension == "parquet":
                print("################ " + extension + " TESTS ############")
            else:
                print("############## " + extension + " TESTS ##############")

        testNames = tests.get(extension)
        for testName in testNames:
            test = testNames.get(testName)

            total = test.get("total")
            countPass = test.get("countPass")
            countCrash = test.get("countCrash")
            failIds = test.get("failIds")

            showTest = False

            if onlyFails:
                if len(failIds) > 0:
                    showTest = True
                    print(tab + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            else:
                showTest = True
                print(tab + "++++++++++++++++++++++++++++++++")

            if showTest:
                green = bcolors.OKGREEN
                yellow = bcolors.WARNING
                red = bcolors.FAIL
                endc = bcolors.ENDC

                # don't use colors since jenkins doesn't support ansi chars
                if on_jenkins():
                    green = ""
                    yellow = ""
                    red = ""
                    endc = ""

                print(
                    tab
                    + "SUMMARY for "
                    + failedPrefix
                    + " test suite: "
                    + testName
                    + " - "
                    + extension
                )

                if not onlyFails:
                    pass_green = green
                    pass_endc = endc
                    if (
                        countPass != total
                    ):  # if no full pass then don't use green colors here
                        pass_green = ""
                        pass_endc = ""

                    print(
                        pass_green
                        + tab
                        + "PASSED: "
                        + str(countPass)
                        + "/"
                        + str(total)
                        + pass_endc
                    )

                fails = total - countPass - countCrash
                yellow_fail = yellow
                yellow_endc = endc
                if fails == 0:
                    yellow_fail = ""
                    yellow_endc = ""

                print(
                    yellow_fail
                    + tab
                    + "FAILED: "
                    + str(fails)
                    + "/"
                    + str(total)
                    + " "
                    + str(failIds)
                    + yellow_endc
                )

                red_crash = red
                red_endc = endc

                # if no crashes then don't use red colors here
                if countCrash == 0:
                    red_crash = ""
                    red_endc = ""

                print(
                    red_crash
                    + tab
                    + "CRASH: "
                    + str(countCrash)
                    + "/"
                    + str(total)
                    + red_endc
                )

                if not onlyFails:
                    print(tab + "TOTAL: " + str(total))


def printSummary(countPass, countCrash, total, no_color):

    if no_color:
        green = ""
        yellow = ""
        red = ""
        endc = ""
    else:
        green = bcolors.OKGREEN
        yellow = bcolors.WARNING
        red = bcolors.FAIL
        endc = bcolors.ENDC

    # Second: print the global summary (totals from all the tests)
    fails = total - countPass - countCrash
    print(
        """**********************************************************
          *********************"""
    )
    print("TOTAL SUMMARY for test suite: ")
    print(green + "PASSED: " + str(countPass) + "/" + str(total) + endc)
    print(yellow + "FAILED: " + str(fails) + "/" + str(total) + endc)
    print(red + "CRASH: " + str(countCrash) + "/" + str(total) + endc)
    print("TOTAL: " + str(total))


def getFileName(dir_log):

    fecha = time.strftime("%H%M%S")
    hora = time.strftime("%I%M%S")
    return dir_log + "LogTest" + fecha + hora + ".xlsx"  #


# ===========================================================================

tableNames = [
    "customer",
    "orders",
    "supplier",
    "lineitem",
    "part",
    "partsupp",
    "nation",
    "region",
    "perf",
    "acq",
    "names",
    "bool_orders",
    "web_site",
    "web_sales",
    "web_returns",
    "web_page",
    "web_clickstreams",
    "warehouse",
    "time_dim",
    "store_sales",
    "store_returns",
    "store",
    "ship_mode",
    "reason",
    "promotion",
    "product_reviews",
    "item_marketprices",
    "item",
    "inventory",
    "income_band",
    "household_demographics",
    "date_dim",
    "customer_demographics",
    "customer_address",
    "customer",
]


def get_table_occurrences(query):
    res = []
    for name in tableNames:
        if query.find(name) != -1:
            res.append(name)
    return res


def replace_all(text, dic):
    for i, j in dic.items():
        text = re.sub(r"\s%s(\s|$|\,)" % i, j, text)
    return text


def get_blazingsql_query(db_name, query):
    new_query = query
    for table_name in get_table_occurrences(query):
        new_query = replace_all(
            new_query,
            {table_name: " %(table)s " % {"table": db_name + "." + table_name}},
        )
    return new_query


def get_drill_query(query):
    new_query = query
    for table_name in get_table_occurrences(query):
        new_query = replace_all(
            new_query, {table_name: " dfs.tmp.`%(table)s` " % {"table": table_name}}
        )
    return new_query


# ================================================================================================================


def run_query_drill(drill, query_str):
    timeout = 400
    query_result = drill.query(query_str, timeout)
    df = query_result.to_dataframe()
    if df.size == 0:
        return Result(query_result.columns, df, None)
    df = df[query_result.columns]
    result = Result(query_result.columns, df, None)
    return result


def run_query_spark(spark, query_str):
    query_result = spark.sql(query_str)
    df = query_result.toPandas()
    if df.size == 0:
        return Result(query_result.columns, df, None)
    df = df[query_result.columns]
    result = Result(query_result.columns, df, None)
    return result


def save_results_arrow(filename, pdf2):
    # save results
    import pyarrow as pa

    table = pa.Table.from_pandas(pdf2)
    # schema = pa.Schema.from_pandas(pdf2)
    with open(filename, "bw") as f:
        writer = pa.RecordBatchFileWriter(f, table.schema)
        writer.write(table)
        writer.close()


def save_results_parquet(filename, pdf2):
    pdf2.to_parquet(filename, compression="GZIP")


def run_query(
    bc,
    engine,
    query,
    queryId,
    queryType,
    worder,
    orderBy,
    acceptable_difference,
    use_percentage,
    input_type,
    **kwargs
):
    print(query)
    
    query_spark = kwargs.get("query_spark", query)

    algebra = kwargs.get("algebra", "")

    nRals = Settings.data["RunSettings"]["nRals"]

    print_result = kwargs.get("print_result")
    if print_result is None:
        print_result = False

    data_type = cs.get_extension(input_type)

    if Settings.execution_mode != "Generator":
        print(
            "\n=============== New query: "
            + str(queryId)
            + " - "
            + data_type
            + " ================="
        )

    load_time = 0
    engine_time = 0
    total_time = 0

    nested_query = kwargs.get("nested_query")
    if nested_query is None:
        nested_query = False

    if not nested_query:
        # if int(nRals) == 1:  # Single Node
        query_blz = query  # get_blazingsql_query('main', query)
        if algebra == "":
            start_time = time.time()
            result_gdf = bc.sql(query_blz)
            end_time = time.time()
            total_time = (end_time - start_time) * 1000
            # SUM(CASE WHEN info = 'evaluate_split_query load_data' THEN
            # duration ELSE 0 END) AS load_time,
            # MAX(load_time) AS load_time,
           
        else:
            result_gdf = bc.sql(query_blz, algebra=algebra)

    else:  # for nested queries as column basis test
        result_gdf = kwargs.get("blz_result")
        if result_gdf is None:
            result_gdf = []

    str_code_test = str(get_codTest(queryType)).upper()
    filename = str_code_test + "-" + str(queryId) + ".parquet"

    result_dir = Settings.data["TestSettings"]["fileResultsDirectory"]
    file_results_dir = str(result_dir)

    if not isinstance(engine, str):
        if isinstance(engine, PyDrill):
            # Drill
            query_drill = get_drill_query(query)
            result_drill_gd = run_query_drill(engine, query_drill)
            if result_gdf is not None:
                if result_gdf.columns is not None:
                    # FOR DASK CUDF
                    import dask_cudf

                    if type(result_gdf) is dask_cudf.core.DataFrame:
                        result_gdf = result_gdf.compute()

                    expected_dtypes = result_gdf.dtypes.to_list()
                    pdf1 = (
                        upcast_to_float(result_gdf)
                        .fillna(get_null_constants(result_gdf))
                        .to_pandas()
                    )
                    pdf2 = to_pandas_f64_engine(
                        result_drill_gd.resultSet, expected_dtypes
                    )
                    pdf2 = upcast_to_float(pdf2).fillna(get_null_constants(pdf2))
                    formatResults(pdf1, pdf2, worder, orderBy)

                    if Settings.execution_mode == ExecutionMode.GENERATOR:
                        file_res_drill_dir = (
                            file_results_dir + "/" + "drill" + "/" + filename
                        )

                        if not os.path.exists(file_res_drill_dir):
                            save_results_parquet(file_res_drill_dir, pdf2)

                        print("Drill: " + filename + " generated.")

                    else:
                        print_query_results(
                            query,
                            queryId,
                            queryType,
                            pdf1,
                            pdf2,
                            result_gdf,
                            acceptable_difference,
                            use_percentage,
                            print_result,
                            engine,
                            input_type,
                            load_time,
                            engine_time,
                            total_time,
                        )

                else:
                    print_query_results2(
                        query, queryId, queryType, result_gdf.error_message
                    )
        elif isinstance(engine, SparkSession):
            # Spark
            result_spark_df = run_query_spark(engine, query_spark)

            if result_gdf is not None:
                if result_gdf.columns is not None:

                    import dask_cudf

                    if type(result_gdf) is dask_cudf.core.DataFrame:
                        result_gdf = result_gdf.compute()

                    expected_dtypes = result_gdf.dtypes.to_list()
                    pdf1 = (
                        upcast_to_float(result_gdf)
                        .fillna(get_null_constants(result_gdf))
                        .to_pandas()
                    )
                    pdf2 = to_pandas_f64_engine(
                        result_spark_df.resultSet, expected_dtypes
                    )
                    pdf2 = upcast_to_float(pdf2).fillna(get_null_constants(pdf2))
                    formatResults(pdf1, pdf2, worder, orderBy)

                    if Settings.execution_mode == ExecutionMode.GENERATOR:

                        file_res_drill_dir = (
                            file_results_dir + "/" + "spark" + "/" + filename
                        )

                        if not os.path.exists(file_res_drill_dir):
                            save_results_parquet(file_res_drill_dir, pdf2)
                            print("Spark: " + filename + " generated.")

                    else:
                        print_query_results(
                            query_spark,
                            queryId,
                            queryType,
                            pdf1,
                            pdf2,
                            result_gdf,
                            acceptable_difference,
                            use_percentage,
                            print_result,
                            engine,
                            input_type,
                            load_time,
                            engine_time,
                            total_time,
                        )
            else:
                print_query_results2(
                    query_spark, queryId, queryType, result_gdf.error_message
                )
    else:  # GPUCI

        compareResults = True
        if "compare_results" in Settings.data["RunSettings"]:
            compareResults = Settings.data["RunSettings"]["compare_results"]

        if compareResults == "true":
            resultFile = file_results_dir + "/" + str(engine) + "/" + filename
            pdf2 = get_results(resultFile)
            if result_gdf is not None:
                if result_gdf.columns is not None:
                    # FOR DASK CUDF
                    import dask_cudf

                    if type(result_gdf) is dask_cudf.core.DataFrame:
                        result_gdf = result_gdf.compute()

                    expected_dtypes = result_gdf.dtypes.to_list()
                    pdf1 = (
                        upcast_to_float(result_gdf)
                        .fillna(get_null_constants(result_gdf))
                        .to_pandas()
                    )
                    format_pdf(pdf1, worder, orderBy)
                    print(pdf2)

                    print_query_results(
                        query,
                        queryId,
                        queryType,
                        pdf1,
                        pdf2,
                        result_gdf,
                        acceptable_difference,
                        use_percentage,
                        print_result,
                        engine,
                        input_type,
                        load_time,
                        engine_time,
                        total_time,
                    )

                else:
                    print_query_results2(
                        query, queryId, queryType, result_gdf.error_message
                    )
        else:
            if result_gdf is not None:
                if result_gdf.columns is not None:
                    # FOR DASK CUDF
                    import dask_cudf

                    if type(result_gdf) is dask_cudf.core.DataFrame:
                        result_gdf = result_gdf.compute()

                    expected_dtypes = result_gdf.dtypes.to_list()
                    pdf1 = (
                        upcast_to_float(result_gdf)
                        .fillna(get_null_constants(result_gdf))
                        .to_pandas()
                    )
                    pdf2 = pd.DataFrame()
                    formatResults(pdf1, pdf2, worder, orderBy)

                    print_query_results(
                        query,
                        queryId,
                        queryType,
                        pdf1,
                        pdf2,
                        result_gdf,
                        acceptable_difference,
                        use_percentage,
                        print_result,
                        engine,
                        input_type,
                        load_time,
                        engine_time,
                        total_time,
                    )
            else:
                print_query_results2(
                    query, queryId, queryType, result_gdf.error_message
                )


def run_query_performance(
    bc,
    drill,
    query,
    queryId,
    queryType,
    worder,
    orderBy,
    acceptable_difference,
    use_percentage,
    **kwargs
):
    # Blazing
    query_blz = query  # get_blazingsql_query('main', query)
    result_gdf = bc.sql(query_blz).get()
    if result_gdf.error_message == "":
        print_query_results_performance(query, queryId, queryType, result_gdf)
    else:
        print_query_results2(query, queryId, queryType, result_gdf.error_message)


def formatResults(pdf1, pdf2, worder, orderBy):
    if worder == 1 and pdf1.size != 0 and pdf2.size != 0:
        if len(pdf1.columns) == len(pdf2.columns):
            pdf1.sort_values(
                [orderBy] if orderBy else pdf1.columns.to_list(), inplace=True
            )
            pdf2.sort_values(
                [orderBy] if orderBy else pdf2.columns.to_list(), inplace=True
            )


def format_pdf(pdf, worder, orderBy):
    if worder == 1 and pdf.size != 0:
        pdf.sort_values([orderBy] if orderBy else pdf.columns.to_list(), inplace=True)


def get_results(result_file):
    df = pd.read_parquet(result_file)

    return df
