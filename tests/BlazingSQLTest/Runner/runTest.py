# Cast column to f64 before convert it to pandas
# This is a hack, use the assert_equal comparator when nulls is
# fully supported on cudf.sort_values

import json
import logging
import os
import re
import time
import yaml
import numpy as np
import pandas as pd

import blazingsql
from blazingsql import DataType

from BlazingLogging import loggingHandler as lhandler
from Configuration import ExecutionMode
from Configuration import Settings as Settings

from DataBase import createSchema as cs

if ((Settings.execution_mode == ExecutionMode.FULL and
     Settings.compare_res == "true") or
            Settings.execution_mode == ExecutionMode.GENERATOR):
    print(Settings.execution_mode)
    print(Settings.compare_res)
    from pydrill.client import PyDrill
    from pyspark.sql.session import SparkSession

class Result:
    def __init__(self, columns, resultSet, resultBlz):
        self.columns = columns
        self.resultSet = resultSet
        self.resultBlz = resultBlz

name = "blzlogging"

HANDLER = lhandler.logging_handler()


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
        elif np.issubdtype(df[name].dtype, np.timedelta64):
            # issue: cannot astype a timedelta from [timedelta64[ns]] to [float64]
            # so first cast from timedelta64[ns] to timedelta64[ms] and after to np.float64
            df[name] = df[name].astype('timedelta64[ms]')
            df[name] = df[name].astype(np.float64)
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
                    elif np.issubdtype(expected_types_list[count], np.timedelta64):
                        # Drill case: always converts to timedelta64[ns]
                        df[col] = pd.to_timedelta(df[col])
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


def compare_result_values(pdf1, pdf2, acceptable_difference, use_percentage, engine):   
    """
        The purpose of this functions is to compare the values from blazingsql and drill/spark results.

        ----------
        pdf1 : blazing results (pandas dataframe)
        pdf2: drill/spark results (pandas dataframe)
        acceptable_difference: This parameter is related to the acceptable difference beetween values 
        from blazingsql results and drill/spark results.
        use_percentage: (True/False) to indicate if the results will be compared by percentage or difference.
        engine: pydrill or pyspark instances
    """

    np.warnings.filterwarnings("ignore")

    if pdf1.size == 0 and pdf2.size == 0:
        return "Success"

    msg = ""
    if not isinstance(engine, str):
        if isinstance(engine, PyDrill):
            msg = "PyDrill"
        else:
            msg = "PySpark"
    elif engine=="drill":
        msg = "PyDrill"
    else:
        msg = "PySpark"

    msg = ""
    if not isinstance(engine, str):
        if isinstance(engine, PyDrill):
            msg = "PyDrill"
        else:
            msg = "PySpark"
    elif engine=="drill":
        msg = "PyDrill"
    else:
        msg = "PySpark"

    if pdf1.shape[0] == pdf2.shape[0]:
        if pdf1.shape[1] == pdf2.shape[1]:

            for name in pdf1.columns:
                if pdf1[name].dtype == np.object:
                    pdf1[name] = pdf1[name].astype('string')

            for name in pdf2.columns:
                if pdf2[name].dtype == np.object:
                    pdf2[name] = pdf2[name].astype('string')

            # Removing indexes, because those are considered when
            # comparing with equals()
            pdf1.reset_index(drop=True, inplace=True)
            pdf2.reset_index(drop=True, inplace=True)

            # Make the column labels equal as equals() also compare labels
            orig_pdf2_labels = pdf2.columns.to_list()
            pdf2.columns = pdf1.columns.to_list()

            exac_comp = pdf1.select_dtypes(exclude=np.inexact).equals(
                pdf2.select_dtypes(exclude=np.inexact)
            )

            # Restore labels
            pdf2.columns = orig_pdf2_labels

            tmp_pdf1 = pdf1.select_dtypes(include=np.inexact)
            tmp_pdf2 = pdf2.select_dtypes(include=np.inexact)


            if use_percentage:
                relative_tolerance = acceptable_difference
                absolute_tolerance = 0
            else:
                relative_tolerance = 0
                absolute_tolerance = acceptable_difference
            # np.allclose follows this formula:
            #    absolute(a - b) <= (absolute_tolerance + relative_tolerance * absolute(b))

            res = np.all(exac_comp) and np.allclose(
                tmp_pdf1.values, tmp_pdf2.values, relative_tolerance,
                absolute_tolerance, equal_nan=True
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
                    + msg
                    + " result: "
                    + str(pdf2.shape[1])
                )
    else:
        return (
            "Fail: Different number of rows blzSQLresult: "
            + str(pdf1.shape[0])
            + " "
            + msg
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
    cwd = os.path.dirname(os.path.realpath(__file__))
    fileName = cwd + "/targetTest.yml"
    if os.path.isfile(fileName):
        with open(fileName, 'r') as stream:
            fileYaml = yaml.safe_load(stream)["LIST_TEST"]

    if test_name in fileYaml:
        if "CODE" in fileYaml[test_name]:
            return fileYaml[test_name]["CODE"]
    else: #Legacy Test
        for item in fileYaml:
            if "NAME" in fileYaml[item] and fileYaml[item]["NAME"] == test_name:
                return fileYaml[item]["CODE"]

    raise Exception("ERROR: CODE configuration not found for '" + test_name + "' in targetTest.yaml, i.e. CODE: BALIAS")

def logger_results(
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


def compare_test_results(  pdf1,
						pdf2,
						acceptable_difference,
						use_percentage,
						engine,
						comparing=True
					):

    """
        Compare values, number of rows, columns, and column names

        ----------
        pdf1 : blazing results (pandas dataframe)
        pdf2: drill/spark results (pandas dataframe)
        acceptable_difference: This parameter is related to the acceptable difference beetween values 
        from blazingsql results and drill/spark results.
        use_percentage: (True/False) to indicate if the results will be compared by percentage or difference.
        comparing: Parameter to indicate if the results from blazingsql will be compared with the results from drill or spark
    """

    compareResults = True
    error_message = ""
    stringResult = ""

    if "compare_result_values" in Settings.data["RunSettings"]:
        compareResults = Settings.data["RunSettings"]["compare_result_values"]

    # For dateTest (CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP)
    if not comparing:
        compareResults = False

    if compareResults:
        columnNamesComparison = compare_column_names(pdf1, pdf2)
        if columnNamesComparison is not True:
            error_message = "Column names are not the same"

        resultComparisson = compare_result_values(
            pdf1, pdf2, acceptable_difference, use_percentage, engine
        )
        if resultComparisson != "Success":
            error_message = resultComparisson[6:]

        stringResult = resultComparisson
        if resultComparisson != "Success" or columnNamesComparison is False:
            stringResult = "Fail"
    else:
        stringResult = "Success"

    return error_message, stringResult, columnNamesComparison, resultComparisson

def print_comparison_results(
    sql,
    queryId,
    queryType,
    pdf1,
    pdf2,
    print_result,
    engine,
    input_type,
    total_time,
    error_message,
    stringResult,
    columnNamesComparison,
    resultComparisson
):
    if print_result:
        print("#BLZ:")
        print(pdf1)
        if not isinstance(engine, str):
            if isinstance(engine, PyDrill):
                print("#DRILL:")
            else:
                print("#PYSPARK:")
            print(pdf2)
        else:
            if engine=="drill":
                print("#DRILL:")
            else:
                print("#PYSPARK:")
    data_type = cs.get_extension(input_type)
    print(str(queryId) + " Test " + queryType + " - " + data_type)
    print("#QUERY:")
    print(sql)
    print("RESULT:")
    print(stringResult)
    if columnNamesComparison is not True:
        print("Columns:")
        print(pdf1.columns)
        print(pdf2.columns)
        print("ERROR:")
        print(error_message)
    if resultComparisson != "Success":
        print("ERROR:")
        print(error_message)

    print("TOTAL TIME: ")
    print(total_time)
    print("CRASHED NODES: ")
    # print(resultgdf.n_crashed_nodes)
    print("TOTAL NODES: ")
    # print(resultgdf.total_nodes)
    print("===================================================")

def print_validation_results(sql, queryId, input_type, queryType, error_message, message_validation):
    print(queryId)
    print("#QUERY:")
    print(sql)
    print("RESULT:")
    result = validate_messages(error_message, message_validation)
    print(result)
    print("ERROR:")
    if result=="Fail":
        print(error_message)
    else:
        error_message=""
    print("CALCITE TIME: ")
    print("-")
    print("RAL TIME: ")
    print("-")
    print("EXECUTION TIME: ")
    print("-")

    print("===================================================")

    logger = logginghelper(name)

    logger_results(
        logger, queryType, input_type, queryId, sql, result, error_message, None, None, None
    )

def print_performance_results(sql, queryId, queryType, resultgdf):
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

    logger_results(
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


class Test:
    def __init__(self, test_name):
        self.test_name = test_name
        self.total = 0
        self.success = 0
        self.fail_ids = []


def save_log(gpu_ci_mode=False):
    """
        put the log into a pandas dataframe
    """

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
        if countPass < total:
            result, error_msgs = False, []
        else:
            result, error_msgs = True, []

    loggingClose(name)
    return result, error_msgs


def create_summary_detail(df, no_color):

    """
        Build a summary with the details about hoy many queries pass/fail  and what queries have the failed status.

        ----------
        df : pdf log
    """

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
    pd.set_option("display.max_colwidth", None)
    print(
        pdf_fail.groupby(["TestGroup", "InputType", "Result"])["TestId"]
        .apply(",".join)
        .reset_index()
    )


def saveLogInFile(df):
    dir_log = Settings.data["TestSettings"]["logDirectory"]
    filepath = getFileName(dir_log)
    df.to_excel(filepath, index=False)

def validate_messages(error_message, message_validation):
    error_message = error_message.replace('\n', ' ').replace('\r', ' ')
    message_validation = message_validation.replace('\n', ' ').replace('\r', ' ')
    error_message = error_message.replace(' ', '')
    message_validation = message_validation.replace(' ', '')

    if error_message == message_validation:
        result = "Success"
    else:
        result = "Fail"

    return result

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
    """
        After we have the pdf log with this function we are printing the values on the screem at the end of the execution tests.

        ----------
        tests : pdf log
        onlyFails: we can choose print only the failed results and queries.
    """
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
    "interval_table",
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
    "split",
    "docked",
    "smiles",
    "dcoids",
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
        # for concurrent test and tables from sql tests
        enum_list = list(map(lambda c: c.name, DataType))
        a = ["_"+e for e in enum_list]
        a.remove("_UNDEFINED")
        for dtyp in a:
            new_query = new_query.replace(str(dtyp), "")

        # patch the tables
        new_query = replace_all(
            new_query, {table_name: " dfs.tmp.`%(table)s` " % {"table": table_name}}
        )
    return new_query


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

def run_query_log(
    bc,
    query,
    queryId,
    queryType,
    **kwargs
):
    result_gdf = None
    error_message = ""
    message_validation = ""

    try:
        result_gdf = bc.log(query)
    except Exception as e:
        error_message=str(e)

    if result_gdf is not None:
        if result_gdf.columns is not None:
            # FOR DASK CUDF
            import dask_cudf

            if type(result_gdf) is dask_cudf.core.DataFrame:
                result_gdf = result_gdf.compute()

            print_validation_results(
                query, queryId, DataType.CUDF, queryType, error_message, message_validation
            )
    else:
        print_validation_results(
            query, queryId, DataType.CUDF, queryType, error_message, message_validation
        )

def save_results_parquet(filename, pdf2):
    pdf2.to_parquet(filename, compression="GZIP")

def results_file_generator(file_results_dir, testsWithNulls, filename, engine, pdf2):
    file_res_drill_dir = None
    if testsWithNulls != "true":
        file_res_drill_dir = (
            file_results_dir + "/" + engine + "/" + filename
        )
    else:
        file_res_drill_dir = (
            file_results_dir + "/" + engine + "-nulls" + "/" + filename
        )

    if not os.path.exists(file_res_drill_dir):
        save_results_parquet(file_res_drill_dir, pdf2)
    print(engine.capitalize() + ": " + filename + " generated.")


def run_query_blazing(bc, nested_query, query, algebra, message_validation, blz_result):
    
    result_gdf = None

    load_time = 0
    engine_time = 0
    total_time = 0

    error_message = ""

    if not nested_query:
        # if int(nRals) == 1:  # Single Node
        query_blz = query 
        if algebra == "":
            start_time = time.time()
            try:
                result_gdf = bc.sql(query_blz)
            except Exception as e:
                error_message=str(e)

            if not message_validation:
                end_time = time.time()
                total_time = (end_time - start_time) * 1000

                load_time = 0  # n_log['load_time'][0]
                engine_time = 0 #n_log["total_time"][0]
        else:
            result_gdf = bc.sql(query_blz, algebra=algebra)

    else:  # for nested queries as column basis test
        result_gdf = blz_result
    
    return result_gdf, load_time, engine_time, total_time, error_message
        

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
    """
        This function execute the query with blazingsql and drill/spark and call the functions to compare, print results
        and logs.

        ----------
        bc : blazing context
        engine: It's the instance of the engine (pydrill/ỳspark).
        query: Executed query.
        queryId: Query Id.
        worder : (True/False) parameter to indicate if it's neccesary to order the results.
        orderBy : It indicate by what column we want to order the results.
        acceptable_difference: This parameter is related to the acceptable difference beetween values 
        from blazingsql results and drill/spark results.
        use_percentage: (True/False) to indicate if the results will be compared by percentage or difference.
        input_type: The data type (CSV, PARQUET, DASK_CUDF, JSON, ORC, GDF) that we use to run the query.
    """

    print(query)

    worder = 1 if worder == True else worder

    query_spark = kwargs.get("query_spark", query)

    algebra = kwargs.get("algebra", "")

    comparing = kwargs.get("comparing", True)

    nRals = Settings.data["RunSettings"]["nRals"]

    print_result = kwargs.get("print_result")
    if print_result is None:
        print_result = False

    message_validation = kwargs.get("message_validation", "")
    if message_validation is None:
        message_validation = False

    nested_query = kwargs.get("nested_query", False)

    blz_result = None
    if nested_query:
        blz_result = kwargs.get("blz_result", [])

    data_type = cs.get_extension(input_type)

    if Settings.execution_mode != "generator":
        print(
            "\n=============== New query: "
            + str(queryId)
            + " - "
            + data_type
            + " ("+queryType+")" + "================="
        )

    str_code_test = str(get_codTest(queryType)).upper()
    filename = str_code_test + "-" + str(queryId) + ".parquet"

    result_dir = Settings.data["TestSettings"]["fileResultsDirectory"]
    file_results_dir = str(result_dir)

    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]

    result_gdf, load_time, engine_time, total_time, error_message = run_query_blazing(bc, nested_query, query, algebra, message_validation, blz_result)

    base_results_gd = None

    compareResults = True

    resultFile = None

    str_engine = ""

    if not message_validation== "":
        print_validation_results(
                        query,
                        queryId,
                        input_type,
                        queryType,
                        error_message,
                        message_validation
                )
    elif not isinstance(engine, str):
        if isinstance(engine, PyDrill):
            # Drill
            query_drill = get_drill_query(query)
            base_results_gd = run_query_drill(engine, query_drill)
            str_engine="drill"

        elif isinstance(engine, SparkSession):
            # Spark
            base_results_gd = run_query_spark(engine, query_spark)
            str_engine="spark"

    else:  # GPUCI
        if "compare_result_values" in Settings.data["RunSettings"]:
            compareResults = Settings.data["RunSettings"]["compare_result_values"]

        if compareResults == "true":
            if testsWithNulls != "true":
                resultFile = file_results_dir + "/" + str(engine) + "/" + filename
            else:
                resultFile = file_results_dir + "/" + str(engine) + "-nulls" + "/" + filename

    results_processing(result_gdf, 
                        base_results_gd, 
                        worder, 
                        orderBy, 
                        testsWithNulls, 
                        filename, 
                        query, 
                        queryId, 
                        queryType,
                        acceptable_difference,
                        use_percentage,
                        print_result,
                        engine,
                        input_type,
                        load_time,
                        engine_time,
                        total_time,
                        comparing,
                        compareResults,
                        resultFile,
                        file_results_dir,
                        str_engine)


def results_processing(result_gdf, 
                    base_results_gd, 
                    worder, 
                    orderBy, 
                    testsWithNulls, 
                    filename, 
                    query, 
                    queryId, 
                    queryType,
                    acceptable_difference,
                    use_percentage,
                    print_result,
                    engine,
                    input_type,
                    load_time,
                    engine_time,
                    total_time,
                    comparing,
                    compareResults,
                    resultFile,
                    file_results_dir,
                    str_engine
                    ):

    """
        Results processing of the query execution with blazingsql and drill/spark.
        It include the results formatting, comparison and print of results.

        ----------
        result_gdf : blazing results gdf
        base_results_gd : drill/spark results gdf
        worder : (True/False) parameter to indicate if it's neccesary to order the results.
        orderBy : It indicate by what column we want to order the results
        testsWithNulls : that is about the data with we get the results, if it contains nulls or not.
        filename: it's the name that the generated result file will have.
        query: Executed query.
        queryId: Query Id.
        acceptable_difference: This parameter is related to the acceptable difference beetween values 
        from blazingsql results and drill/spark results.
        use_percentage: (True/False) to indicate if the results will be compared by percentage or difference.
        print_result: (True/False) To show the query results information on the screen.
        engine: It's the instance of the engine (pydrill/ỳspark).
        input_type: The data type (CSV, PARQUET, DASK_CUDF, JSON, ORC, GDF) that we use to run the query.
        load_time: 
        engine_time: 
        total_time: Total time to execute the query 
        comparing: It indicate if the results will be compared with the results with the based engines (drill/spark)
        resultFile: Complete path where the results file will be saved.
        file_results_dir: Path where the results file will be saved. 
        str_engine: the values will be drill/spark

    """

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

            if not isinstance(engine, str):
                pdf2 = to_pandas_f64_engine(
                    base_results_gd.resultSet, expected_dtypes
                )
                pdf2 = upcast_to_float(pdf2).fillna(get_null_constants(pdf2))

                formatResults(pdf1, pdf2, worder, orderBy)
            else:
                if compareResults == "true":
                        format_pdf(pdf1, worder, orderBy)
                        pdf2 = get_results(resultFile)
                else:
                    pdf2 = pd.DataFrame()
                    formatResults(pdf1, pdf2, worder, orderBy)

            if Settings.execution_mode == ExecutionMode.GENERATOR:
                results_file_generator(file_results_dir, testsWithNulls, filename, str_engine, pdf2)
                print("==============================")
            else:
                error_message, stringResult, columnNamesComparison, resultComparisson = compare_test_results( pdf1,
                                                                                                            pdf2,
                                                                                                            acceptable_difference,
                                                                                                            use_percentage,
                                                                                                            engine,
                                                                                                            comparing)
                print_comparison_results(
                    query,
                    queryId,
                    queryType,
                    pdf1,
                    pdf2,
                    print_result,
                    engine,
                    input_type,
                    total_time,
                    error_message,
                    stringResult,
                    columnNamesComparison,
                    resultComparisson
                )

                logger = logginghelper(name)

                # TODO percy kharoly bindings we need to get the number from internal api
                logger_results(
                    logger,
                    queryType,
                    input_type,
                    queryId,
                    query,
                    stringResult,
                    error_message,
                    load_time,
                    engine_time,
                    total_time,
                )
        else:
            print_validation_results(
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
        print_performance_results(query, queryId, queryType, result_gdf)
    else:
        print_validation_results(query, queryId, queryType, result_gdf.error_message)


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
