import time

import pymysql

from Configuration import Settings


def openCon():

    connection = pymysql.connect(
        host=Settings.data["MysqlConnection"]["host"],
        user=Settings.data["MysqlConnection"]["user"],
        port=int(Settings.data["MysqlConnection"]["port"]),  # optional
        password=Settings.data["MysqlConnection"]["password"],
        db=Settings.data["MysqlConnection"]["database"],
    )

    return connection


def getQueryKey(queryType, queryId, query):

    connection = openCon()

    queryKey = -1

    try:

        with connection.cursor() as cursor:
            # Read a single record
            sql = """SELECT `queryKey` FROM `tbl_Querys` where
                     `query` =%s and `testType` =%s """
            cursor.execute(sql, (query, queryType))
            result = cursor.fetchone()

            if result is not None:
                queryKey = result[0]
            else:
                queryKey = 0

        if queryKey == 0:
            with connection.cursor() as cursor:
                # Create a new record
                fecha = time.strftime("%d/%m/%y")
                sql = """INSERT INTO `tbl_Querys`
                     (`testType`, `testId`, `query`, `dateRecord`)
                     VALUES (%s, %s, %s, %s)"""
                cursor.execute(sql, (queryType, queryId, query, fecha))
                connection.commit()

            with connection.cursor() as cursor:
                # Read a single record
                sql = """SELECT `queryKey` FROM `tbl_Querys` where
                        `query` =%s and `testType` =%s """
                cursor.execute(sql, (query, queryType))
                result = cursor.fetchone()

                if result is not None:
                    queryKey = result[0]
                else:
                    queryKey = 0

    finally:
        connection.close()

    return queryKey


def insertExecution(
    connection,
    dataSet,
    environment,
    pathLog,
    dateTest,
    pathDataSet,
    calciteBranch,
    ralBranch,
    cudfBranch,
    orchBranch,
    protocolBranch,
    blzIoBranch,
    pyBlzBranch,
    calciteCommit,
    ralCommit,
    cudfCommit,
    orchCommit,
    protocolCommit,
    blzIoCommit,
    pyBlzCommit,
    jobId,
):

    # connection = openCon()

    try:

        with connection.cursor() as cursor:
            # Create a new record
            sql = """INSERT INTO `tbl_Execution` (`dataSet` , `environment`,
                 `pathLog`, `dateTest`, `pathDataSet`, `calciteBranch`,
                 `ralBranch`, `cudfBranch`, `orchBranch`, `protocolBranch`,
                 `blzIoBranch`, `pyBlzBranch`, `calciteCommit`, `ralCommit`,
                 `cudfCommit`, `orchCommit`, `protocolCommit`, `blzIoCommit`,
                 `pyBlzCommit`, `jobId`) VALUES (%s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(
                sql,
                (
                    dataSet,
                    environment,
                    pathLog,
                    dateTest,
                    pathDataSet,
                    calciteBranch,
                    ralBranch,
                    cudfBranch,
                    orchBranch,
                    protocolBranch,
                    blzIoBranch,
                    pyBlzBranch,
                    calciteCommit,
                    ralCommit,
                    cudfCommit,
                    orchCommit,
                    protocolCommit,
                    blzIoCommit,
                    pyBlzCommit,
                    jobId,
                ),
            )
            connection.commit()

    finally:
        # connection.close()
        return True


def getExecutionId(connection):

    # connection = openCon()
    executionId = -1
    try:

        with connection.cursor() as cursor:
            # Get the last ID inserted
            sql = """select MAX(idExecution) from tbl_Execution;"""
            cursor.execute(sql, ())

            result = cursor.fetchone()
            if result is not None:
                executionId = result[0]
            else:
                executionId = 0
    finally:
        # connection.close()
        return executionId


def getJobId():

    connection = openCon()
    jobId = -1
    try:

        with connection.cursor() as cursor:
            # Get the last ID inserted
            sql = """select MAX(jobId) from tbl_Execution;"""
            cursor.execute(sql, ())

            result = cursor.fetchone()
            if result is not None:
                res = int(result[0])
                jobId = res + 1
            else:
                jobId = 1
    finally:
        connection.close()
        return jobId


def insertExecutionDetail(
    connection,
    queryKey,
    resultTest,
    errorTest,
    calciteTime,
    ralTime,
    totalTime,
    idExecution,
):

    try:

        # connection = openCon()

        with connection.cursor() as cursor:
            # Create a new record

            sql = """INSERT INTO `tbl_ExecutionDetail` (`queryKey`,
                     `resultTest`, `calciteTime`, `ralTime`, `totalTime`,
                     `idExecution`, `resultError`)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(
                sql,
                (
                    queryKey,
                    resultTest,
                    calciteTime,
                    ralTime,
                    totalTime,
                    idExecution,
                    errorTest,
                ),
            )
            connection.commit()

    finally:
        # connection.close()
        return True


def insertInfoExecution(
    jobId,
    df,
    dataSet,
    environment,
    pathLog,
    dateTest,
    pathDataSet,
    calciteBranch,
    ralBranch,
    cudfBranch,
    orchBranch,
    protocolBranch,
    blzIoBranch,
    pyBlzBranch,
    calciteCommit,
    ralCommit,
    cudfCommit,
    orchCommit,
    protocolCommit,
    blzIoCommit,
    pyBlzCommit,
):
    try:

        connection = openCon()

        insertExecution(
            connection,
            dataSet,
            environment,
            pathLog,
            dateTest,
            pathDataSet,
            calciteBranch,
            ralBranch,
            cudfBranch,
            orchBranch,
            protocolBranch,
            blzIoBranch,
            pyBlzBranch,
            calciteCommit,
            ralCommit,
            cudfCommit,
            orchCommit,
            protocolCommit,
            blzIoCommit,
            pyBlzCommit,
            jobId,
        )

        totalRows = df.shape[0]

        idExecution = getExecutionId(connection)

        for i in range(0, totalRows):
            queryKey = int(df["QueryKey"].iloc[i])
            resultTest = str(df["Result"].iloc[i])
            errorTest = str(df["Error"].iloc[i])
            if resultTest != "Success" and resultTest != "Fail":
                calciteTime = 0
                ralTime = 0
                totalTime = 0
            else:
                calciteTime = float(df["CalciteTime"].iloc[i])
                ralTime = float(df["RalTime"].iloc[i])
                totalTime = float(df["ExecutionTime"].iloc[i])
            insertExecutionDetail(
                connection,
                queryKey,
                resultTest,
                errorTest,
                calciteTime,
                ralTime,
                totalTime,
                idExecution,
            )

    except pymysql.connector.Error as error:
        print("Failed to update record to database rollback: {}".format(error))
        connection.rollback()
        return False
    finally:
        # closing database connection.
        if connection.open:
            connection.close()
        return True
