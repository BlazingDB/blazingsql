import time
import traceback

from blazingdb.protocol.errors import Error
from .api import (_get_client, _to_table_group, _get_table_def_from_gdf, ResultSetHandle, _createResultSetIPCHandles)

async def run_query(sql, tables):
    """
    Run a SQL query over a dictionary of GPU DataFrames.
    Parameters
    ----------
    sql : str
        The SQL query.
    tables : dict[str]:GPU ``DataFrame``
        A dictionary where each key is the table name and each value is the
        associated GPU ``DataFrame`` object.
    Returns
    -------
    A GPU ``DataFrame`` object that contains the SQL query result.
    Examples
    --------
    >>> import cudf as gd
    >>> import pyblazing
    >>> products = gd.DataFrame({'month': [2, 8, 11], 'sales': [12.1, 20.6, 13.79]})
    >>> cats = gd.DataFrame({'age': [12, 28, 19], 'weight': [5.3, 9, 7.68]})
    >>> tables = {'products': products, 'cats': cats}
    >>> result = pyblazing.run_query('select * from products, cats limit 2', tables)
    >>> type(result)
    cudf.dataframe.DataFrame
    """
    return await _private_run_query(sql, tables)


async def _private_run_query(sql, tables):
    token = await _run_query_get_token(sql, tables)
    if token['error_message'] is not '':
        error = token['error_message']
        if isinstance(error, str):
            raise Exception(error)
        else:
            raise error
    return await _run_query_get_results(token)


async def _reset_table(client, table, gdf):
    await client.run_ddl_drop_table(table, 'main')
    cols, types = _get_table_def_from_gdf(gdf)
    await client.run_ddl_create_table(table, cols, types, 'main')


async def _run_query_get_results(metaToken):

    error_message = ''

    try:
        resultSet, ipchandles = await _private_get_result(
            metaToken["resultToken"],
            metaToken["interpreter_path"],
            metaToken["interpreter_port"]
        )

        totalTime = (time.time() - metaToken["startTime"]) * 1000  # in milliseconds
        return_result = ResultSetHandle(
            resultSet.columns, 
            resultSet.columnTokens, 
            metaToken["resultToken"],
            metaToken["interpreter_path"],
            metaToken["interpreter_port"],
            ipchandles,
            metaToken["client"],
            metaToken["calciteTime"],
            resultSet.metadata.time,
            totalTime,
            ''
        )
        return return_result
    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        print(error)
        print(traceback.print_exc())
        error_message = error
    except Error as error:
        print(error)
        print(traceback.print_exc())
        error_message = str(error)
    except Exception as error:
        print(error)
        print(traceback.print_exc())
        error_message = "Unexpected error on " + _run_query_get_results.__name__ + ", " + str(error)

    return_result = ResultSetHandle(None, None,
        metaToken["resultToken"],
        metaToken["interpreter_path"],
        metaToken["interpreter_port"], None,
        metaToken["client"],
        metaToken["calciteTime"],
        0, 0, error_message
    )
    return return_result


async def _private_get_result(resultToken, interpreter_path, interpreter_port):
    return _createResultSetIPCHandles(await _get_client()._get_result(resultToken, interpreter_path, interpreter_port))


#@exceptions_wrapper
async def _run_query_get_token(sql, tables):
    startTime = time.time()

    resultToken = 0
    interpreter_path = None
    interpreter_port = None
    calciteTime = 0
    error_message = ''

    try:
        client = _get_client()

        for table, gdf in tables.items():
            await _reset_table(client, table, gdf)

        resultToken, interpreter_path, interpreter_port, calciteTime = \
            await client.run_dml_query_token(sql, _to_table_group(tables))

    except (SyntaxError, RuntimeError, ValueError, ConnectionRefusedError, AttributeError) as error:
        print(str(error))
        print(traceback.print_exc())
        error_message = error
    except Error as error:
        print(str(error))
        print(traceback.print_exc())
        error_message = str(error)
    except Exception as error:
        print(str(error))
        print(traceback.print_exc())
        error_message = "Unexpected error on " + _run_query_get_token.__name__ + ", " + str(error)

    if error_message is not '':
        print(error_message)

    metaToken = {
        "client" : client,
        "resultToken" : resultToken,
        "interpreter_path" : interpreter_path,
        "interpreter_port" : interpreter_port,
        "startTime" : startTime,
        "calciteTime" : calciteTime,
        "error_message": error_message
    }
    return metaToken
