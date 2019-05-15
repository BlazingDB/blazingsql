from .api import run_query_get_token
from .api import run_query_filesystem_get_token
from .api import run_query_get_results
from .api import run_query_pandas
from .api import register_file_system
from .api import deregister_file_system
from .api  import register_table_schema
from .api  import FileSystemType, DriverType, EncryptionType
from .api import SchemaFrom
from .api import run_query_filesystem
from .api import create_table
#TODO this api is experimental
#from .api import run_query_arrow

from .api import ResultSetHandle
from .api import _get_client

from .connector import PyConnector
from .connector.utils import is_caller_async
from .api import run_query as run_query_sync
from .aio import run_query as run_query_async

def run_query(sql, tables):
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
    if PyConnector.asyncio_enabled and is_caller_async():
        return run_query_async(sql, tables)
    return run_query_sync(sql, tables)
