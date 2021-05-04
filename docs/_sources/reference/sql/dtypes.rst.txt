.. _sql_dtypes:

Data types
==========

Blazing SQL engine supports most of the same data types that `RAPIDS cudf supports <https://docs.rapids.ai/api/cudf/stable/basics.html#supported-dtypes>`_ supports.

The table below shows all the data types supported with their mapping to cuDF. 


.. list-table:: Data types
    :widths: 15 25 25 25 25
    :header-rows: 1

    * - Data Kind
      - Scalar 
      - cuDF Alias
      - BlazingSQL Support 
      - SQL Equivalent
    * - Integer
      - np.int8_, np.int16_, np.int32_, np.int64_, np.uint8_, np.uint16_, np.uint32_, np.uint64_ 
      - ``'int8'``, ``'int16'``, ``'int32'``, ``'int64'``, ``'uint8'``, ``'uint16'``, ``'uint32'``, ``'uint64'``
      - ``'int8'``, ``'int16'``, ``'int32'``, ``'int64'``, ``'uint8'``, ``'uint16'``, ``'uint32'``, ``'uint64'``
      - ``TINYINT``, ``SMALLINT``, ``INT``, ``BIGINT``
    * - Float
      - np.float32_, np.float64_ 
      - ``'float32'``, ``'float64'``
      - ``'float32'``, ``'float64'``
      - ``DECIMAL``, ``FLOAT``, ``DOUBLE``
    * - Strings
      - str_ 
      - ``'string'``, ``'object'``
      - ``'string'``, ``'object'``
      - ``VARCHAR``
    * - Datetime
      - np.datetime64_ 
      - ``'datetime64[s]'``, ``'datetime64[ms]'``, ``'datetime64[us]'``, ``'datetime64[ns]'``
      - ``'datetime64[s]'``, ``'datetime64[ms]'``, ``'datetime64[us]'``, ``'datetime64[ns]'``
      - ``TIMESTAMP``
    * - Timedelta (duration type)
      - np.timedelta64_ 
      - ``'timedelta64[s]'``, ``'timedelta64[ms]'``, ``'timedelta64[us]'``, ``'timedelta64[ns]'``
      - Currently not supported
      -
    * - Categorical
      - 
      - ``'category'``
      - Convert to ``'string'`` before creating table
      - ``VARCHAR``
    * - Boolean
      - np.bool_
      - ``'bool'``
      - ``'bool'``
      - ``BOOLEAN``

**Note: All dtypes above are Nullable**

.. _np.int8: 
.. _np.int16: 
.. _np.int32:
.. _np.int64:
.. _np.uint8:
.. _np.uint16:
.. _np.uint32:
.. _np.uint64:
.. _np.float32:
.. _np.float64:
.. _np.bool: https://numpy.org/doc/stable/user/basics.types.html
.. _np.datetime64: https://numpy.org/doc/stable/reference/arrays.datetime.html#basic-datetimes
.. _np.timedelta64: https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-and-timedelta-arithmetic
.. _str: https://docs.python.org/3/library/stdtypes.html#str