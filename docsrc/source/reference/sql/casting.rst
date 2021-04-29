.. _sql_casting:

Converting data types
=====================

The below table shows all the allowed type conversions supported by BlazingSQL.

Types of conversion:

* **i -** implicit cast 
* **e -** explicit cast 
* **x -** not allowed


.. list-table:: Allowed data conversions
    :widths: auto
    :header-rows: 1

    * - FROM - TO	
      - ``NULL``
      - ``BOOLEAN``
      - ``TINYINT``
      - ``SMALLINT``
      - ``INT``
      - ``BIGINT``
      - ``DECIMAL``
      - ``FLOAT``
      - ``DOUBLE``
      - ``DATE``
      - ``TIMESTAMP``
      - ``VARCHAR``
    * - ``NULL``
      - i	
      - i	
      - i	
      - i	
      - i	
      - i	
      - i	
      - i	
      - i	
      - i	
      - i	
      - i	
    * - ``BOOLEAN``
      - x
      - i
      - e
      - e
      - e
      - e
      - e
      - e
      - e
      - x
      - x
      - i
    * - ``TINYINT``
      - x
      - e
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - x
      - e
      - i
    * - ``SMALLINT``
      - x
      - e
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - x
      - e
      - i
    * - ``INT``
      - x
      - e
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - x
      - e
      - i
    * - ``BIGINT``
      - x
      - e
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - x
      - e
      - i
    * - ``DECIMAL``
      - x
      - e
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - x
      - e
      - i
    * - ``FLOAT``
      - x
      - e
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - x
      - e
      - i
    * - ``DOUBLE``
      - x
      - e
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - x
      - e
      - i
    * - ``DATE``
      - x
      - x
      - x
      - x
      - x
      - x
      - x
      - x
      - x
      - i
      - i
      - i
    * - ``TIMESTAMP``
      - x
      - x
      - e
      - e
      - e
      - e
      - e
      - e
      - e
      - i
      - i
      - i
    * - ``VARCHAR``
      - x
      - e
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - i
      - i


:code:`TO_DATE` method
----------------------

Casts a string to a date, allowing for formatting options.
Formatting can be specified by using:

* ``%Y`` for year
* ``%m`` for month
* ``%d`` for day
* ``%H`` for hour
* ``%M`` for minute
* ``%S`` for second

The syntax is of the form:

.. code-block:: sql
  
    SELECT TO_DATE(<col>, <format_str>) 
    FROM <table>

Examples
""""""""

.. code-block:: sql
  
    SELECT TO_DATE(<col>, '%Y-%m-%d') 
    FROM <table>

:code:`TO_TIMESTAMP` method
---------------------------

Casts a string to a timestamp, allowing for formatting options.
Formatting can be specified by using:

* ``%Y`` for year
* ``%m`` for month
* ``%d`` for day
* ``%H`` for hour
* ``%M`` for minute
* ``%S`` for second

The syntax is of the form

.. code-block:: sql
  
    SELECT TO_TIMESTAMP(<col>, <format_str>) 
    FROM <table>

Examples
""""""""

.. code-block:: sql
  
    SELECT TO_DATE(<col>, '%Y-%m-%d %H:%M:%S') 
    FROM <table>

.. toctree::
    :maxdepth: 2