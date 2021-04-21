SELECT DISTINCT
---------------

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Returns unique rows from a table. 

Examples
""""""""

:code:`SELECT DISTINCT *`
~~~~~~~~~~~~~~~~~~~~~~~~~

Selects all the unique rows from the table. Uniqueness is assessed across all the columns.

.. code-block:: sql

    SELECT DISTINCT *
    FROM <table_name>

:code:`SELECT DISTINCT expr`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Select all the non-duplicate rows as defined by a single column or multiple columns.

.. code-block:: sql

    SELECT DISTINCT <col_1>
        , <col_2>
    FROM <table_name>

The :code:`SELECT DISTINCT expr` suports unary and binary functions such as :ref:`arithmetic<sql_arithmetic>`,
:ref:`string<sql_strings>`, :ref:`timestamp<sql_dates>`, :ref:`windowing<sql_windowing>`, and :ref:`conditional<sql_functions>`.