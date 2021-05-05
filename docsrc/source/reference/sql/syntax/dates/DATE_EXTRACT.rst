EXTRACT
^^^^^^^

**Supported datatypes:** :ref:`TIMESTAMP<sql_dtypes>`

Extract the value from a :code:`TIMESTAMP` column specified by the qualifier.

This functionality is compatible with the other :code:`TIMESTAMP` functions. 

.. list-table:: Functionality mapping
    :widths: 20 40 40
    :header-rows: 1

    * - Datetime part
      - SQL function
      - :code:`EXTRACT` qualifier
    * - Year value
      - :ref:`sql_date_year`
      - :code:`YEAR`
    * - Month value
      - :ref:`sql_date_month`
      - :code:`MONTH`
    * - Day of a month
      - :ref:`sql_date_dayofmonth`
      - :code:`DAY`
    * - Day of the week
      - :ref:`sql_date_dayofweek`
      - :code:`DAYOFWEEK`
    * - Hour of the day
      - :ref:`sql_date_hour`
      - :code:`HOUR`
    * - Minute of the hour
      - :ref:`sql_date_minute`
      - :code:`MINUTE`
    * - Second of the minute
      - :ref:`sql_date_second`
      - :code:`SECOND`
    

Examples
""""""""

Extract year value from :code:`TIMESTAMP` column.

.. code-block:: sql

    SELECT EXTRACT(YEAR FROM <col_1>)
    FROM <table_name>

Extract second value from :code:`TIMESTAMP` column.

.. code-block:: sql

    SELECT EXTRACT(SECOND FROM <col_1>)
    FROM <table_name>