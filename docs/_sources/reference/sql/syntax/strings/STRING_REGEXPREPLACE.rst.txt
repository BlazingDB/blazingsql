REGEXP_REPLACE
^^^^^^^^^^^^^^

**Supported datatypes:** :ref:`VARCHAR<sql_dtypes>`

Obtain a string with all instances of a Regex pattern are replaced with a specified
replacement.

.. note:: The regular expressions pattern rules apply when specifying the pattern.
    For example, to replace a backslash character :code:`\ ` it needs to be *escaped*
    resulting in the :code:`\\` pattern.

.. seealso:: :ref:`sql_string_replace`

Example
"""""""

.. code-block:: sql

    SELECT REGEXP_REPLACE(<col_1>, <pattern>, <replacement>)
    FROM <table_name>
