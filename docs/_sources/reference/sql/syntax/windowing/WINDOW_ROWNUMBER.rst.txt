ROW_NUMBER
~~~~~~~~~~

Obtain a row number per partition. This function does not
require :code:`ORDER BY` clause but the results are then 
non-deterministic.

Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , ROW_NUMBER() OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
        )
    FROM <table_name>
