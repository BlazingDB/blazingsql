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



.. toctree::
    :maxdepth: 2