.. _sql_casting:

Converting data types
=====================

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
      - ``FLOAT`` OR ``REAL`` [#f1]_
      - ``DOUBLE``
      - ``INTERVAL``
      - ``DATE``
      - ``TIME``
      - ``TIMESTAMP``
      - ``CHAR`` OR ``VARCHAR``
      - ``BINARY`` OR ``VARBINARY``
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
      - x
      - x
      - i
      - x
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
      - e
      - x
      - x
      - e
      - i
      - x
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
      - e
      - x
      - x
      - e
      - i
      - x
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
      - e
      - x
      - x
      - e
      - i
      - x
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
      - e
      - x
      - x
      - e
      - i
      - x
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
      - e
      - x
      - x
      - e
      - i
      - x
    * - ``FLOAT`` or ``REAL``  [#f1]_
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
      - x
      - x
      - e
      - i
      - x
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
      - x
      - x
      - e
      - i
      - x
    * - ``INTERVAL`` [#f1]_
      - x
      - x
      - e
      - e
      - e
      - e
      - e
      - x
      - x
      - i
      - x
      - x
      - x
      - e
      - x
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
      - x
      - i
      - x
      - i
      - i
      - x
    * - ``TIME`` [#f1]_
      - x
      - x
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
      - e
      - i
      - x
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
      - x
      - i
      - e
      - i
      - i
      - x
    * - ``CHAR`` [#f1]_ or ``VARCHAR``
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
      - i
      - i
      - i
    * - ``BINARY`` [#f1]_ or ``VARBINARY`` [#f1]_
      - 
      - 
      - 
      - 
      - 
      - 
      - 
      - 
      - 
      - 
      - 
      - 
      - 
      - 
      - 

**i:** implicit cast 
**e:** explicit cast 
**x:** not allowed

.. rubric:: Footnotes

.. [#f1] Currently not supported

.. toctree::
    :maxdepth: 2