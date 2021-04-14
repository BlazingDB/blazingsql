Query Apache Parquet Files
==========================

You can use BlazingSQL to SQL query Apache Parquet files.

`Apache Parquet <https://parquet.apache.org/documentation/latest/>`_ has the following characteristics:

* Self-describing
* Columnar format
* Compressed

BlazingSQL relies on :io:`io` when reading Parquet files, which means **ALL** of the decompression happens on GPUs, dramatically reducing GPU over PCIe bandwidth concerns.

Querying Apache Parquet is straightforward.

.. code-block:: python
    
    bc.create_table('table_name', '...file_path/table.parquet')

Parameters
~~~~~~~~~~

* **table_name** - string. **Required.** Name of the table you are creating. 
* **file_path** - string. **Required.** Location of the file you are creating a table off of.
* **file_format** - string. **Optional.** A string describing the file format (``"csv"``, ``"orc"``, ``"parquet"``). This field must only be set if the files do not have an extension. **Default:** the engine will infer the file format by the extension.

You can also query multiple Parquet files. For now, you need a **List** of all the Parquet files.

.. code-block:: python

    bc.create_table(
        'table_name', 
        [
            '...file_path/table_0.parquet', 
            '...file_path/table_1.parquet'
        ]
    )