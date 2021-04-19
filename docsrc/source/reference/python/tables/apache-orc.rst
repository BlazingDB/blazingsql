Query Apache ORC Files
======================

You can use BlazingSQL to SQL query ORC files.

BlazingSQL relies on :io:`io` when reading files, which means we 
can leverage numerous features, such as inferring column names through a 
header row and data types through a sampling method.

Creating a Table off of a ORC has never been easier.

.. code-block:: python

    bc.create_table('table_name', '...file_path/table.orc')

That's all there is to it.

Parameters
~~~~~~~~~~

* **table_name** - string. **Required.** Name of the table you are creating. 
* **file_path** - string. **Required.** Location of the file you are creating a table off of.
* **file_format** - string. **Optional.** A string describing the file format (``"csv"``, ``"orc"``, ``"parquet"``). This field must only be set if the files do not have an extension. **Default:** the engine will infer the file format by the extension.
* **skiprows** - integer. **Optional.** The number of rows to skip from the start of the file. **Default:** 0.
* **stripes** - integer. **Optional.**  Only the stripe with the specified index will be read. **Default:** 1.

Example
~~~~~~~

You can create a table off of multiple ORC files if they share the same schema. 
For now, you need a **List** of all the ORC files.

.. code-block:: python

    # Multiple listed files with same schema
    bc.create_table(
        'table_name', 
        [
            '<file_path>/table0.orc', 
            '<file_path>/table1.orc'
        ]
    )

File paths provided must be absolute file paths, but they can contain a 
wildcard in the filename. If a wildcard is used in the file name, then the 
table will contain the data from all the files that match that file name. 
File paths can also point to a directory, in which the table will contain the 
data from all the files in that directory. In any case that a table is created 
from multiple files, all files must have the same data schema.

.. code-block:: python

    # multiple files via wildcard
    bc.create_table('table_name', '<file_path>/table_*.orc')

.. code-block:: python

    # All files in a directory
    bc.create_table('table_name', '<directory_path>/')