Query Plain Text Files
======================

You can use BlazingSQL to SQL query plain text files (flat files), such as:

* CSV files (comma-separated values)
* TSV files (tab-separated values)
* PSV files (pipe-separated values)

.. code-block:: python
    bc.create_table('table_name', '...file_path/table.csv')


Parameters
~~~~~~~~~~

* **table_name** - string. **Required.** Name of the table you are creating.
* **file_path** - string, **Required.** Location of the file you are creating a table off of.
* **names** - list of strings, **Optional.** Names of all the columns in the file. **Default:** ``None``
* **dtype** - list of strings, **Optional.** Data types of all the columns in the file. **Default:** ``None``
* **delimiter** - character, **Optional.** Delimiter in the file. **Default:** ``','``,
* **skiprows** - integer, **Optional.** Number of rows to be skipped from the start of file. **Default:** ``0``
* **skipfooter** - integer, **Optional.** Nnumber of rows to be skipped at the bottom of file. **Default:** ``0``
* **lineterminator** - character, **Optional.** End of line indicator. **Default:** ``\n``
* **header** - integer, **Optional.** Row number to use as the column names. Default behavior is to infer the column names: if no names are passed, header = 0. **Default:** ``0``
* **nrows** - integer, **Optional.** Maximum number of rows to read. **Default:** ``None`` (all rows)
* **skip_blank_lines** - boolean, **Optional.** If *True*, skip over blank lines rather than interpreting as *null* values. **Default:** ``True``
* **decimal** - character, **Optional.** Character to use as a decimal point (e.g. use ‘,’ for European data). **Default:** ``‘.’``
* **true_values** - list of strings, **Optional.** Values to consider as *True*. **Default:** ``None``
* **false_values** - list of strings, **Optional.** Values to consider as *False*. **Default:** ``None``
* **na_values** - list of strings, **Optional.** Values to consider as *invalid*. **Default:** ``None``
* **keep_default_na** - boolean, **Optional.** Speficies whether or not to include the default *NaN* values when parsing the data. **Default:** ``True``
* **na_filter** - boolean, **Optional.** Detect missing values (empty strings and the values in *na_values*). Passing *False* can improve performance. **Default:** ``True``
* **quotechar** - character, **Optional.**Start and end of quote item indicator. **Default:** ``‘”’``
* **quoting** - integer, **Optional.** Controls quoting behavior. Use one of QUOTE_MINIMAL (0), QUOTE_ALL (1), QUOTE_NONNUMERIC (2) or QUOTE_NONE (3). Quoting is enabled with all values except 3. **Default:** ``0``
* **doublequote** - boolean, **Optional.** When quoting is enabled (not QUOTE_NONE), indicates whether to interpret two consecutive quotechar inside fields as single quotechar. **Default:** ``True``
* **comment** - character, **Optional.** Character used as a comments indicator. If found at the beginning of a line, the line will be ignored altogether. **Default:** ``None``
* **delim_whitespace** - boolean, **Optional.** Determines whether to use whitespace as delimiter. If this option is set to *True*,  *delimiter* will be ignored. **Default:** ``False``
* **file_format** - string, **Optional.** Describes the file format (``"csv"``, ``"orc"``, ``"parquet"``). This field must only be set if the files do not have an extension. **Default:** By default the engine will infer the file format by the extension
* **compression** - string, **Optional.** This field only needs to be set if the file is compressed. Note that you can only have one test file per compressed archive, otherwise only the first file will be read. **Valid options**: ``['infer', 'snappy', 'gzip', 'bz2', 'brotli', 'zip', 'xz']``. **Default:** By default the engine will assume the file(s) are not compressed.
* **max_bytes_chunk_read** - integer, **Optional.** The maximum number of bytes to read in a row. **Default:** By default it will always try to read the whole file.


Example
~~~~~~~

.. code-block:: python

    # using a PSV extension file
    col_names = ['col_1', 'col_2', 'col_3']

    # Create Table
    bc.create_table('table_name', '...file_path/table.psv', delimiter='|', 
    names=col_names)

.. code-block:: python

    # Using your well knowed data types
    col_dtypes = ['int32','float32','str', 'float64'] 

    # Create Table
    bc.create_table('table_name', '...file_path/table.csv', dtype=col_dtypes)

.. code-block:: python

    # Get the first 5 rows
    bc.create_table('table_name', '...file_path/table.csv', nrows=5)

You can create a table off of multiple text files if they share the same schema. 
For now, you need a ``List`` of all the text files.

.. code-block:: python

    # Multiple listed files
    bc.create_table(
        'table_name', 
        [
            '...file_path/table0.csv', 
            '...file_path/table1.csv'
        ]
    )

File paths provided must be absolute file paths, but they can contain 
a wildcard in the filename. If a wildcard is used in the file name, 
then the table will contain the data from all the files that match that 
file name. File paths can also point to a directory, in which the table 
will contain the data from all the files in that directory. In any case 
that a table is created from multiple files, all files must have 
the same data schema. 

.. code-block:: python

    # multiple files via wildcard
    bc.create_table('table_name', '...file_path/table_*.csv') 

.. code-block:: python

    # All files in a directory
    bc.create_table('table_name', '/directory_path/')
