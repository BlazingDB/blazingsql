.. _blazing_context:


================
BlazingContext()
================
.. automodule:: blazingsql
   :no-members:
   :no-inherited-members:

.. currentmodule:: blazingsql

BlazingContext is the Python API of BlazingSQL. Along with initialization arguments, the BlazingContext class has a number of methods which assist not only in creating and querying tables, but also in connecting remote data sources and understanding your ETL.

You must establish a BlazingContext to connect to a BlazingSQL instance to create tables, run queries, and basically do anything with BlazingSQL. First, import the required libraries. 

.. code-block:: python

    from blazingsql import BlazingContext

    bc = BlazingContext()

Constructor
~~~~~~~~~~~
.. autosummary::
   :toctree: api/

   BlazingContext

Managing tables
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: api/

   BlazingContext.create_table
   BlazingContext.drop_table
   BlazingContext.describe_table
   BlazingContext.list_tables
   BlazingContext.partition
   BlazingContext.add_remove_table

Querying tables
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: api/

   BlazingContext.sql
   BlazingContext.explain
   BlazingContext.fetch
   BlazingContext.status

Ancillary methods
~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: api/

   BlazingContext.do_progress_bar
   BlazingContext.get_free_memory
   BlazingContext.get_max_memory_used
   BlazingContext.reset_max_memory_used