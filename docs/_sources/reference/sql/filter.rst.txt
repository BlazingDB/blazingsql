.. _sql_filter:

Filtering data
==============

SQL syntax allows for filtering data using :code:`WHERE` statement.

{% for member in sql.filter %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/filter/{{ member }}.rst
{% endfor %}
 
.. toctree::
    :maxdepth: 2