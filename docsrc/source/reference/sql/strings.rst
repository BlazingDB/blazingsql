.. _sql_strings:

String functions
----------------

String functions work on values of a text column.

{% for member in sql.strings %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/strings/{{ member }}.rst
{% endfor %}
 
.. toctree::
    :maxdepth: 2