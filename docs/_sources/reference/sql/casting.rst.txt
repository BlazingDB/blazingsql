.. _sql_casting:

Converting data types
=====================

{% for member in sql.casting %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/casting/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2