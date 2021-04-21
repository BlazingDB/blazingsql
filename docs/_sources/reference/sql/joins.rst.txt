.. _sql_windowing:

Joining data
============

{% for member in sql.joins %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/joins/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2