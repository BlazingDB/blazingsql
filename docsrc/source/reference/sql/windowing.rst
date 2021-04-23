.. _sql_windowing:

Window functions
================

{% for member in sql.windowing %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/windowing/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2