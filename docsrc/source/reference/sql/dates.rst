.. _sql_dates:

Date and timestamp functions
----------------------------

Operations on :code:`TIMESTAMP` columns.

{% for member in sql.dates %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/dates/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2