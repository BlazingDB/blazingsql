Interops
========

Interops, short for interpreted operations is a powerful row based data transformation engine. Whereas most data transformation operations in BlazingSQL
leverage cudf, most row based operations use interops. Row based operations in this context refers to any transformation that is applied in the same manner to all rows 
independant of any other rows, for example simple arithmetic, casting and more.