from blazingsql import BlazingContext

import pandas as pd

bc = BlazingContext()

location='/home/kharoly/BlazingSQL/partitions/region/'

# This is the same table as the 'asia_transactions' example above, but without using the hive cursor
bc.create_table('nation', location, 
                file_format='parquet',  
                partitions = {'n_regionkey':[1, 2, 3, 4]}, 
                partitions_schema = [('n_regionkey','int')])

print("table nation created")

query='select * from nation'

result=bc.sql(query)

print(result.dtypes)

print(result)




return partiions partition_schema