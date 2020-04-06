
from pyhive import hive
from TCLIService.ttypes import TOperationState
import numpy as np
import cudf
from itertools import repeat
import pandas as pd


def convertHiveTypeToCudfType(hiveType):
    if(hiveType == 'int' or hiveType == 'integer'):
        return 3 # INT32
    elif(hiveType == 'string' or hiveType.startswith('varchar') or hiveType.startswith('char') or hiveType == "binary"):
        return 14  # STRING
    elif(hiveType == 'tinyint'):
        return 1  # INT8
    elif(hiveType == 'smallint'):
        return 2  # INT16
    elif(hiveType == 'bigint'):
        return 4  # INT64
    elif(hiveType == 'float'):
        return 5  # FLOAT32
    elif(hiveType == 'double' or hiveType == 'double precision' or hiveType.startswith('decimal')):
        return 6  # FLOAT64
    elif(hiveType == 'decimal' or hiveType == 'numeric'):
        return None
    elif(hiveType == 'timestamp'):
        return 10  # TIMESTAMP_MILLISECONDS
    elif(hiveType == 'date'):
        return 8  # TIMESTAMP_DAYS
    elif(hiveType == 'boolean'):
        return 7  # BOOL8


cudfTypeToCsvType = {
    1: "int8",
    2: "int16",
    3: "int32",
    4: "int64",
    5: "float32",
    6: "float64",
    7: "boolean",
    8: "date32",
    9: "timestamp[s]",
    10: "timestamp[ms]",
    11: "timestamp[us]",
    12: "timestamp[ns]",
    14: "str",
}  


def getPartitions(tableName, schema, cursor):
    query = "show partitions " + tableName
    result = runHiveQuery(cursor, query)
    partitions = {}
    for partition in result[0]:
        columnPartitions = []
        for columnPartition in partition:
            for columnData in columnPartition.split("/"):
                columnName = columnData.split("=")[0]
                for column in schema['columns']:
                    if column[0] == columnName:
                        columnValue = columnData.split("=")[1]
                        columnPartitions.append((columnName, columnValue))
        partitions[partition[0]] = columnPartitions
    return partitions


def get_hive_table(cursor, tableName, hive_database_name):
    query = 'use ' + hive_database_name
    runHiveDDL(cursor, query)
    query = 'describe formatted ' + tableName
    result, description = runHiveQuery(cursor, query)

    schema = {}
    schema['columns'] = []
    schema['column_types'] = []
    i = 0
    parsingColumns = False
    parsingPartitionColumns = False
    startParsingPartitionRows = 0
    schema['delimiter'] = chr(1)
    for triple in result:
        if triple[0] is not None:
            if(i == 2):
                parsingColumns = True
            if(parsingColumns):
                if triple[0] == '':
                    parsingColumns = False
                else:
                    schema['columns'].append(
                        (triple[0], convertHiveTypeToCudfType(triple[1]), False))                    
            elif isinstance(triple[0], str) and triple[0].startswith('Location:'):
                if triple[1].startswith("file:"):
                    schema['location'] = triple[1].replace("file:", "")
                else:
                    schema['location'] = triple[1]
            elif isinstance(triple[0], str) and triple[0].startswith('InputFormat:'):
                if "TextInputFormat" in triple[1]:
                  #                  schema['fileType'] = self.CSV_FILE_TYPE
                    schema['fileType'] = 'csv'
                if "ParquetInputFormat" in triple[1]:
                  #                  schema['fileType'] = self.PARQUET_FILE_TYPE
                    schema['fileType'] = 'parquet'
                if "OrcInputFormat" in triple[1]:
                  #                  schema['fileType'] = self.ORC_FILE_TYPE
                    schema['fileType'] = 'orc'
                if "JsonInputFormat" in triple[1]:
                  #                  schema['fileType'] = self.JSON_FILE_TYPE
                    schema['fileType'] = 'json'
            elif isinstance(triple[1], str) and triple[1].startswith("field.delim"):
                schema['delimiter'] = triple[2][0]
            elif triple[0] == "# Partition Information":
                parsingPartitionColumns = True
                startParsingPartitionRows = i + 2
            elif parsingPartitionColumns and i > startParsingPartitionRows:
                if triple[0] == "# Detailed Table Information":
                    parsingPartitionColumns = False
                elif triple[0] != "":
                    schema['columns'].append(
                        (triple[0], convertHiveTypeToCudfType(triple[1]), True))                    
        i = i + 1
    
    hasPartitions = False
    for column in schema['columns']:
        if column[2]:
            hasPartitions = True
    file_list = []
    if hasPartitions:
        schema['partitions'] = getPartitions(tableName, schema, cursor)
    else:
        schema['partitions'] = {}
        file_list.append(schema['location'] + "/*")

    extra_kwargs = {}
    extra_kwargs['delimiter'] = schema['delimiter']
    if schema['fileType'] == 'csv':
        extra_kwargs['names'] = [col_name for col_name, dtype, is_virtual_col  in schema['columns'] if not is_virtual_col ]
        extra_kwargs['dtype'] = [cudfTypeToCsvType[dtype] for col_name, dtype, is_virtual_col in schema['columns'] if not is_virtual_col]
    
    #schema['column_names'] and schema['column_types'] will be given to the BlazingTable object. We want to use these instead of that gets returned from _parseSchema because the files and the hive cursor may not agree and we want to go off of the hive cursor
    schema['column_names'] = [col_name.encode() for col_name, dtype, is_virtual_col  in schema['columns'] ]
    schema['column_types'] = [dtype for col_name, dtype, is_virtual_col  in schema['columns'] ]
    
    extra_kwargs['file_format'] = schema['fileType']
    extra_columns = []
    in_file = []
    for column in schema['columns']:
        in_file.append(column[2] == False)
        if(column[2]):
            extra_columns.append((column[0], column[1]))
    for partitionName in schema['partitions']:
        file_list.append(schema['location'] + "/" + partitionName + "/*")
    
    return file_list, schema['fileType'], extra_kwargs, extra_columns, in_file, schema


def runHiveDDL(cursor, query):
    cursor.execute(query, async_=True)
    status = cursor.poll().operationState
    while status in (
            TOperationState.INITIALIZED_STATE,
            TOperationState.RUNNING_STATE):
        status = cursor.poll().operationState


def runHiveQuery(cursor, query):
    cursor.execute(query, async_=True)
    status = cursor.poll().operationState
    while status in (
            TOperationState.INITIALIZED_STATE,
            TOperationState.RUNNING_STATE):
        status = cursor.poll().operationState
    return cursor.fetchall(), cursor.description


def convertHiveToCudf(cursor, query):
    df = cudf.DataFrame()
    result, description = runHiveQuery(cursor, query)
    arrays = [[] for i in repeat(None, len(result[0]))]
    for row in result:
        i = 0
        for val in row:
            arrays[i].append(val)
            i = i + 1
    i = 0
    while i < len(result[0]):
        column = cudf.Series(arrays[i])
        df[description[i][0].split('.')[1]] = column
        i = i + 1
    return df
