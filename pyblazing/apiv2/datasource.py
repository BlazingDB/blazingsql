# from cio import DataType_PARQUET
# 
# print(holas)
# 
# from cio import DataType
# 
# 
# def file_format(data_type):
#     if data_type in [DataType.CUDF, DataType.DASK_CUDF]:
#         raise "Could not determine the file format for " + data_type.name
# 
#     return data_type.name.lower()
# 
# 
# def to_data_type(file_format):
#     for dt in DataType:
#         if file_format == file_format(dt):
#             return dt
# 
# 
# def is_valid_file_format(self, file_format_hint):
#     if file_format_hint is not None:
#         available_file_formats = [
#             file_format(DataType.UNDEFINED),  # in case of dir or wildcard
#             file_format(DataType.PARQUET),
#             file_format(DataType.ORC),
#             file_format(DataType.CSV),
#             file_format(DataType.JSON)
#         ]
#         if not any([type == file_format_hint for type in available_file_formats]):
#             print("WARNING: file_format does not match any of the supported types: 'parquet','orc','csv','json'")
#             return False
#     return True
