#ifndef _BZ_RAL_ARGS_UTIL_H_
#define _BZ_RAL_ARGS_UTIL_H_

#include "io/io.h"
#include <cudf/io/json.hpp>
#include <cudf/io/orc.hpp>
#include <cudf/io/csv.hpp>

namespace ral {
namespace io {

struct ReaderArgs {
	cudf::io::orc_reader_options orcReaderArg = cudf::io::orc_reader_options::builder(cudf::io::source_info(""));
	cudf::io::json_reader_options jsonReaderArg = cudf::io::json_reader_options::builder(cudf::io::source_info(""));
	cudf::io::csv_reader_options csvReaderArg = cudf::io::csv_reader_options::builder(cudf::io::source_info(""));
};

DataType inferDataType(std::string file_format_hint);

DataType inferFileType(std::vector<std::string> files, DataType data_type_hint);

bool in(std::string key, std::map<std::string, std::string> args);

bool to_bool(std::string value);

char ord(std::string value);

int to_int(std::string value);

std::vector<std::string> to_vector_string(std::string value);

void getReaderArgJson(std::map<std::string, std::string> args, ReaderArgs & readerArg);

void getReaderArgOrc(std::map<std::string, std::string> args, ReaderArgs & readerArg);

void getReaderArgCSV(std::map<std::string, std::string> args, ReaderArgs & readerArg);

ReaderArgs getReaderArgs(DataType fileType, std::map<std::string, std::string> args);

std::map<std::string, std::string> to_map(std::vector<std::string> arg_keys, std::vector<std::string> arg_values);

std::string getDataTypeName(DataType dataType);

} /* namespace io */
} /* namespace ral */

#endif /* _BZ_RAL_ARGS_UTIL_H_ */
