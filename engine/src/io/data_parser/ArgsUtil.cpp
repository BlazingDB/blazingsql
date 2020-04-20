#include "ArgsUtil.h"

#include <blazingdb/io/FileSystem/Uri.h>
#include <blazingdb/io/Util/StringUtil.h>

#include "../data_provider/UriDataProvider.h"

namespace ral {
namespace io {

DataType inferDataType(std::string file_format_hint) {
	if(file_format_hint == "parquet")
		return DataType::PARQUET;
	if(file_format_hint == "json")
		return DataType::JSON;
	if(file_format_hint == "orc")
		return DataType::ORC;
	if(file_format_hint == "csv")
		return DataType::CSV;
	if(file_format_hint == "psv")
		return DataType::CSV;
	if(file_format_hint == "tbl")
		return DataType::CSV;
	if(file_format_hint == "txt")
		return DataType::CSV;
	// NOTE if you need more options the user can pass file_format in the create table

	return DataType::UNDEFINED;
}

DataType inferFileType(std::vector<std::string> files, DataType data_type_hint) {
	if(data_type_hint == DataType::PARQUET || data_type_hint == DataType::CSV || data_type_hint == DataType::JSON ||
		data_type_hint == DataType::ORC) {
		return data_type_hint;
	}

	std::vector<Uri> uris;
	std::transform(
		files.begin(), files.end(), std::back_inserter(uris), [](std::string uri) -> Uri { return Uri(uri); });
	ral::io::uri_data_provider udp(uris);
	bool open_file = false;
	const ral::io::data_handle dh = udp.get_next(open_file);
	std::string ext = dh.uri.getPath().getFileExtension();
	std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

	return inferDataType(ext);
}

bool in(std::string key, std::map<std::string, std::string> args) { return !(args.find(key) == args.end()); }

bool to_bool(std::string value) {
	if(value == "True")
		return true;
	if(value == "False")
		return false;
	return false;
}

char ord(std::string value) { return (char) value[0]; }

int to_int(std::string value) { return std::atoi(value.c_str()); }

std::vector<std::string> to_vector_string(std::string value) {
	std::string vec = StringUtil::replace(value, "'", "");
	vec = StringUtil::replace(vec, "[", "");
	vec = StringUtil::replace(vec, "]", "");
	vec = StringUtil::replace(vec, " ", "");
	std::vector<std::string> ret = StringUtil::split(vec, ",");
	return ret;
}

std::vector<int> to_vector_int(std::string value) {
	std::vector<std::string> input = to_vector_string(value);
	std::vector<int> ret;
	std::transform(input.begin(), input.end(), std::back_inserter(ret), [](std::string v) -> int { return to_int(v); });
	return ret;
}

void getReaderArgJson(std::map<std::string, std::string> args, ReaderArgs & readerArg) {
	std::vector<std::string> dtypes;
	readerArg.jsonReaderArg.lines = true;
	if(in("dtype", args)) {
		readerArg.jsonReaderArg.dtype = to_vector_string(args["dtype"]);
	}
	if(in("compression", args)) {
		readerArg.jsonReaderArg.compression = static_cast<cudf_io::compression_type>(to_int(args["compression"]));
	}
	if(in("lines", args)) {
		readerArg.jsonReaderArg.lines = to_bool(args["lines"]);
	}
	if(in("byte_range_offset", args)) {
		readerArg.jsonReaderArg.byte_range_offset = (size_t) to_int(args["byte_range_offset"]);
	}
	if(in("byte_range_size", args)) {
		readerArg.jsonReaderArg.byte_range_size = (size_t) to_int(args["byte_range_size"]);
	}
}

void getReaderArgOrc(std::map<std::string, std::string> args, ReaderArgs & readerArg) {
	if(in("stripe", args)) {
		readerArg.orcReaderArg.stripe = to_int(args["stripe"]);
	}
	if(in("skip_rows", args)) {
		readerArg.orcReaderArg.skip_rows = to_int(args["skip_rows"]);
	}
	if(in("num_rows", args)) {
		readerArg.orcReaderArg.num_rows = to_int(args["num_rows"]);
	}
	if(in("use_index", args)) {
		readerArg.orcReaderArg.use_index = to_int(args["use_index"]);
	} else {
		readerArg.orcReaderArg.use_index = true;
	}
	if(in("byte_range_size", args)) {
		readerArg.jsonReaderArg.byte_range_size = (size_t) to_int(args["byte_range_size"]);
	}
}

void getReaderArgCSV(std::map<std::string, std::string> args, ReaderArgs & readerArg) {
	if(in("compression", args)) {
		readerArg.csvReaderArg.compression = (cudf::experimental::io::compression_type) to_int(args["compression"]);
	}
	if(in("lineterminator", args)) {
		readerArg.csvReaderArg.lineterminator = ord(args["lineterminator"]);
	}
	if(in("delimiter", args)) {
		readerArg.csvReaderArg.delimiter = ord(args["delimiter"]);
	}
	if(in("windowslinetermination", args)) {
		readerArg.csvReaderArg.windowslinetermination = to_bool(args["windowslinetermination"]);
	}
	if(in("delim_whitespace", args)) {
		readerArg.csvReaderArg.delim_whitespace = to_bool(args["delim_whitespace"]);
	}
	if(in("skipinitialspace", args)) {
		readerArg.csvReaderArg.skipinitialspace = to_bool(args["skipinitialspace"]);
	}
	if(in("skip_blank_lines", args)) {
		readerArg.csvReaderArg.skip_blank_lines = to_bool(args["skip_blank_lines"]);
	}
	if(in("nrows", args)) {
		readerArg.csvReaderArg.nrows = (cudf::size_type) to_int(args["nrows"]);
	}
	if(in("skiprows", args)) {
		readerArg.csvReaderArg.skiprows = (cudf::size_type) to_int(args["skiprows"]);
	}
	if(in("skipfooter", args)) {
		readerArg.csvReaderArg.skipfooter = (cudf::size_type) to_int(args["skipfooter"]);
	}
	if(in("header", args) && args["header"] != "None" ) {
		readerArg.csvReaderArg.header = (cudf::size_type) to_int(args["header"]);
	} else if(args["header"] == "None"){
		readerArg.csvReaderArg.header = -1;
	}
	if(in("names", args)) {
		readerArg.csvReaderArg.names = to_vector_string(args["names"]);
	}
	if(in("dtype", args)) {
		readerArg.csvReaderArg.dtype = to_vector_string(args["dtype"]);
	}
	if(in("use_cols_indexes", args)) {
		readerArg.csvReaderArg.use_cols_indexes = to_vector_int(args["use_cols_indexes"]);
	}
	if(in("use_cols_names", args)) {
		readerArg.csvReaderArg.use_cols_names = to_vector_string(args["use_cols_names"]);
	}
	if(in("true_values", args)) {
		readerArg.csvReaderArg.true_values = to_vector_string(args["true_values"]);
	}
	if(in("false_values", args)) {
		readerArg.csvReaderArg.false_values = to_vector_string(args["false_values"]);
	}
	if(in("na_values", args)) {
		readerArg.csvReaderArg.na_values = to_vector_string(args["na_values"]);
	}
	if(in("keep_default_na", args)) {
		readerArg.csvReaderArg.keep_default_na = to_bool(args["keep_default_na"]);
	}
	if(in("na_filter", args)) {
		readerArg.csvReaderArg.na_filter = to_bool(args["na_filter"]);
	}
	if(in("prefix", args)) {
		readerArg.csvReaderArg.prefix = args["prefix"];
	}
	if(in("mangle_dupe_cols", args)) {
		readerArg.csvReaderArg.mangle_dupe_cols = to_bool(args["mangle_dupe_cols"]);
	}
	if(in("dayfirst", args)) {
		readerArg.csvReaderArg.dayfirst = to_bool(args["dayfirst"]);
	}
	if(in("thousands", args)) {
		readerArg.csvReaderArg.thousands = ord(args["thousands"]);
	}
	if(in("decimal", args)) {
		readerArg.csvReaderArg.decimal = ord(args["decimal"]);
	}
	if(in("comment", args)) {
		readerArg.csvReaderArg.comment = ord(args["comment"]);
	}
	if(in("quotechar", args)) {
		readerArg.csvReaderArg.quotechar = ord(args["quotechar"]);
	}
	// if (in("quoting", args)) {
	//    readerArg.csvReaderArg.quoting = args["quoting"]
	if(in("doublequote", args)) {
		readerArg.csvReaderArg.doublequote = to_bool(args["doublequote"]);
	}
	if(in("byte_range_offset", args)) {
		readerArg.csvReaderArg.byte_range_offset = (size_t) to_int(args["byte_range_offset"]);
	}
	if(in("byte_range_size", args)) {
		readerArg.csvReaderArg.byte_range_size = (size_t) to_int(args["byte_range_size"]);
	}
	if(in("out_time_unit", args)) {
		// TODO
		// readerArg.csvReaderArg.out_time_unit = args["out_time_unit"]
	}
}

ReaderArgs getReaderArgs(DataType fileType, std::map<std::string, std::string> args) {
	ReaderArgs readerArg;

	if(fileType == DataType::JSON) {
		getReaderArgJson(args, readerArg);
	} else if(fileType == DataType::CSV) {
		getReaderArgCSV(args, readerArg);
	} else if(fileType == DataType::ORC) {
		getReaderArgOrc(args, readerArg);
	}

	return readerArg;
}

std::map<std::string, std::string> to_map(std::vector<std::string> arg_keys, std::vector<std::string> arg_values) {
	std::map<std::string, std::string> ret;
	for(int i = 0; i < arg_keys.size(); ++i) {
		ret[arg_keys[i]] = arg_values[i];
	}
	return ret;
}

std::string getDataTypeName(DataType dataType) {
	switch(dataType) {
	case DataType::PARQUET: return "parquet"; break;
	case DataType::ORC: return "orc"; break;
	case DataType::CSV: return "csv"; break;
	case DataType::JSON: return "json"; break;
	case DataType::CUDF: return "cudf"; break;
	case DataType::DASK_CUDF: return "dask_cudf"; break;
	}

	return "undefined";
}

} /* namespace io */
} /* namespace ral */
