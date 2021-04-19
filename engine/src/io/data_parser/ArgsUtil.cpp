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
	if(file_format_hint == "mysql")
		return DataType::MYSQL;
	if(file_format_hint == "postgresql")
		return DataType::POSTGRESQL;
	if(file_format_hint == "sqlite")
		return DataType::SQLITE;
	// NOTE if you need more options the user can pass file_format in the create table

	return DataType::UNDEFINED;
}

DataType inferFileType(std::vector<std::string> files, DataType data_type_hint, bool ignore_missing_paths) {
	if(data_type_hint == DataType::PARQUET || data_type_hint == DataType::CSV || data_type_hint == DataType::JSON ||
		data_type_hint == DataType::ORC || data_type_hint == DataType::MYSQL ||
    data_type_hint == DataType::POSTGRESQL || data_type_hint == DataType::SQLITE) {
		return data_type_hint;
	}

	std::vector<Uri> uris;
	std::transform(
		files.begin(), files.end(), std::back_inserter(uris), [](std::string uri) -> Uri { return Uri(uri); });
	ral::io::uri_data_provider udp(uris, ignore_missing_paths);
	bool open_file = false;
	const ral::io::data_handle dh = udp.get_next(open_file);
	std::string ext = dh.uri.getPath().getFileExtension();
	std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

	return inferDataType(ext);
}

bool map_contains(std::string key, std::map<std::string, std::string> args) { return !(args.find(key) == args.end()); }

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

cudf::io::json_reader_options getJsonReaderOptions(const std::map<std::string, std::string> & args, cudf::io::arrow_io_source & arrow_source) {

	cudf::io::json_reader_options reader_opts = cudf::io::json_reader_options::builder(cudf::io::source_info{&arrow_source});
	reader_opts.enable_lines(true);
	if(map_contains("dtype", args)) {
		reader_opts.dtypes(to_vector_string(args.at("dtype")));
	}
	if(map_contains("compression", args)) {
		reader_opts.compression(static_cast<cudf::io::compression_type>(to_int(args.at("compression"))));
	}
	if(map_contains("lines", args)) {
		reader_opts.enable_lines(to_bool(args.at("lines")));
	}
	if(map_contains("dayfirst", args)) {
		reader_opts.enable_dayfirst(to_bool(args.at("dayfirst")));
	}
	if(map_contains("byte_range_offset", args)) {
		reader_opts.set_byte_range_offset( (size_t) to_int(args.at("byte_range_offset")) );
	}
	if(map_contains("byte_range_size", args)) {
		reader_opts.set_byte_range_size( (size_t) to_int(args.at("byte_range_size")) );
	}
	return reader_opts;
}

cudf::io::orc_reader_options getOrcReaderOptions(const std::map<std::string, std::string> & args, cudf::io::arrow_io_source & arrow_source) {

	cudf::io::orc_reader_options reader_opts = cudf::io::orc_reader_options::builder(cudf::io::source_info{&arrow_source});
	if(map_contains("stripes", args)) {
		reader_opts.set_stripes(to_vector_int(args.at("stripes")));
	}
	if(map_contains("skiprows", args)) {
		reader_opts.set_skip_rows(to_int(args.at("skiprows")));
	}
	if(map_contains("num_rows", args)) {
		reader_opts.set_num_rows(to_int(args.at("num_rows")));
	}
	if(map_contains("use_index", args)) {
		reader_opts.enable_use_index(to_int(args.at("use_index")));
	} else {
		reader_opts.enable_use_index(true);
	}
	return reader_opts;
}

cudf::io::csv_reader_options getCsvReaderOptions(const std::map<std::string, std::string> & args, cudf::io::arrow_io_source & arrow_source) {

	cudf::io::csv_reader_options reader_opts = cudf::io::csv_reader_options::builder(cudf::io::source_info{&arrow_source});
	if(map_contains("compression", args)) {
		reader_opts.set_compression((cudf::io::compression_type) to_int(args.at("compression")));
	}
	if(map_contains("lineterminator", args)) {
		reader_opts.set_lineterminator(ord(args.at("lineterminator")));
	}
	if(map_contains("delimiter", args)) {
		reader_opts.set_delimiter(ord(args.at("delimiter")));
	}
	if(map_contains("windowslinetermination", args)) {
		reader_opts.enable_windowslinetermination(to_bool(args.at("windowslinetermination")));
	}
	if(map_contains("delim_whitespace", args)) {
		reader_opts.enable_delim_whitespace(to_bool(args.at("delim_whitespace")));
	}
	if(map_contains("skipinitialspace", args)) {
		reader_opts.enable_skipinitialspace(to_bool(args.at("skipinitialspace")));
	}
	if(map_contains("skip_blank_lines", args)) {
		reader_opts.enable_skip_blank_lines(to_bool(args.at("skip_blank_lines")));
	}
	if(map_contains("nrows", args)) {
		reader_opts.set_nrows((cudf::size_type) to_int(args.at("nrows")));
	}
	if(map_contains("skiprows", args)) {
		reader_opts.set_skiprows((cudf::size_type) to_int(args.at("skiprows")));
	}
	if(map_contains("skipfooter", args)) {
		reader_opts.set_skipfooter((cudf::size_type) to_int(args.at("skipfooter")));
	}
	if(map_contains("names", args)) {
		reader_opts.set_names(to_vector_string(args.at("names")));
		reader_opts.set_header(-1);
	} else {
		reader_opts.set_header(0);
	}
	if(map_contains("header", args)) {
		reader_opts.set_header((cudf::size_type) to_int(args.at("header")));
	}
	if(map_contains("dtype", args)) {
		reader_opts.set_dtypes(to_vector_string(args.at("dtype")));
	}
	if(map_contains("use_cols_indexes", args)) {
		reader_opts.set_use_cols_indexes(to_vector_int(args.at("use_cols_indexes")));
	}
	if(map_contains("use_cols_names", args)) {
		reader_opts.set_use_cols_names(to_vector_string(args.at("use_cols_names")));
	}
	if(map_contains("true_values", args)) {
		reader_opts.set_true_values(to_vector_string(args.at("true_values")));
	}
	if(map_contains("false_values", args)) {
		reader_opts.set_false_values(to_vector_string(args.at("false_values")));
	}
	if(map_contains("na_values", args)) {
		reader_opts.set_na_values(to_vector_string(args.at("na_values")));
	}
	if(map_contains("keep_default_na", args)) {
		reader_opts.enable_keep_default_na(to_bool(args.at("keep_default_na")));
	}
	if(map_contains("na_filter", args)) {
		reader_opts.enable_na_filter(to_bool(args.at("na_filter")));
	}
	if(map_contains("prefix", args)) {
		reader_opts.set_prefix(args.at("prefix"));
	}
	if(map_contains("mangle_dupe_cols", args)) {
		reader_opts.enable_mangle_dupe_cols(to_bool(args.at("mangle_dupe_cols")));
	}
	if(map_contains("dayfirst", args)) {
		reader_opts.enable_dayfirst(to_bool(args.at("dayfirst")));
	}
	if(map_contains("thousands", args)) {
		reader_opts.set_thousands(ord(args.at("thousands")));
	}
	if(map_contains("decimal", args)) {
		reader_opts.set_decimal(ord(args.at("decimal")));
	}
	if(map_contains("comment", args)) {
		reader_opts.set_comment(ord(args.at("comment")));
	}
	if(map_contains("quotechar", args)) {
		reader_opts.set_quotechar(ord(args.at("quotechar")));
	}
	// if (map_contains("quoting", args)) {
	//    reader_opts.quoting = args.at("quoting"]
	if(map_contains("doublequote", args)) {
		reader_opts.enable_doublequote(to_bool(args.at("doublequote")));
	}
	if(map_contains("byte_range_offset", args)) {
		reader_opts.set_byte_range_offset((size_t) to_int(args.at("byte_range_offset")));
	}
	if(map_contains("byte_range_size", args)) {
		reader_opts.set_byte_range_size((size_t) to_int(args.at("byte_range_size")));
	}
	if(map_contains("out_time_unit", args)) {
		// TODO
		// reader_opts.out_time_unit = args.at("out_time_unit");
	}
	return reader_opts;
}

std::map<std::string, std::string> to_map(std::vector<std::string> arg_keys, std::vector<std::string> arg_values) {
	std::map<std::string, std::string> ret;
	for(size_t i = 0; i < arg_keys.size(); ++i) {
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
	case DataType::MYSQL: return "mysql"; break;
	case DataType::POSTGRESQL: return "postgresql"; break;
	case DataType::SQLITE: return "sqlite"; break;
	default: break;
	}

	return "undefined";
}

sql_info getSqlInfo(std::map<std::string, std::string> &args_map) {
  // TODO percy william maybe we can move this constant as a bc.BlazingContext config opt
  const size_t DETAULT_TABLE_BATCH_SIZE = 100000;
  // TODO(percy, cristhian): add exception for key error and const
  // TODO(percy, cristhian): for sqlite, add contionals to avoid unncessary fields
  sql_info sql;
  if (args_map.find("hostname") != args_map.end()) {
    sql.host = args_map.at("hostname");
  }
  if (args_map.find("port") != args_map.end()) {
    sql.port = static_cast<std::size_t>(std::atoll(args_map["port"].data()));
  }
  if (args_map.find("username") != args_map.end()) {
    sql.user = args_map.at("username");
  }
  if (args_map.find("password") != args_map.end()) {
    sql.password = args_map.at("password");
  }
  if (args_map.find("database") != args_map.end()) {
    sql.schema = args_map.at("database");
  }
  if (args_map.find("table") != args_map.end()) {
    sql.table = args_map.at("table");
  }
  if (args_map.find("table_filter") != args_map.end()) {
    sql.table_filter = args_map.at("table_filter");
  }
  if (args_map.find("table_batch_size") != args_map.end()) {
    if (args_map.at("table_batch_size").empty()) {
      sql.table_batch_size = DETAULT_TABLE_BATCH_SIZE;
    } else {
      sql.table_batch_size = static_cast<std::size_t>(std::atoll(args_map.at("table_batch_size").data())); 
    }
  } else {
    sql.table_batch_size = DETAULT_TABLE_BATCH_SIZE;
  }
  return sql;
}

} /* namespace io */
} /* namespace ral */
