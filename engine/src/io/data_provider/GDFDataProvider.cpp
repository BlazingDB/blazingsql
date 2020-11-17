/*
 * uridataprovider.cpp
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 *
 * 2019 Percy Camilo Triveño Aucahuasi <percy@blazingsql.com>
 */

#include "GDFDataProvider.h"
#include "Config/BlazingContext.h"
#include "arrow/status.h"
#include <blazingdb/io/FileSystem/Uri.h>
#include <blazingdb/io/Util/StringUtil.h>

using namespace fmt::literals;

namespace ral {
namespace io {

gdf_data_provider::gdf_data_provider(std::vector<ral::frame::BlazingTableView> table_views, std::vector< std::map<std::string,std::string> > column_values)
: table_views(table_views), current_file(0), column_values(column_values)
{

}

size_t gdf_data_provider::get_num_handles(){
	return table_views.size();
}


std::shared_ptr<data_provider> gdf_data_provider::clone() {
	return std::make_shared<gdf_data_provider>(this->table_views, this->column_values);
}

gdf_data_provider::~gdf_data_provider() {

}

bool gdf_data_provider::has_next() { return this->current_file < table_views.size(); }

void gdf_data_provider::reset() {
	this->current_file = 0;
}

/**
 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files, 
 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
 * open_file = true will actually open the file and return a std::shared_ptr<arrow::io::RandomAccessFile>. If its false it will return a nullptr
 */
std::vector<data_handle> gdf_data_provider::get_some(std::size_t num_files, bool open_file){
	std::size_t count = 0;
	std::vector<data_handle> file_handles;
	while(this->has_next() && count < num_files) {
		auto handle = this->get_next(open_file);
		if (handle.is_valid())
			file_handles.emplace_back(std::move(handle));
		count++;
	}
	return file_handles;
}


data_handle gdf_data_provider::get_next(bool open_file) {
	
	data_handle handle(nullptr,column_values[current_file],Uri("gdf"),table_views[current_file]);
	current_file++;
	return handle;
	
}

/**
 * Closes currently open set of file handles maintained by the provider
*/
void gdf_data_provider::close_file_handles() {

}

std::vector<std::string> gdf_data_provider::get_errors() { return {}; }

} /* namespace io */
} /* namespace ral */
