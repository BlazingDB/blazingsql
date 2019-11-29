/*
 * GDFCounter.h
 *
 *  Created on: Sep 12, 2018
 *      Author: rqc
 */

#ifndef GDFCOUNTER_H_
#define GDFCOUNTER_H_

#include "gdf_wrapper/gdf_wrapper.cuh"
#include <map>
#include <mutex>


class GDFRefCounter
{
	private:
		GDFRefCounter();

		static GDFRefCounter* Instance;

		std::mutex gc_mutex;

		std::map<gdf_column *, size_t> map; // std::map<key_ptr, ref_counter>

	public:
		void increment(gdf_column* col_ptr);

		void decrement(gdf_column* col_ptr);

		//Deallocating memory from the resultset repository
		//Used by decrement and for freeing data that has been deregistered previously
		void free(gdf_column* col_ptr);

		void register_column(gdf_column* col_ptr);

		void deregister_column(gdf_column* col_ptr);

		size_t get_map_size();

		void show_summary();

		bool contains_column(gdf_column * ptrs);
		static GDFRefCounter* getInstance();

    std::size_t column_ref_value(gdf_column* column);
};

#endif /* GDFCOUNTER_H_ */
