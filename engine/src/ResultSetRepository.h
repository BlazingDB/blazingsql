/*
 * ResultSetRepository.h
 *
 *  Created on: Sep 21, 2018
 *      Author: felipe
 */

#ifndef RESULTSETREPOSITORY_H_
#define RESULTSETREPOSITORY_H_

#include "DataFrame.h"
#include "Types.h"
#include <map>
#include <vector>
#include <random>
#include <mutex>
#include <condition_variable>

typedef void * response_descriptor; //this shoudl be substituted for something that can generate a response

struct result_set_t {
    bool is_ready;
    blazing_frame result_frame;
    double duration;
	std::string errorMsg;
    size_t ref_counter;
};

//singleton class
class result_set_repository {
public:

	bool try_free_result(connection_id_t connection, query_token_t token);
	void free_result(connection_id_t connection, query_token_t token);
	virtual ~result_set_repository();
	result_set_repository();
	static result_set_repository & get_instance(){
		  static result_set_repository instance;
		  return instance;
	}

	query_token_t register_query(connection_id_t connection, query_token_t token);
	void update_token(query_token_t token, blazing_frame frame, double duration, std::string errorMsg = "");
	connection_id_t init_session();
	void remove_all_connection_tokens(connection_id_t connection);
	result_set_t get_result(connection_id_t connection, query_token_t token);
	gdf_column_cpp get_column(connection_id_t connection, column_token_t columnToken);

	result_set_repository(result_set_repository const&)	= delete;
	void operator=(result_set_repository const&)		= delete;
private:
	std::map<query_token_t,result_set_t> result_sets;
	std::map<connection_id_t,std::vector<query_token_t> > connection_result_sets;
	std::map<column_token_t,gdf_column_cpp> precalculated_columns;

	void add_token(query_token_t token, connection_id_t connection);
	std::map<query_token_t,response_descriptor> requested_responses;
	std::mutex repo_mutex;
	std::condition_variable cv;
};

template<typename T>
T gen_token(){
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<T> dis(
			std::numeric_limits<T>::min(),
			std::numeric_limits<T>::max());

	return dis(gen);
}

#endif /* RESULTSETREPOSITORY_H_ */
