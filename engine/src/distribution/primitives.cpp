#include "distribution/primitives.h"
#include "CalciteExpressionParsing.h"
#include "ColumnManipulation.cuh"
#include "Traits/RuntimeTraits.h"
#include "communication/CommunicationData.h"
#include "communication/factory/MessageFactory.h"
#include "communication/messages/ComponentMessages.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include "config/GPUManager.cuh"
#include "cuDF/generator/sample_generator.h"
#include "cuDF/safe_nvcategory_gather.hpp"
#include "distribution/Exception.h"
#include "distribution/primitives_util.cuh"
#include "legacy/groupby.hpp"
#include "legacy/reduction.hpp"
#include "operators/GroupBy.h"
#include "utilities/RalColumn.h"
#include "utilities/StringUtils.h"
#include "utilities/TableWrapper.h"
#include <algorithm>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cassert>
#include <cmath>
#include <cudf/legacy/table.hpp>
#include <iostream>
#include <memory>
#include <numeric>
#include <types.hpp>

#include "cudf/legacy/copying.hpp"
#include "cudf/legacy/merge.hpp"
#include "cudf/legacy/search.hpp"
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>

#include "utilities/CommonOperations.h"

namespace ral {
namespace distribution {
namespace sampling {

double calculateSampleRatio(cudf::size_type tableSize) { return std::ceil(1.0 - std::pow(tableSize / 1.0E11, 8E-4)); }

std::vector<gdf_column_cpp> generateSample(const std::vector<gdf_column_cpp> & table, double ratio) {
	std::size_t quantity = std::ceil(table[0].get_gdf_column()->size() * ratio);
	return generateSample(table, quantity);
}

std::vector<std::vector<gdf_column_cpp>> generateSamples(
	const std::vector<std::vector<gdf_column_cpp>> & tables, const std::vector<double> & ratios) {
	std::vector<std::size_t> quantities;
	quantities.reserve(tables.size());

	for(std::size_t i = 0; i < tables.size(); i++) {
		quantities.push_back(std::ceil(tables[i][0].get_gdf_column()->size() * ratios[i]));
	}

	return generateSamples(tables, quantities);
}

std::vector<gdf_column_cpp> generateSample(const std::vector<gdf_column_cpp> & table, std::size_t quantity) {
	std::vector<gdf_column_cpp> sample;

	gdf_error gdf_status = cudf::generator::generate_sample(table, sample, quantity);
	if(GDF_SUCCESS != gdf_status) {
		throw std::runtime_error(
			"[ERROR] " + std::string{__FUNCTION__} + " -- CUDF: " + gdf_error_get_name(gdf_status));
	}

	return sample;
}

std::vector<std::vector<gdf_column_cpp>> generateSamples(
	const std::vector<std::vector<gdf_column_cpp>> & input_tables, std::vector<std::size_t> & quantities) {
	// verify
	if(input_tables.size() != quantities.size()) {
		throw std::runtime_error("[ERROR] " + std::string{__FUNCTION__} + " -- size mismatch.");
	}

	// output data
	std::vector<std::vector<gdf_column_cpp>> result;

	// make sample for each table
	for(std::size_t k = 0; k < input_tables.size(); ++k) {
		result.emplace_back(generateSample(input_tables[k], quantities[k]));
	}

	// done
	return result;
}

void normalizeSamples(std::vector<NodeSamples> & samples) {
	std::vector<double> representativities(samples.size());

	for(std::size_t i = 0; i < samples.size(); i++) {
		representativities[i] = (double) samples[i].getColumns()[0].get_gdf_column()->size() / samples[i].getTotalRowSize();
	}

	const double minimumRepresentativity = *std::min_element(representativities.cbegin(), representativities.cend());

	for(std::size_t i = 0; i < samples.size(); i++) {
		double representativenessRatio = minimumRepresentativity / representativities[i];

		if(representativenessRatio > THRESHOLD_FOR_SUBSAMPLING) {
			samples[i].setColumns(generateSample(samples[i].getColumns(), representativenessRatio));
		}
	}
}

}  // namespace sampling
}  // namespace distribution
}  // namespace ral


namespace ral {
namespace distribution {

void sendSamplesToMaster(const Context & context, std::vector<gdf_column_cpp> & samples, std::size_t total_row_size) {
	using MessageFactory = ral::communication::messages::Factory;
	using SampleToNodeMasterMessage = ral::communication::messages::SampleToNodeMasterMessage;

	// Get master node
	const Node & master_node = context.getMasterNode();

	// Get self node
	using CommunicationData = ral::communication::CommunicationData;
	auto self_node = CommunicationData::getInstance().getSharedSelfNode();

	// Get context token
	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto message =
		MessageFactory::createSampleToNodeMaster(message_id, context_token, self_node, total_row_size, samples);

	// Send message to master
	using Client = ral::communication::network::Client;
	Library::Logging::Logger().logTrace(ral::utilities::buildLogString(std::to_string(context_token),
		std::to_string(context.getQueryStep()),
		std::to_string(context.getQuerySubstep()),
		"About to send sendSamplesToMaster message"));
	Client::send(master_node, *message);
}

std::vector<NodeSamples> collectSamples(const Context & context) {
	using ral::communication::messages::SampleToNodeMasterMessage;
	using ral::communication::network::Server;

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	std::vector<NodeSamples> nodeSamples;
	size_t size = context.getWorkerNodes().size();
	std::vector<bool> received(context.getTotalNodes(), false);
	for(int k = 0; k < size; ++k) {
		auto message = Server::getInstance().getMessage(context_token, message_id);

		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}

		auto concreteMessage = std::static_pointer_cast<SampleToNodeMasterMessage>(message);
		auto node = message->getSenderNode();
		int node_idx = context.getNodeIndex(*node);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context.getQueryStep()),
				std::to_string(context.getQuerySubstep()),
				"ERROR: Already received collectSamples from node " + std::to_string(node_idx)));
		}
		nodeSamples.emplace_back(concreteMessage->getTotalRowSize(), *node, concreteMessage->getSamples());
		received[node_idx] = true;
	}

	return nodeSamples;
}

std::vector<gdf_column_cpp> generatePartitionPlans(
	const Context & context, std::vector<NodeSamples> & samples, std::vector<int8_t> & sortOrderTypes) {
	std::vector<std::vector<gdf_column_cpp>> tables(samples.size());
	std::transform(samples.begin(), samples.end(), tables.begin(), [](NodeSamples & nodeSamples) {
		return nodeSamples.getColumns();
	});

	std::vector<gdf_column_cpp> concatSamples = ral::utilities::concatTables(tables);

	cudf::size_type outputRowSize = concatSamples[0].get_gdf_column()->size();

	std::vector<cudf::column *> rawConcatSamples(concatSamples.size());
	std::transform(
		concatSamples.cbegin(), concatSamples.cend(), rawConcatSamples.begin(), [](const gdf_column_cpp & col) {
			return col.get_gdf_column();
		});

	// Sort
	gdf_column_cpp ascDescCol;
	ascDescCol.create_gdf_column(cudf::type_id::INT8,
		sortOrderTypes.size(),
		sortOrderTypes.data(),
		ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT8),
		"");

	gdf_column_cpp sortedIndexCol;
	sortedIndexCol.create_gdf_column(cudf::type_id::INT32,
		outputRowSize,
		nullptr,
		ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
		"");

	gdf_context gdfContext;
	gdfContext.flag_null_sort_behavior = GDF_NULL_AS_LARGEST;  // Nulls are are treated as largest

	// TODO percy cudf0.12 port to cudf::column
//	CUDF_CALL(gdf_order_by(rawConcatSamples.data(),
//		(int8_t *) (ascDescCol.get_gdf_column()->data),
//		rawConcatSamples.size(),
//		sortedIndexCol.get_gdf_column(),
//		&gdfContext));

	std::vector<gdf_column_cpp> sortedSamples(concatSamples.size());
	for(size_t i = 0; i < sortedSamples.size(); i++) {
		auto & col = concatSamples[i];
		if(col.get_gdf_column()->has_nulls()) {
			sortedSamples[i].create_gdf_column(col.get_gdf_column()->type().id(),
				col.get_gdf_column()->size(),
				nullptr,
				ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
				col.name());
		} else {
			sortedSamples[i].create_gdf_column(col.get_gdf_column()->type().id(),
				col.get_gdf_column()->size(),
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
				col.name());
		}

		materialize_column(concatSamples[i].get_gdf_column(), sortedSamples[i].get_gdf_column(), sortedIndexCol.get_gdf_column());
		
		// TODO percy cudf0.12 port to cudf::column
		//sortedSamples[i].update_null_count();
	}

	// Gather
	cudf::size_type pivotsSize = outputRowSize > 0 ? context.getTotalNodes() - 1 : 0;
	std::vector<gdf_column_cpp> pivots(sortedSamples.size());
	for(size_t i = 0; i < sortedSamples.size(); i++) {
		auto & col = sortedSamples[i];
		if(col.get_gdf_column()->has_nulls()) {
			pivots[i].create_gdf_column(col.get_gdf_column()->type().id(),
				pivotsSize,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
				col.name());
		} else {
			pivots[i].create_gdf_column(col.get_gdf_column()->type().id(),
				pivotsSize,
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
				col.name());
		}
	}

	if(outputRowSize > 0) {
		cudf::table srcTable = ral::utilities::create_table(sortedSamples);
		cudf::table destTable = ral::utilities::create_table(pivots);

		int step = outputRowSize / context.getTotalNodes();
		gdf_column_cpp gatherMap;
		gatherMap.create_gdf_column(cudf::type_id::INT32,
			context.getTotalNodes() - 1,
			nullptr,
			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
			"");
		
		// TODO percy cudf0.12 port to cudf::column
		//gdf_sequence(static_cast<int32_t *>(gatherMap.get_gdf_column()->data), gatherMap.get_gdf_column()->size(), step, step);

		// TODO percy cudf0.12 port to cudf::column
		//cudf::gather(&srcTable, (gdf_size_type *) (gatherMap.get_gdf_column()->data), &destTable);
		
		// TODO percy cudf0.12 port to cudf::column and custrings
		//ral::init_string_category_if_null(destTable);
	}

	return pivots;
}

void distributePartitionPlan(const Context & context, std::vector<gdf_column_cpp> & pivots) {
	using ral::communication::CommunicationData;
	using ral::communication::messages::Factory;
	using ral::communication::messages::PartitionPivotsMessage;

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto node = CommunicationData::getInstance().getSharedSelfNode();
	auto message = Factory::createPartitionPivotsMessage(message_id, context_token, node, pivots);
	broadcastMessage(context.getWorkerNodes(), message);
}

std::vector<gdf_column_cpp> getPartitionPlan(const Context & context) {
	using ral::communication::messages::PartitionPivotsMessage;
	using ral::communication::network::Server;

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto message = Server::getInstance().getMessage(context_token, message_id);

	if(message->getMessageTokenValue() != message_id) {
		throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
	}

	auto concreteMessage = std::static_pointer_cast<PartitionPivotsMessage>(message);

	return concreteMessage->getColumns();
}

std::vector<NodeColumns> split_data_into_NodeColumns(
	const Context & context, const std::vector<gdf_column_cpp> & table, const gdf_column_cpp & indexes) {
	std::vector<std::vector<cudf::column *>> split_table(table.size());  // this will be [colInd][splitInd]
	for(std::size_t k = 0; k < table.size(); ++k) {
		// TODO percy cudf0.12 port to cudf::column
		//split_table[k] = cudf::split(*(table[k].get_gdf_column()), static_cast<gdf_size_type *>(indexes.data()), indexes.get_gdf_column()->size());
	}

	// get nodes
	auto nodes = context.getAllNodes();

	// generate NodeColumns
	std::vector<NodeColumns> array_node_columns;
	for(std::size_t i = 0; i < nodes.size(); ++i) {
		std::vector<gdf_column_cpp> columns(table.size());
		for(std::size_t k = 0; k < table.size(); ++k) {
			// TODO percy cudf0.12 port to cudf::column
			//split_table[k][i]->col_name = nullptr;
			//columns[k].create_gdf_column(split_table[k][i]);
			
			columns[k].set_name(table[k].name());
		}

		array_node_columns.emplace_back(*nodes[i], columns);
	}
	return array_node_columns;
}


std::vector<NodeColumns> partitionData(const Context & context,
	std::vector<gdf_column_cpp> & table,
	std::vector<int> & searchColIndices,
	std::vector<gdf_column_cpp> & pivots,
	bool isTableSorted,
	std::vector<int8_t> sortOrderTypes) {
	// verify input
	if(pivots.size() == 0) {
		throw std::runtime_error("The pivots array is empty");
	}

	if(pivots.size() != searchColIndices.size()) {
		throw std::runtime_error("The pivots and searchColIndices vectors don't have the same size");
	}

	auto & pivot = pivots[0];
	{
		std::size_t size = pivot.get_gdf_column()->size();

		// verify the size of the pivots.
		for(std::size_t k = 1; k < pivots.size(); ++k) {
			if(size != pivots[k].get_gdf_column()->size()) {
				throw std::runtime_error("The pivots don't have the same size");
			}
		}

		// verify the size in pivots and nodes
		auto nodes = context.getAllNodes();
		if(nodes.size() != (size + 1)) {
			throw std::runtime_error("The size of the nodes needs to be the same as the size of the pivots plus one");
		}
	}

	cudf::size_type table_row_size = table[0].get_gdf_column()->size();
	if(table_row_size == 0) {
		std::vector<NodeColumns> array_node_columns;
		auto nodes = context.getAllNodes();
		for(std::size_t i = 0; i < nodes.size(); ++i) {
			array_node_columns.emplace_back(*nodes[i], table);
		}
		return array_node_columns;
	}

	if(sortOrderTypes.size() == 0) {
		sortOrderTypes.assign(searchColIndices.size(), 0);
	}

	std::vector<bool> desc_flags(sortOrderTypes.begin(), sortOrderTypes.end());

	// Ensure data is sorted.
	// Would it be better to use gdf_hash instead or gdf_order_by?
	std::vector<gdf_column_cpp> sortedTable;
	if(!isTableSorted) {
		std::vector<cudf::column *> key_cols_vect(searchColIndices.size());
		std::transform(searchColIndices.cbegin(), searchColIndices.cend(), key_cols_vect.begin(), [&](const int index) {
			return table[index].get_gdf_column();
		});

		gdf_column_cpp asc_desc_col;
		asc_desc_col.create_gdf_column(cudf::type_id::INT8,
			sortOrderTypes.size(),
			sortOrderTypes.data(),
			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT8),
			"");

		gdf_column_cpp index_col;
		index_col.create_gdf_column(cudf::type_id::INT32,
			table_row_size,
			nullptr,
			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
			"");

		gdf_context gdfcontext;
		gdfcontext.flag_null_sort_behavior = GDF_NULL_AS_LARGEST;  // Nulls are are treated as largest

		// TODO percy cudf0.12 port to cudf::column
//		CUDF_CALL(gdf_order_by(key_cols_vect.data(),
//			(int8_t *) (asc_desc_col.get_gdf_column()->data),
//			key_cols_vect.size(),
//			index_col.get_gdf_column(),
//			&gdfcontext));

		sortedTable.resize(table.size());
		for(size_t i = 0; i < sortedTable.size(); i++) {
			auto & col = table[i];
			if(col.get_gdf_column()->has_nulls()) {
				sortedTable[i].create_gdf_column(col.get_gdf_column()->type().id(),
					col.get_gdf_column()->size(),
					nullptr,
					ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
					col.name());
			} else {
				sortedTable[i].create_gdf_column(col.get_gdf_column()->type().id(),
					col.get_gdf_column()->size(),
					nullptr,
					nullptr,
					ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
					col.name());
			}

			materialize_column(table[i].get_gdf_column(), sortedTable[i].get_gdf_column(), index_col.get_gdf_column());
			
			// TODO percy cudf0.12 port to cudf::column
			//sortedTable[i].update_null_count();
		}

		table = sortedTable;
	}

	std::vector<gdf_column_cpp> sync_haystack(searchColIndices.size());
	std::transform(searchColIndices.cbegin(), searchColIndices.cend(), sync_haystack.begin(), [&](const int index) {
		return table[index];
	});
	std::vector<gdf_column_cpp> sync_needles(pivots);
	for(cudf::size_type i = 0; i < sync_haystack.size(); i++) {
		gdf_column_cpp & left_col = sync_haystack[i];
		gdf_column_cpp & right_col = sync_needles[i];

		if(left_col.get_gdf_column()->type().id() != GDF_STRING_CATEGORY) {
			continue;
		}

		// Sync the nvcategories to make them comparable
		// Workaround for https://github.com/rapidsai/cudf/issues/2790

		gdf_column_cpp new_left_column;
		new_left_column.allocate_like(left_col);
		cudf::column * new_left_column_ptr = new_left_column.get_gdf_column();
		gdf_column_cpp new_right_column;
		new_right_column.allocate_like(right_col);
		cudf::column * new_right_column_ptr = new_right_column.get_gdf_column();

		if(left_col.get_gdf_column()->has_nulls()) {
			// TODO percy cudf0.12 port to cudf::column
//			CUDA_TRY(cudaMemcpy(new_left_column_ptr->valid,
//				left_col.valid(),
//				gdf_valid_allocation_size(new_left_column_ptr->size),
//				cudaMemcpyDefault));
//			new_left_column_ptr->null_count = left_col.null_count();
		}

		if(right_col.get_gdf_column()->has_nulls()) {
			// TODO percy cudf0.12 port to cudf::column
//			CUDA_TRY(cudaMemcpy(new_right_column_ptr->valid,
//				right_col.valid(),
//				gdf_valid_allocation_size(new_right_column_ptr->size),
//				cudaMemcpyDefault));
//			new_right_column_ptr->null_count = right_col.null_count();
		}

		// TODO percy cudf0.12 port to cudf::column
//		cudf::column * tmp_arr_input[2] = {left_col.get_gdf_column(), right_col.get_gdf_column()};
//		cudf::column * tmp_arr_output[2] = {new_left_column_ptr, new_right_column_ptr};
//		CUDF_TRY(sync_column_categories(tmp_arr_input, tmp_arr_output, 2));

		sync_haystack[i] = new_left_column;
		sync_needles[i] = new_right_column;
	}

	cudf::table haystack_table = ral::utilities::create_table(sync_haystack);
	cudf::table needles_table = ral::utilities::create_table(sync_needles);

	// We want the raw_indexes be on the heap because indexes will call delete when it goes out of scope
	// TODO percy cudf0.12 port to cudf::column
//	cudf::column * raw_indexes = new cudf::column{};
//	*raw_indexes = cudf::upper_bound(haystack_table, needles_table, desc_flags,
//		true);  // nulls_as_largest
//	gdf_column_cpp indexes;
//	indexes.create_gdf_column(raw_indexes);
//	sort_indices(indexes);
//	return split_data_into_NodeColumns(context, table, indexes);
}

void distributePartitions(const Context & context, std::vector<NodeColumns> & partitions) {
	using ral::communication::CommunicationData;
	using ral::communication::messages::ColumnDataMessage;
	using ral::communication::messages::Factory;
	using ral::communication::network::Client;

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = ColumnDataMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto self_node = CommunicationData::getInstance().getSharedSelfNode();
	std::vector<std::thread> threads;
	for(auto & nodeColumn : partitions) {
		if(nodeColumn.getNode() == *self_node) {
			continue;
		}
		std::vector<gdf_column_cpp> columns = nodeColumn.getColumns();
		auto destination_node = nodeColumn.getNode();
		threads.push_back(std::thread([message_id, context_token, self_node, destination_node, columns]() mutable {
			auto message = Factory::createColumnDataMessage(message_id, context_token, self_node, columns);
			Client::send(destination_node, *message);
		}));
	}
	for(size_t i = 0; i < threads.size(); i++) {
		threads[i].join();
	}
}

std::vector<NodeColumns> collectPartitions(const Context & context) {
	int num_partitions = context.getTotalNodes() - 1;
	return collectSomePartitions(context, num_partitions);
}

std::vector<NodeColumns> collectSomePartitions(const Context & context, int num_partitions) {
	using ral::communication::messages::ColumnDataMessage;
	using ral::communication::network::Server;

	// Get the numbers of rals in the query
	int number_rals = context.getTotalNodes() - 1;
	std::vector<bool> received(context.getTotalNodes(), false);

	// Create return value
	std::vector<NodeColumns> node_columns;

	// Get message from the server
	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = ColumnDataMessage::MessageID() + "_" + std::to_string(context_comm_token);

	while(0 < num_partitions) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		num_partitions--;

		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}

		auto column_message = std::static_pointer_cast<ColumnDataMessage>(message);
		auto node = message->getSenderNode();
		int node_idx = context.getNodeIndex(*node);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context.getQueryStep()),
				std::to_string(context.getQuerySubstep()),
				"ERROR: Already received collectSomePartitions from node " + std::to_string(node_idx)));
		}
		node_columns.emplace_back(*node, column_message->getColumns());
		received[node_idx] = true;
	}
	return node_columns;
}

void scatterData(const Context & context, std::vector<gdf_column_cpp> & table) {
	using ral::communication::CommunicationData;

	std::vector<NodeColumns> array_node_columns;
	auto nodes = context.getAllNodes();
	for(std::size_t i = 0; i < nodes.size(); ++i) {
		if(!(*(nodes[i].get()) == CommunicationData::getInstance().getSelfNode())) {
			std::vector<gdf_column_cpp> columns = table;
			array_node_columns.emplace_back(*nodes[i], std::move(columns));
		}
	}
	distributePartitions(context, array_node_columns);
}

void sortedMerger(std::vector<NodeColumns> & columns,
	std::vector<int8_t> & sortOrderTypes,
	std::vector<int> & sortColIndices,
	blazing_frame & output) {
	std::vector<order_by_type> ascDesc(sortOrderTypes.size());
	std::transform(sortOrderTypes.begin(), sortOrderTypes.end(), ascDesc.begin(), [&](int8_t sortOrderType) {
		return sortOrderType == 1 ? GDF_ORDER_DESC : GDF_ORDER_ASC;
	});

	std::vector<std::vector<gdf_column_cpp>> gdfColTables;
	for(size_t i = 0; i < columns.size(); i++) {
		std::vector<gdf_column_cpp> cols = columns[i].getColumns();
		if(cols.size() > 0 && cols[0].get_gdf_column()->size() > 0) {
			gdfColTables.emplace_back(cols);
		}
	}

	output.clear();

	if(gdfColTables.empty()) {
		output.add_table(columns[0].getColumns());
		return;
	}

	std::vector<gdf_column_cpp> leftCols(gdfColTables[0]);
	cudf::table leftTable = ral::utilities::create_table(leftCols);
	for(size_t i = 1; i < gdfColTables.size(); i++) {
		std::vector<gdf_column_cpp> rightCols(gdfColTables[i]);
		cudf::table rightTable = ral::utilities::create_table(rightCols);

		// GDF_STRING_CATEGORY columns are sorted but the underlying nvstring may not be, so gather them
		// to ensure they're sorted guaranteeing that sync_column_categories (called in cudf::merge)
		// results are sorted
		gather_and_remap_nvcategory(leftTable);
		gather_and_remap_nvcategory(rightTable);

		cudf::table mergedTable = cudf::merge(leftTable, rightTable, sortColIndices, ascDesc);

		for(size_t j = 0; j < mergedTable.num_columns(); j++) {
			// TODO percy cudf0.12 port to cudf::column
			//cudf::column * col = mergedTable.get_column(j);
			//std::string colName = leftCols[j].name();
			//leftCols[j].create_gdf_column(col);
			//leftCols[j].set_name(colName);
		}

		leftTable = mergedTable;
	}

	output.add_table(leftCols);
}

std::vector<gdf_column_cpp> generatePartitionPlansGroupBy(const Context & context, std::vector<NodeSamples> & samples) {
	std::vector<std::vector<gdf_column_cpp>> tables(samples.size());
	std::transform(samples.begin(), samples.end(), tables.begin(), [](NodeSamples & nodeSamples) {
		return nodeSamples.getColumns();
	});

	std::vector<gdf_column_cpp> concatSamples = ral::utilities::concatTables(tables);

	std::vector<int> groupColumnIndices(concatSamples.size());
	std::iota(groupColumnIndices.begin(), groupColumnIndices.end(), 0);
	
	// TODO percy william alex port distribution
	//std::vector<gdf_column_cpp> groupedSamples = ral::operators::groupby_without_aggregations(concatSamples, groupColumnIndices);
	std::vector<gdf_column_cpp> groupedSamples;
	
	size_t number_of_groups = groupedSamples[0].get_gdf_column()->size();

	// Sort
	std::vector<cudf::column *> rawGroupedSamples{groupedSamples.size()};
	for(size_t i = 0; i < groupedSamples.size(); i++) {
		rawGroupedSamples[i] = groupedSamples[i].get_gdf_column();
	}

	gdf_column_cpp sortedIndexCol;
	sortedIndexCol.create_gdf_column(cudf::type_id::INT32,
		number_of_groups,
		nullptr,
		ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
		"");

	gdf_context gdfContext;
	gdfContext.flag_null_sort_behavior = GDF_NULL_AS_LARGEST;  // Nulls are are treated as largest

	// TODO percy cudf0.12 port to cudf::column
//	CUDF_CALL(gdf_order_by(
//		rawGroupedSamples.data(), nullptr, rawGroupedSamples.size(), sortedIndexCol.get_gdf_column(), &gdfContext));

	std::vector<gdf_column_cpp> sortedSamples(groupedSamples.size());
	for(size_t i = 0; i < sortedSamples.size(); i++) {
		auto & col = groupedSamples[i];
		if(col.get_gdf_column()->has_nulls()) {
			sortedSamples[i].create_gdf_column(col.get_gdf_column()->type().id(),
				col.get_gdf_column()->size(),
				nullptr,
				ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
				col.name());
		} else {
			sortedSamples[i].create_gdf_column(col.get_gdf_column()->type().id(),
				col.get_gdf_column()->size(),
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
				col.name());
		}

		materialize_column(
			groupedSamples[i].get_gdf_column(), sortedSamples[i].get_gdf_column(), sortedIndexCol.get_gdf_column());
		
		// TODO percy cudf0.12 port to cudf::column
//		sortedSamples[i].update_null_count();
	}

	cudf::size_type outputRowSize = sortedSamples[0].get_gdf_column()->size();

	// Gather
	cudf::size_type pivotsSize = outputRowSize > 0 ? context.getTotalNodes() - 1 : 0;
	std::vector<gdf_column_cpp> pivots{sortedSamples.size()};
	for(size_t i = 0; i < sortedSamples.size(); i++) {
		auto & col = sortedSamples[i];
		if(col.get_gdf_column()->has_nulls()) {
			pivots[i].create_gdf_column(col.get_gdf_column()->type().id(),
				pivotsSize,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
				col.name());
		} else {
			pivots[i].create_gdf_column(col.get_gdf_column()->type().id(),
				pivotsSize,
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
				col.name());
		}
	}

	if(number_of_groups > 0) {
		cudf::table srcTable = ral::utilities::create_table(sortedSamples);
		cudf::table destTable = ral::utilities::create_table(pivots);

		int step = number_of_groups / context.getTotalNodes();
		gdf_column_cpp gatherMap;
		gatherMap.create_gdf_column(cudf::type_id::INT32,
			context.getTotalNodes() - 1,
			nullptr,
			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
			"");
		
		// TODO percy cudf0.12 port to cudf::column
//		gdf_sequence(static_cast<int32_t *>(gatherMap.get_gdf_column()->data), gatherMap.get_gdf_column()->size(), step, step);

		// TODO percy cudf0.12 port to cudf::column and custrings
//		cudf::gather(&srcTable, (gdf_size_type *) (gatherMap.get_gdf_column()->data), &destTable);
//		ral::init_string_category_if_null(destTable);
	}

	return pivots;
}

void groupByWithoutAggregationsMerger(
	std::vector<NodeColumns> & groups, const std::vector<int> & groupColIndices, blazing_frame & output) {
	std::vector<std::vector<gdf_column_cpp>> tables(groups.size());
	std::transform(groups.begin(), groups.end(), tables.begin(), [](NodeColumns & nodeColumns) {
		return nodeColumns.getColumns();
	});

	std::vector<gdf_column_cpp> concatGroups = ral::utilities::concatTables(tables);

	// TODO percy william alex port distribution
	//std::vector<gdf_column_cpp> groupedOutput = ral::operators::groupby_without_aggregations(concatGroups, groupColIndices);
	std::vector<gdf_column_cpp> groupedOutput;

	output.clear();
	output.add_table(groupedOutput);
}

void distributeRowSize(const Context & context, std::size_t total_row_size) {
	using ral::communication::CommunicationData;
	using ral::communication::messages::Factory;
	using ral::communication::messages::SampleToNodeMasterMessage;
	using ral::communication::network::Client;


	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto self_node = CommunicationData::getInstance().getSharedSelfNode();
	auto message = Factory::createSampleToNodeMaster(message_id, context_token, self_node, total_row_size, {});

	int self_node_idx = context.getNodeIndex(CommunicationData::getInstance().getSelfNode());
	broadcastMessage(context.getAllOtherNodes(self_node_idx), message);
}

std::vector<cudf::size_type> collectRowSize(const Context & context) {
	using ral::communication::CommunicationData;
	using ral::communication::messages::SampleToNodeMasterMessage;
	using ral::communication::network::Server;

	int num_nodes = context.getTotalNodes();
	std::vector<cudf::size_type> node_row_sizes(num_nodes);
	std::vector<bool> received(num_nodes, false);

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	int self_node_idx = context.getNodeIndex(CommunicationData::getInstance().getSelfNode());
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}
		auto concrete_message = std::static_pointer_cast<SampleToNodeMasterMessage>(message);
		auto node = concrete_message->getSenderNode();
		int node_idx = context.getNodeIndex(*node);
		assert(node_idx >= 0);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context.getQueryStep()),
				std::to_string(context.getQuerySubstep()),
				"ERROR: Already received collectRowSize from node " + std::to_string(node_idx)));
		}
		node_row_sizes[node_idx] = concrete_message->getTotalRowSize();
		received[node_idx] = true;
	}

	return node_row_sizes;
}


void distributeLeftRightNumRows(const Context & context, std::size_t left_num_rows, std::size_t right_num_rows) {
	using ral::communication::CommunicationData;
	using ral::communication::messages::Factory;
	using ral::communication::messages::SampleToNodeMasterMessage;
	using ral::communication::network::Client;

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto self_node = CommunicationData::getInstance().getSharedSelfNode();
	std::vector<gdf_column_cpp> num_rows(1);
	std::vector<int64_t> num_rows_host{left_num_rows, right_num_rows};
	num_rows[0].create_gdf_column(
		cudf::type_id::INT64, 2, &num_rows_host[0], ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT64), "");
	auto message = Factory::createSampleToNodeMaster(message_id, context_token, self_node, 0, num_rows);

	int self_node_idx = context.getNodeIndex(CommunicationData::getInstance().getSelfNode());
	broadcastMessage(context.getAllOtherNodes(self_node_idx), message);
}

void collectLeftRightNumRows(const Context & context,
	std::vector<cudf::size_type> & node_num_rows_left,
	std::vector<cudf::size_type> & node_num_rows_right) {
	using ral::communication::CommunicationData;
	using ral::communication::messages::SampleToNodeMasterMessage;
	using ral::communication::network::Server;

	int num_nodes = context.getTotalNodes();
	node_num_rows_left.resize(num_nodes);
	node_num_rows_right.resize(num_nodes);
	std::vector<bool> received(num_nodes, false);

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = SampleToNodeMasterMessage::MessageID() + "_" + std::to_string(context_comm_token);

	int self_node_idx = context.getNodeIndex(CommunicationData::getInstance().getSelfNode());
	for(cudf::size_type i = 0; i < num_nodes - 1; ++i) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}
		auto concrete_message = std::static_pointer_cast<SampleToNodeMasterMessage>(message);
		auto node = concrete_message->getSenderNode();
		std::vector<gdf_column_cpp> num_rows_data = concrete_message->getSamples();
		assert(num_rows_data.size() == 1);
		assert(num_rows_data[0].get_gdf_column()->size() == 2);
		assert(num_rows_data[0].get_gdf_column()->type().id() == cudf::type_id::INT64);
		std::vector<int64_t> num_rows_host(2);
		
		// TODO percy cudf0.12 port to cudf::column
//		cudaMemcpy(num_rows_host.data(),
//			num_rows_data[0].data(),
//			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT64) * 2,
//			cudaMemcpyDeviceToHost);
		
		int node_idx = context.getNodeIndex(*node);
		assert(node_idx >= 0);
		if(received[node_idx]) {
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
				std::to_string(context.getQueryStep()),
				std::to_string(context.getQuerySubstep()),
				"ERROR: Already received collectLeftRightNumRows from node " + std::to_string(node_idx)));
		}
		node_num_rows_left[node_idx] = num_rows_host[0];
		node_num_rows_right[node_idx] = num_rows_host[1];
		received[node_idx] = true;
	}
}


}  // namespace distribution
}  // namespace ral


namespace ral {
namespace distribution {

std::vector<gdf_column_cpp> generateOutputColumns(
	cudf::size_type column_quantity, cudf::size_type column_size, std::vector<gdf_column_cpp> & table) {
	// Create outcome
	std::vector<gdf_column_cpp> result(column_quantity);

	// Create columns
	for(cudf::size_type k = 0; k < column_quantity; ++k) {
		result[k] = ral::utilities::create_column(column_size, table[k].get_gdf_column()->type().id(), table[k].name());
	}

	// Done
	return result;
}

std::vector<NodeColumns> generateJoinPartitions(
	const Context & context, std::vector<gdf_column_cpp> & table, std::vector<int> & columnIndices) {
	assert(table.size() != 0);

	if(table[0].get_gdf_column()->size() == 0) {
		std::vector<NodeColumns> result;
		auto nodes = context.getAllNodes();
		for(cudf::size_type k = 0; k < nodes.size(); ++k) {
			std::vector<gdf_column_cpp> columns = table;
			result.emplace_back(*nodes[k], columns);
		}
		return result;
	}

	// Support for GDF_STRING_CATEGORY. We need the string hashes not the category indices
	std::vector<gdf_column_cpp> temp_input_table(table);
	std::vector<int> temp_input_col_indices(columnIndices);
	for(size_t i = 0; i < columnIndices.size(); i++) {
		gdf_column_cpp & col = table[columnIndices[i]];

		if(col.get_gdf_column()->type().id() != cudf::type_id::STRING)
			continue;

		// TODO percy cudf0.12 port to cudf::column and custrings
//		NVCategory * nvCategory = static_cast<NVCategory *>(col.get_gdf_column()->dtype_info.category);
//		NVStrings * nvStrings =
//			nvCategory->gather_strings(static_cast<nv_category_index_type *>(col.data()), col.get_gdf_column()->size(), true);

//		gdf_column_cpp str_hash;
//		str_hash.create_gdf_column(cudf::type_id::INT32,
//			nvStrings->size(),
//			nullptr,
//			nullptr,
//			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
//			"");
//		nvStrings->hash(static_cast<unsigned int *>(str_hash.data()));
//		NVStrings::destroy(nvStrings);

//		temp_input_col_indices[i] = temp_input_table.size();
//		temp_input_table.push_back(str_hash);
	}


	std::vector<cudf::column *> raw_input_table_col_ptrs(temp_input_table.size());
	std::transform(
		temp_input_table.begin(), temp_input_table.end(), raw_input_table_col_ptrs.begin(), [](auto & cpp_col) {
			return cpp_col.get_gdf_column();
		});
	
	// TODO percy cudf0.12 port to cudf::column
	//cudf::table input_table_wrapper(raw_input_table_col_ptrs);

	// Generate partition offset vector
	cudf::size_type number_nodes = context.getTotalNodes();
	std::vector<gdf_size_type> partition_offset(number_nodes);

	// Preallocate output columns
	// TODO percy cudf0.12 port to cudf::column
	//std::vector<gdf_column_cpp> output_columns = generateOutputColumns(input_table_wrapper.num_columns(), input_table_wrapper.num_rows(), temp_input_table);

	// copy over nvcategory to output
	// NOTE that i am having to do this due to an already identified issue: https://github.com/rapidsai/cudf/issues/1474
	// TODO percy cudf0.12 port to cudf::column
//	for(size_t i = 0; i < output_columns.size(); i++) {
//		// TODO percy cudf0.12 custrings this was not commented
////		if(output_columns[i].dtype() == GDF_STRING_CATEGORY && temp_input_table[i].dtype_info().category) {
////			output_columns[i].get_gdf_column()->dtype_info.category =
////				static_cast<void *>(static_cast<NVCategory *>(temp_input_table[i].dtype_info().category)->copy());
////		}
//	}

	// TODO percy cudf0.12 port to cudf::column
//	std::vector<gdf_column *> raw_output_table_col_ptrs(output_columns.size());
//	std::transform(output_columns.begin(), output_columns.end(), raw_output_table_col_ptrs.begin(), [](auto & cpp_col) {
//		return cpp_col.get_gdf_column();
//	});
//	cudf::table output_table_wrapper(raw_output_table_col_ptrs);

	// Execute operation
	// TODO percy cudf0.12 port to cudf::column
//	CUDF_CALL(gdf_hash_partition(input_table_wrapper.num_columns(),
//		input_table_wrapper.begin(),
//		temp_input_col_indices.data(),
//		temp_input_col_indices.size(),
//		number_nodes,
//		output_table_wrapper.begin(),
//		partition_offset.data(),
//		gdf_hash_func::GDF_HASH_MURMUR3));

	// TODO percy cudf0.12 port to cudf::column
//	std::vector<gdf_column_cpp> temp_output_columns(output_columns.begin(), output_columns.begin() + table.size());
//	for(size_t i = 0; i < table.size(); i++) {
//		gdf_column * srcCol = input_table_wrapper.get_column(i);
//		gdf_column * dstCol = output_table_wrapper.get_column(i);

//		if(srcCol->dtype != GDF_STRING_CATEGORY)
//			continue;

//		ral::safe_nvcategory_gather_for_string_category(dstCol, srcCol->dtype_info.category);
//	}

	// Erase input table
	table.clear();

	// lets get the split indices. These are all the partition_offset, except for the first since its just 0
	gdf_column_cpp indexes;
	indexes.create_gdf_column(cudf::type_id::INT32,
		number_nodes - 1,
		partition_offset.data() + 1,
		nullptr,
		ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
		"");

	// TODO percy cudf0.12 port to cudf::column
	//return split_data_into_NodeColumns(context, temp_output_columns, indexes);
}


void broadcastMessage(
	std::vector<std::shared_ptr<Node>> nodes, std::shared_ptr<communication::messages::Message> message) {
	std::vector<std::thread> threads(nodes.size());
	for(size_t i = 0; i < nodes.size(); i++) {
		std::shared_ptr<Node> node = nodes[i];
		threads[i] = std::thread([node, message]() {
			ral::communication::network::Client::send(*node, *message);
		});
	}
	for(size_t i = 0; i < threads.size(); i++) {
		threads[i].join();
	}
}

}  // namespace distribution
}  // namespace ral


namespace ral {

namespace distribution {
namespace experimental {

using namespace ral::frame;

std::unique_ptr<BlazingTable> generatePartitionPlans(
				const Context & context, std::vector<NodeColumnView> & samples, 
				std::vector<std::size_t> & table_total_rows, std::vector<int8_t> & sortOrderTypes) {
	
	std::vector<BlazingTableView> tables;
	for (auto sample : samples){
		tables.push_back(sample.second);
	}
	
	std::unique_ptr<BlazingTable> concatSamples = ral::utilities::experimental::concatTables(tables);

	std::vector<cudf::order> column_order;
	for(auto col_order : sortOrderTypes){
		if(col_order)
			column_order.push_back(cudf::order::DESCENDING);
		else
			column_order.push_back(cudf::order::ASCENDING);
	}
	std::vector<cudf::null_order> null_orders(column_order.size(), cudf::null_order::AFTER);
	std::unique_ptr<cudf::column> sort_indices = cudf::experimental::sorted_order( concatSamples->view(), column_order, null_orders);

	std::unique_ptr<CudfTable> sortedSamples = cudf::experimental::gather( concatSamples->view(), sort_indices->view() );

	cudf::size_type outputRowSize = sortedSamples->view().num_rows();
	cudf::size_type pivotsSize = outputRowSize > 0 ? context.getTotalNodes() - 1 : 0;
	
	int32_t step = outputRowSize / context.getTotalNodes();

	auto sequence_iter = cudf::test::make_counting_transform_iterator(0, [step](auto i) { return int32_t(i * step) + step;});    
	cudf::test::fixed_width_column_wrapper<int32_t> gather_map_wrapper(sequence_iter, sequence_iter + pivotsSize);
	CudfColumnView gather_map(gather_map_wrapper);
	std::unique_ptr<CudfTable> pivots = cudf::experimental::gather( sortedSamples->view(), gather_map );

	return std::make_unique<BlazingTable>(std::move(pivots), concatSamples->names());	
}

void distributePartitionPlan(const Context & context, BlazingTableView & pivots) {
	using ral::communication::experimental::CommunicationData;
	using ral::communication::messages::Factory;
	using ral::communication::messages::PartitionPivotsMessage;

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto node = CommunicationData::getInstance().getSelfNode();
	// WSM TODO wating on message
	// auto message = Factory::createPartitionPivotsMessage(message_id, context_token, node, pivots);
	// broadcastMessage(context.getWorkerNodes(), message);
}

std::unique_ptr<BlazingTable> getPartitionPlan(const Context & context) {
	using ral::communication::messages::PartitionPivotsMessage;
	using ral::communication::network::Server;

	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = PartitionPivotsMessage::MessageID() + "_" + std::to_string(context_comm_token);

	auto message = Server::getInstance().getMessage(context_token, message_id);

	if(message->getMessageTokenValue() != message_id) {
		throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
	}

	auto concreteMessage = std::static_pointer_cast<PartitionPivotsMessage>(message);

	// WSM TODO waiting on message
	// return concreteMessage->getBlazingTable();
}

// std::vector<NodeColumn> partitionData(const Context & context,
// 											BlazingTableView & table,
// 											std::vector<int> & searchColIndices,
// 											BlazingTableView & pivots,
// 											bool isTableSorted,
// 											std::vector<int8_t> sortOrderTypes) {

// 	// verify input
// 	if(pivots.view().num_columns() == 0) {
// 		throw std::runtime_error("The pivots array is empty");
// 	}
// 	if(pivots.view().num_columns() != searchColIndices.size()) {
// 		throw std::runtime_error("The pivots and searchColIndices vectors don't have the same size");
// 	}

// 	cudf::size_type num_rows = table.view().num_rows();
// 	if(num_rows == 0) {
// 		std::vector<NodeColumn> array_node_columns;
// 		auto nodes = context.getAllNodes();
// 		for(std::size_t i = 0; i < nodes.size(); ++i) {
// 			std::unique_ptr<CudfTable> cudfTable = std::make_unique<CudfTable>(table.view());
// 			array_node_columns.emplace_back(nodes[i], std::make_unique<BlazingTable>(cudfTable, table.names()));
// 		}
// 		return array_node_columns;
// 	}

// 	if(sortOrderTypes.size() == 0) {
// 		sortOrderTypes.assign(searchColIndices.size(), 0);
// 	}

// 	std::vector<bool> desc_flags(sortOrderTypes.begin(), sortOrderTypes.end());


// 	// Ensure data is sorted.
// 	// Would it be better to use gdf_hash instead or gdf_order_by?
// 	std::vector<gdf_column_cpp> sortedTable;
// 	if(!isTableSorted) {
// 		std::vector<cudf::column *> key_cols_vect(searchColIndices.size());
// 		std::transform(searchColIndices.cbegin(), searchColIndices.cend(), key_cols_vect.begin(), [&](const int index) {
// 			return table[index].get_gdf_column();
// 		});

// 		gdf_column_cpp asc_desc_col;
// 		asc_desc_col.create_gdf_column(cudf::type_id::INT8,
// 			sortOrderTypes.size(),
// 			sortOrderTypes.data(),
// 			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT8),
// 			"");

// 		gdf_column_cpp index_col;
// 		index_col.create_gdf_column(cudf::type_id::INT32,
// 			table_row_size,
// 			nullptr,
// 			ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
// 			"");

// 		gdf_context gdfcontext;
// 		gdfcontext.flag_null_sort_behavior = GDF_NULL_AS_LARGEST;  // Nulls are are treated as largest

// 		// TODO percy cudf0.12 port to cudf::column
// //		CUDF_CALL(gdf_order_by(key_cols_vect.data(),
// //			(int8_t *) (asc_desc_col.get_gdf_column()->data),
// //			key_cols_vect.size(),
// //			index_col.get_gdf_column(),
// //			&gdfcontext));

// 		sortedTable.resize(table.size());
// 		for(size_t i = 0; i < sortedTable.size(); i++) {
// 			auto & col = table[i];
// 			if(col.get_gdf_column()->has_nulls()) {
// 				sortedTable[i].create_gdf_column(col.get_gdf_column()->type().id(),
// 					col.get_gdf_column()->size(),
// 					nullptr,
// 					ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
// 					col.name());
// 			} else {
// 				sortedTable[i].create_gdf_column(col.get_gdf_column()->type().id(),
// 					col.get_gdf_column()->size(),
// 					nullptr,
// 					nullptr,
// 					ral::traits::get_dtype_size_in_bytes(col.get_gdf_column()->type().id()),
// 					col.name());
// 			}

// 			materialize_column(table[i].get_gdf_column(), sortedTable[i].get_gdf_column(), index_col.get_gdf_column());
			
// 			// TODO percy cudf0.12 port to cudf::column
// 			//sortedTable[i].update_null_count();
// 		}

// 		table = sortedTable;
// 	}

// 	std::vector<gdf_column_cpp> sync_haystack(searchColIndices.size());
// 	std::transform(searchColIndices.cbegin(), searchColIndices.cend(), sync_haystack.begin(), [&](const int index) {
// 		return table[index];
// 	});
// 	std::vector<gdf_column_cpp> sync_needles(pivots);
// 	for(cudf::size_type i = 0; i < sync_haystack.size(); i++) {
// 		gdf_column_cpp & left_col = sync_haystack[i];
// 		gdf_column_cpp & right_col = sync_needles[i];

// 		if(left_col.get_gdf_column()->type().id() != GDF_STRING_CATEGORY) {
// 			continue;
// 		}

// 		// Sync the nvcategories to make them comparable
// 		// Workaround for https://github.com/rapidsai/cudf/issues/2790

// 		gdf_column_cpp new_left_column;
// 		new_left_column.allocate_like(left_col);
// 		cudf::column * new_left_column_ptr = new_left_column.get_gdf_column();
// 		gdf_column_cpp new_right_column;
// 		new_right_column.allocate_like(right_col);
// 		cudf::column * new_right_column_ptr = new_right_column.get_gdf_column();

// 		if(left_col.get_gdf_column()->has_nulls()) {
// 			// TODO percy cudf0.12 port to cudf::column
// //			CUDA_TRY(cudaMemcpy(new_left_column_ptr->valid,
// //				left_col.valid(),
// //				gdf_valid_allocation_size(new_left_column_ptr->size),
// //				cudaMemcpyDefault));
// //			new_left_column_ptr->null_count = left_col.null_count();
// 		}

// 		if(right_col.get_gdf_column()->has_nulls()) {
// 			// TODO percy cudf0.12 port to cudf::column
// //			CUDA_TRY(cudaMemcpy(new_right_column_ptr->valid,
// //				right_col.valid(),
// //				gdf_valid_allocation_size(new_right_column_ptr->size),
// //				cudaMemcpyDefault));
// //			new_right_column_ptr->null_count = right_col.null_count();
// 		}

// 		// TODO percy cudf0.12 port to cudf::column
// //		cudf::column * tmp_arr_input[2] = {left_col.get_gdf_column(), right_col.get_gdf_column()};
// //		cudf::column * tmp_arr_output[2] = {new_left_column_ptr, new_right_column_ptr};
// //		CUDF_TRY(sync_column_categories(tmp_arr_input, tmp_arr_output, 2));

// 		sync_haystack[i] = new_left_column;
// 		sync_needles[i] = new_right_column;
// 	}

// 	cudf::table haystack_table = ral::utilities::create_table(sync_haystack);
// 	cudf::table needles_table = ral::utilities::create_table(sync_needles);

// 	// We want the raw_indexes be on the heap because indexes will call delete when it goes out of scope
// 	// TODO percy cudf0.12 port to cudf::column
// //	cudf::column * raw_indexes = new cudf::column{};
// //	*raw_indexes = cudf::upper_bound(haystack_table, needles_table, desc_flags,
// //		true);  // nulls_as_largest
// //	gdf_column_cpp indexes;
// //	indexes.create_gdf_column(raw_indexes);
// //	sort_indices(indexes);
// //	return split_data_into_NodeColumns(context, table, indexes);
// }

 	void distributePartitions(const Context & context, std::vector<NodeColumnView> & partitions) {
 	using ral::communication::experimental::CommunicationData;
 	using ral::communication::messages::ColumnDataMessage;
 	using ral::communication::messages::Factory;
 	using ral::communication::network::Client;

 	const uint32_t context_comm_token = context.getContextCommunicationToken();
 	const uint32_t context_token = context.getContextToken();
 	const std::string message_id = ColumnDataMessage::MessageID() + "_" + std::to_string(context_comm_token);

 	auto self_node = CommunicationData::getInstance().getSelfNode();
 	std::vector<std::thread> threads;
 	for(auto & nodeColumn : partitions) {
 		if(nodeColumn.first == self_node) {
 			continue;
 		}
 		BlazingTableView columns = nodeColumn.second;
 		auto destination_node = nodeColumn.first;
 		//TODO WSM waiting on Factory::createColumnDataMessage
 		// threads.push_back(std::thread([message_id, context_token, self_node, destination_node, columns]() mutable {
 		// 	auto message = Factory::createColumnDataMessage(message_id, context_token, self_node, columns);
 		// 	Client::send(destination_node, *message);
 		// }));
 	}
 	for(size_t i = 0; i < threads.size(); i++) {
 		threads[i].join();
 	}
 }

std::vector<NodeColumn> collectPartitions(const Context & context) {
	int num_partitions = context.getTotalNodes() - 1;
	return collectSomePartitions(context, num_partitions);
}

std::vector<NodeColumn> collectSomePartitions(const Context & context, int num_partitions) {
	using ral::communication::messages::ColumnDataMessage;
	using ral::communication::network::Server;

	// Get the numbers of rals in the query
	int number_rals = context.getTotalNodes() - 1;
	std::vector<bool> received(context.getTotalNodes(), false);

	// Create return value
	std::vector<NodeColumn> node_columns;

	// Get message from the server
	const uint32_t context_comm_token = context.getContextCommunicationToken();
	const uint32_t context_token = context.getContextToken();
	const std::string message_id = ColumnDataMessage::MessageID() + "_" + std::to_string(context_comm_token);

	while(0 < num_partitions) {
		auto message = Server::getInstance().getMessage(context_token, message_id);
		num_partitions--;

		if(message->getMessageTokenValue() != message_id) {
			throw createMessageMismatchException(__FUNCTION__, message_id, message->getMessageTokenValue());
		}

		auto column_message = std::static_pointer_cast<ColumnDataMessage>(message);
		// WSM TODO waiting on message Node refactor
		// auto node = message->getSenderNode();
		// int node_idx = context.getNodeIndex(node);
		// if(received[node_idx]) {
		// 	Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(context_token),
		// 		std::to_string(context.getQueryStep()),
		// 		std::to_string(context.getQuerySubstep()),
		// 		"ERROR: Already received collectSomePartitions from node " + std::to_string(node_idx)));
		// }
		// WSM waiting on getBlazingTable
		// node_columns.emplace_back(std::make_pair(node, std::move(column_message->getBlazingTable())));
		// received[node_idx] = true;
	}
	return node_columns;
}

void scatterData(const Context & context, BlazingTableView table) {
	using ral::communication::experimental::CommunicationData;

	std::vector<NodeColumnView> node_columns;
	auto nodes = context.getAllNodes();
	for(std::size_t i = 0; i < nodes.size(); ++i) {
		if(!(nodes[i] == CommunicationData::getInstance().getSelfNode())) {
			node_columns.emplace_back(nodes[i], table);
		}
	}
	distributePartitions(context, node_columns);
}

}  // namespace experimental
}  // namespace distribution
}  // namespace ral