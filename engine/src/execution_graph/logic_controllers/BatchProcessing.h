#pragma once

#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "TaskFlowProcessor.h"
#include "io/DataLoader.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"


namespace ral {
namespace batch {

using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;

class BatchSequence {
public:
	BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache)
	: cache{cache}
	{}

	RecordBatch next() {
		return cache->pullFromCache();
	}
	bool has_next() {
		return not cache->is_finished();
	}
private:
	std::shared_ptr<ral::cache::CacheMachine> cache;
};


class DataSourceSequence {
public:
	DataSourceSequence(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
		: context(context), loader(loader), schema(schema), batch_index{0}, file_index{0}, batch_id{0}
	{
		// n_partitions{n_partitions}: TODO Update n_batches using data_loader
		this->provider = loader.get_provider();
		this->parser = loader.get_parser();

		// iterates through files and parses them into columns
		while(this->provider->has_next()) {
			// a file handle that we can use in case errors occur to tell the user which file had parsing issues
			user_readable_file_handles.push_back(this->provider->get_current_user_readable_file_handle());
			files.push_back(this->provider->get_next());
		}
		n_files = files.size();
		for (size_t index = 0; index < n_files; index++) {
			ral::io::Schema fileSchema = schema.fileSchema(file_index);
			std::vector<int> row_groups = fileSchema.get_rowgroup_ids(0);
			n_batches += row_groups.size();
			all_row_groups.push_back(row_groups);
		}
		

	}
	RecordBatch next() {
		// std::cout << "Datasource.next: " << file_index << "|" << batch_id << "|" << all_row_groups[file_index].size() << std::endl;
		auto ret = loader.load_batch(context.get(), {}, schema, user_readable_file_handles[file_index], files[file_index], file_index, batch_id);
		batch_index++;
		
		batch_id++;
		if (batch_id == all_row_groups[file_index].size()) {
			file_index++;
			batch_id = 0;
		}
		return std::move(ret);
	}
	bool has_next() {
		return file_index < n_files and batch_index < n_batches;
	}

private:
	std::shared_ptr<ral::io::data_provider> provider;
	std::shared_ptr<ral::io::data_parser> parser;
	std::vector<std::string> user_readable_file_handles;
	std::vector<ral::io::data_handle> files;

	std::shared_ptr<Context> context;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
	size_t file_index;
	size_t batch_index;
	size_t batch_id;
	size_t n_batches;
	size_t n_files;
	std::vector<std::vector<int>> all_row_groups; 
};

struct PhysicalPlan : kernel {
	virtual kstatus run() = 0;

	std::shared_ptr<ral::cache::CacheMachine>  input_cache() {
		auto kernel_id = std::to_string(this->get_id());
		return this->input_.get_cache(kernel_id);
	}
	std::shared_ptr<ral::cache::CacheMachine>  output_cache() {
		auto kernel_id = std::to_string(this->get_id());
		return this->output_.get_cache(kernel_id);
	}
	size_t n_batches() {
		return n_batches_; // TODO: use set_n_batches(n_batches) in make_kernel
	}
private:
	size_t n_batches_;
};

class TableScan : public PhysicalPlan {
public:
	TableScan(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
	: PhysicalPlan(), input(loader, schema, context)
	{}
	virtual kstatus run() {
		while( input.has_next() ) {
			auto batch = input.next();
			this->output_cache()->addToCache(std::move(batch));
		}
		return kstatus::proceed;
	}
private:
	DataSourceSequence input;
};

class Projection : public PhysicalPlan {
public:
	Projection(const std::string & queryString, std::shared_ptr<Context> context)
	{
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		BatchSequence input(this->input_cache());
		while (input.has_next() ) {
			auto batch = input.next();
			auto columns = ral::processor::process_project(std::move(batch), expression, context.get());
			this->output_cache()->addToCache(std::move(columns));
		}
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class Filter : public PhysicalPlan {
public:
	Filter(const std::string & queryString, std::shared_ptr<Context> context)
	{
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		BatchSequence input(this->input_cache());
		while (input.has_next() ) {
			auto batch = input.next();
			auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, context.get());
			this->output_cache()->addToCache(std::move(columns));
		}
		return kstatus::proceed;
	}
private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class Print : public PhysicalPlan {
public:
	Print() : PhysicalPlan() { ofs = &(std::cout); }
	Print(std::ostream & stream) : PhysicalPlan() { ofs = &stream; }
	virtual kstatus run() {
		std::lock_guard<std::mutex> lg(print_lock);
		BatchSequence input(this->input_cache());
		while (input.has_next() ) {
			auto batch = input.next();
			ral::utilities::print_blazing_table_view(batch->toBlazingTableView());
		}
		return kstatus::stop;
	}

protected:
	std::ostream * ofs = nullptr;
	std::mutex print_lock;
};

class SortAndSampleSingleNodeKernel : public PhysicalPlan {
public:
	SortAndSampleSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context}
	{
		this->output_.add_port("output_a", "output_b");
	}
	
	virtual kstatus run() {
		BatchSequence input(this->input_cache());
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
		std::vector<ral::frame::BlazingTableView> sampledTableViews;
		std::vector<size_t> tableTotalRows;
		size_t total_num_rows = 0;
		while (input.has_next()) {
			auto batch = input.next();
			auto sortedTable = ral::operators::experimental::sort(batch->toBlazingTableView(), this->expression);
			auto sampledTable = ral::operators::experimental::sample(batch->toBlazingTableView(), this->expression);
			sampledTableViews.push_back(sampledTable->toBlazingTableView());
			sampledTables.push_back(std::move(sampledTable));
			tableTotalRows.push_back(batch->view().num_rows());
			this->output_.get_cache("output_a")->addToCache(std::move(sortedTable));
		}
		// call total_num_partitions = partition_function(size_of_all_data, number_of_nodes, avaiable_memory, ....)
		auto partitionPlan = ral::operators::experimental::generate_partition_plan(10 /*how many?*/, sampledTableViews, tableTotalRows, this->expression);
		this->output_.get_cache("output_b")->addToCache(std::move(partitionPlan));
		
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class PartitionSingleNodeKernel : public PhysicalPlan {
public:
	PartitionSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
		this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
		BatchSequence input_partitionPlan(this->input_.get_cache("input_b"));
		auto partitionPlan = std::move(input_partitionPlan.next());
		
		BatchSequence input(this->input_.get_cache("input_a"));
		while (input.has_next()) {
			auto batch = input.next();
			
			auto partitions = ral::operators::experimental::partition_table(partitionPlan->toBlazingTableView(), batch->toBlazingTableView(), this->expression);
			
//			for(auto& partition : partitions)
//				ral::utilities::print_blazing_table_view(partition->toBlazingTableView());

			for (auto i = 0; i < partitions.size(); i++) {
				std::string cache_id = "output_" + std::to_string(i);
				this->output_[cache_id]->addToCache(
					std::make_unique<ral::frame::BlazingTable>(
						std::make_unique<cudf::experimental::table>(partitions[i]),
						batch->names())
					);
			}
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class SortAndSampleKernel : public PhysicalPlan {
public:
	SortAndSampleKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context}
	{
		this->output_.add_port("output_a", "output_b");
	}
	
	virtual kstatus run() {
		BatchSequence input(this->input_cache());
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
		std::vector<ral::frame::BlazingTableView> sampledTableViews;
		size_t total_num_rows = 0;
		while (input.has_next()) {
			auto batch = input.next();
			auto sortedTable = ral::operators::experimental::sort(batch->toBlazingTableView(), this->expression);
			if(context->getTotalNodes() > 1) {
				auto sampledTable = ral::operators::experimental::sample(batch->toBlazingTableView(), this->expression);
				sampledTableViews.push_back(sampledTable->toBlazingTableView());
				sampledTables.push_back(std::move(sampledTable));
				total_num_rows += batch->view().num_rows();
			}
			this->output_.get_cache("output_a")->addToCache(std::move(sortedTable));
		}

		std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
		if (context->getTotalNodes() > 1) {
			auto concatSamples = ral::utilities::experimental::concatTables(sampledTableViews);
			partitionPlan = ral::operators::experimental::generate_distributed_partition_plan(concatSamples->toBlazingTableView(), total_num_rows, this->expression, this->context.get());
		}
		this->output_.get_cache("output_b")->addToCache(std::move(partitionPlan));
		
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class PartitionKernel : public PhysicalPlan {
public:
	PartitionKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
		this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
		BatchSequence input_partitionPlan(this->input_.get_cache("input_b"));
		std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
		if (context->getTotalNodes() > 1) {
			partitionPlan = std::move(input_partitionPlan.next());
		}
		
		BatchSequence input(this->input_.get_cache("input_a"));
		while (input.has_next()) {
			auto batch = input.next();
			
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions;
			if (context->getTotalNodes() > 1) {
				partitions = ral::operators::experimental::partition_sort(partitionPlan->toBlazingTableView(), batch->toBlazingTableView(), this->expression, this->context.get());
			} else {
				partitions.push_back(std::move(batch));
			}
			
//			for(auto& partition : partitions)
//				ral::utilities::print_blazing_table_view(partition->toBlazingTableView());

			std::vector<std::unique_ptr<ral::frame::BlazingTable>> samples;
			std::vector<ral::frame::BlazingTableView> sample_views;
			std::vector<size_t> total_rows_tables;
			for (auto i = 0; i < partitions.size(); i++) {
				auto part_samples = ral::operators::experimental::sample(partitions[i]->toBlazingTableView(), this->expression);
				sample_views.push_back(part_samples->toBlazingTableView());
				samples.push_back(std::move(part_samples));
				total_rows_tables.push_back(partitions[i]->view().num_rows());
			}

			auto pivots = ral::operators::experimental::generate_partition_plan(3 /*how many?*/, sample_views, total_rows_tables, this->expression);

//			std::cout<< "PIVOTS: " <<std::endl;
//			for (auto &&c : pivots->view())
//			{
//				cudf::test::print(c);
//				std::cout << std::endl;
//			}

			for (auto i = 0; i < partitions.size(); i++) {
				auto partitioned_data = ral::operators::experimental::partition_table(pivots->toBlazingTableView(), partitions[i]->toBlazingTableView(), this->expression);

				std::string cache_id = "output_" + std::to_string(i);
				for (auto &&subpartition : partitioned_data) {
					this->output_[cache_id]->addToCache(
						std::make_unique<ral::frame::BlazingTable>(
							std::make_unique<cudf::experimental::table>(subpartition),
							partitions[i]->names())
						);
				}
			}
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class MergeStreamKernel : public PhysicalPlan {
public:
	MergeStreamKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context}  {
	}
	
	virtual kstatus run() {
		while (!this->input_.get_cache("input_0")->is_finished())
		{
			std::vector<ral::frame::BlazingTableView> partitions_to_merge;
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions_to_merge_holder;
			for (size_t index = 0; index < this->input_.count(); index++) {
				auto cache_id = "input_" + std::to_string(index);
				if (not this->input_.get_cache(cache_id)->is_finished()) {
					auto input = std::move(this->input_.get_cache(cache_id)->pullFromCache());
					if (input) {
						partitions_to_merge.emplace_back(input->toBlazingTableView());
						partitions_to_merge_holder.emplace_back(std::move(input));
					}
				}
			}

			if (partitions_to_merge.empty()) {
				// noop
			} else if(partitions_to_merge.size() == 1) {
				this->output_.get_cache()->addToCache(std::move(partitions_to_merge_holder.front()));
			}	else {
				for (auto view : partitions_to_merge)
					ral::utilities::print_blazing_table_view(view);

				auto output = ral::operators::experimental::merge(partitions_to_merge, this->expression, this->context.get());

//					ral::utilities::print_blazing_table_view(output->toBlazingTableView());

				this->output_.get_cache()->addToCache(std::move(output));
			}
		}
		
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};
 
struct tree_processor {
	struct node {
		std::string expr;               // expr
		int level;                      // level
		std::shared_ptr<kernel>            kernel_unit;
		std::vector<std::shared_ptr<node>> children;  // children nodes

	} root;
	std::shared_ptr<Context> context;
	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::vector<std::string> table_names;
	const bool transform_operators_bigger_than_gpu = false;

	std::shared_ptr<kernel> make_kernel(std::string expr) {
		std::shared_ptr<kernel> k;
		auto kernel_context = this->context->clone();
		if ( is_project(expr) ) {
			k = std::make_shared<Projection>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::ProjectKernel);
		} else if ( is_filter(expr) ) {
			k = std::make_shared<Filter>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::FilterKernel);
		}	else if ( is_logical_scan(expr) ) {
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
			auto schema = this->schemas[table_index];
			k = std::make_shared<TableScan>(*loader, schema, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::TableScanKernel);
		} else if (is_bindable_scan(expr)) {
//			size_t table_index = get_table_index(table_names, extract_table_name(expr));
//			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
//			auto schema = this->schemas[table_index];
//			k = std::make_shared<BindableTableScanKernel>(expr, *loader, schema, kernel_context);
//			kernel_context->setKernelId(k->get_id());
//			k->set_type_id(kernel_type::BindableTableScanKernel);
		} else if (is_single_node_partition(expr)) {
			k = std::make_shared<PartitionSingleNodeKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::PartitionSingleNodeKernel);
		} else if (is_single_node_sort_and_sample(expr)) {
			k = std::make_shared<SortAndSampleSingleNodeKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortAndSampleSingleNodeKernel);
		} else if (is_merge(expr)) {
			k = std::make_shared<MergeStreamKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::MergeStreamKernel);
		}
		
		return k;
	}
	void expr_tree_from_json(boost::property_tree::ptree const& p_tree, node * root_ptr, int level) {
		auto expr = p_tree.get<std::string>("expr", "");
		// for(int i = 0; i < level*2 ; ++i) {
		// 	std::cout << " ";
		// }
		// std::cout << expr << std::endl;
		root_ptr->expr = expr;
		root_ptr->level = level;
		root_ptr->kernel_unit = make_kernel(expr);
		for (auto &child : p_tree.get_child("children")) {
			auto child_node_ptr = std::make_shared<node>();
			root_ptr->children.push_back(child_node_ptr);
			expr_tree_from_json(child.second, child_node_ptr.get(), level + 1);
		}
	}

	ral::cache::graph build_batch_graph(std::string json) {
		try {
			std::istringstream input(json);
			boost::property_tree::ptree p_tree;
			boost::property_tree::read_json(input, p_tree);
			expr_tree_from_json(p_tree, &this->root, 0);

		} catch (std::exception & e) {
			std::cerr << e.what() <<  std::endl;
		}


		ral::cache::graph graph;
		if (this->root.kernel_unit != nullptr) {
			graph.add_node(this->root.kernel_unit.get()); // register first node
			visit(graph, &this->root, this->root.children);
		}
		return graph;
	}

	void visit(ral::cache::graph& graph, node * parent, std::vector<std::shared_ptr<node>>& children) {
		for (size_t index = 0; index < children.size(); index++) {
			auto& child  =  children[index];
			visit(graph, child.get(), child->children);
			std::string port_name = "input";

			if (children.size() > 1) {
				char index_char = 'a' + index;
				port_name = std::string("input_");
				port_name.push_back(index_char);
				graph +=  *child->kernel_unit >> (*parent->kernel_unit)[port_name];
			} else {
				auto a = child->kernel_unit->get_type_id();
				auto b = parent->kernel_unit->get_type_id();
				graph +=  *child->kernel_unit >> (*parent->kernel_unit);
			}
		}
	}

};
 

} // namespace batch
} // namespace ral
