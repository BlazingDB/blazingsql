Kernels
=======

Every step in the physical plan maps to exactly one kernel. Kernels Organize logically the transformations that are going to be performed on a distributed dataframe.

They operate in parallel both to other kernels on the same node, and to its counterpart on other nodes. 
Some kernels are able to operate on every partition of data as it comes. As in the case of something like a Project or ComputeAggregate kernel. 
Some of them have to wait for certain conditions to be met like the MergeAggregate or the SortAndSampleKernel. 
Put together kernels can be used to string together scalable, distributed, and resource exhaustion tolerant transformations on data.

Complex Logical operations are broken down into smaller parts. An aggregation is actually comprised of three kernels for example. 
One that performs distributed reductions (ComputeAggregateKernel) another that performs the scattering of certain partitions of the 
reduced data to other nodes (PartitionKernel), and one that combines those multiple reductions (MergeAggregateKernel).


Base Kernel
------------

The Kernel base class, :blazing_repo:`found here</engine/src/execution_graph/logic_controllers/taskflow/kernel.h>`, 
provides many of the functionalities which are common to all Kernels. 

For example: All kernels have an input port and an output port. Each of which
contains a map of named CacheMachines. A kernel might write to multiple outputs
and may receive input from multiple inputs but they are all contained within
the input and output ports.

Only in the TableScan and BindableTableScan kernels are the input ports not defined.
In these two cases the kernels themselves generate data either by passing
through a cudf or by reading files.

The base kernel also provides mechanisms for tracking the state of tasks which were dispatched by the kernel to the executor.


Non Distributing Kernels
------------------------
* `BindableTableScan <api/classral_1_1batch_1_1BindableTableScan.html>`_
* `ComputeAggregateKernel <api/classral_1_1batch_1_1ComputeAggregateKernel.html>`_
* `ComputeWindowKernel <api/classral_1_1batch_1_1ComputeAggregateKernel.html>`_
* `Filter <api/classral_1_1batch_1_1Filter.html>`_
* `MergeAggregateKernel <api/classral_1_1batch_1_1MergeAggregateKernel.html>`_
* `MergeStreamKernel <api/classral_1_1batch_1_1MergeStreamKernel.html>`_
* `OutputKernel <api/classral_1_1batch_1_1OutputKernel.html>`_
* `PartitionSingleNodeKernel <api/classral_1_1batch_1_1PartitionSingleNodeKernel.html>`_
* `PartwiseJoin <api/classral_1_1batch_1_1PartwiseJoin.html>`_
* `Print <api/classral_1_1batch_1_1PartitionSingleNodeKernel.html>`_
* `Projection <api/classral_1_1batch_1_1Projection.html>`_
* `TableScan <api/classral_1_1batch_1_1TableScan.html>`_
* `UnionKernel <api/classral_1_1batch_1_1UnionKernel.html>`_


Distributing kernels
--------------------

Kernels that distribute information between different nodes have a series of helper functions that it allow it to push dataframes or portions of dataframes to other nodes. It has no expectations about how those messages will be distributed and all it does is push these dataframes into a cache with routing information.

* `DistributeAggregateKernel <api/classral_1_1batch_1_1DistributeAggregateKernel.html>`_
* `JoinPartitionKernel <api/classral_1_1batch_1_1JoinPartitionKernel.html>`_
* `LimitKernel <api/classral_1_1batch_1_1LimitKernel.html>`_
* `PartitionKernel <api/classral_1_1batch_1_1PartitionKernel.html>`_
* `SortAndSampleKernel <api/classral_1_1batch_1_1SortAndSampleKernel.html>`_


Implementing a Basic Kernel
---------------------------

A Kernel has two functions that must be implemented for it to be operational. A `run() <api/classral_1_1cache_1_1kernel.html#classral_1_1cache_1_1kernel_1a735b081cccae9574924e74ea6d293ef7>`_ function that takes no parameters and a `do_process() <api/classral_1_1cache_1_1kernel.html#classral_1_1cache_1_1kernel_1aa8d19c5f112f8965ea2f9999fb5fd625>`_ function. Below we are going to go over an example of a simple kernel and how these two functions are implemented.

In addition to this there are other functions that if implemented will allow the engine to be more judicious in how it schedules work. Examples of this are things like estimate_output_bytes() and estimate_operating_bytes() lets the engine be able to estimate how much memory it will need for either storing the output or will need as temporary space to perform this kernels operation on a specified input.

Constructor
^^^^^^^^^^^^

The constructors for kernels are usually trivial to implement. The parameter queryString is the relational algebra snippet, in this case the predicate,  
which is currently being evaluated. In the contructor we also usually perform any pre-processing to the queryString as necessary to obtain any parameters we
use in the kernel.

.. code-block:: cpp

    Filter::Filter(std::size_t kernel_id,
     const std::string & queryString,
     std::shared_ptr<Context> context,
     std::shared_ptr<ral::cache::graph> query_graph)
    : kernel(kernel_id, queryString, context, kernel_type::FilterKernel)
    {
        this->query_graph = query_graph;
    }


Run Function
^^^^^^^^^^^^

Most run functions are implemented relatively trivially. In general they pull one or more CacheData inputs from an input CacheMachine and use them to create a task 
along with the output cache destination, and the kernel itself as parameters. For other kernels, the run function will need to wait until certain messages are 
received from other nodes or other conditions before tasks are created. But ultimatelly the purpose of the run function is to handle the 
orchestration and business logic necessary for creating execution tasks.

.. code-block:: cpp

    kstatus Filter::run() {
        CodeTimer timer;

        std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
        while(cache_data != nullptr){
            std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
            inputs.push_back(std::move(cache_data));

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    this->output_cache(),
                    this);

            cache_data = this->input_cache()->pullCacheData();
        }

        if(logger){
            logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="Filter Kernel tasks created",
                                        "duration"_a=timer.elapsed_time(),
                                        "kernel_id"_a=this->get_id());
        }

        std::unique_lock<std::mutex> lock(kernel_mutex);
        kernel_cv.wait(lock,[this]{
            return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
        });

        if(auto ep = ral::execution::executor::get_instance()->last_exception()){
            std::rethrow_exception(ep);
        }

        if(logger) {
            logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="Filter Kernel Completed",
                                        "duration"_a=timer.elapsed_time(),
                                        "kernel_id"_a=this->get_id());
        }

        return kstatus::proceed;
    }

do_process Function
^^^^^^^^^^^^^^^^^^^
	
The do_process function is what gets run by a task. It is invoked by the :doc:`Task Executor <executor>` after the kernel has submitted a task for execution. 

The do_process function also has mechanisms that allow for out of memory error recovery. If in the execution of a do_process function call, there is an OOM error,
the do_process function, when possible, will take the inputs and return them back to the caller (the Task Executor), so that it can recreate the task to try again later.
In the case of a filter kernel we can recover from out of memory errors because the input into this kernel is not modified. 
This is the case for almost every kernel. This function always receives an array because some kernels operate on more than one dataframe at a time like Union or PartwiseJoin. 
This function returns a struct with a status, an error message, and the inputs to it in case it failed.

Some kernels need to be able to have different types of tasks, or take in different types of inputs. This is handled by the key value args map. The key value args map
can have a parameter "operation_type" which will define which type of task is is.

At some point we need to add a way to be able to have different do_process 
functions to be able to target different backends. 
	
	.. code-block:: cpp
	
		ral::execution::task_result Filter::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
			std::shared_ptr<ral::cache::CacheMachine> output,
			cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
	
			std::unique_ptr<ral::frame::BlazingTable> columns;
			try{
				//Get the input we are working on
				auto & input = inputs[0];
	
				//Perform manipulations and end up with a Dataframe
				columns = ral::processor::process_filter(input->toBlazingTableView(), expression, this->context.get());
	
				//Write the output of that dataframe to the output cache.
				output->addToCache(std::move(columns));
			}catch(const rmm::bad_alloc& e){
				//If we ran out of memory we can retry returning the inputs to the caller
				return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
			}catch(const std::exception& e){
	
				return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
			}
	
			return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
		}
	

Estimation Functions
^^^^^^^^^^^^^^^^^^^^

These functions are used so that a kernel can generate an estimates of things like their output size, how much data in total it should process, 
an estimate for how much overhead is needed to process a transformation on an input of a given size. Below we show the function used to estimate 
the number of output rows it will generate in total during execution. It gets an estimate from its input of how many rows it expects to receive
and then multiples this by how much it has filtered out in the previous executions. If no data has yet to be filtered it returns 0 with an indicator 
that the estimate isn't valid yet.

.. code-block:: cpp

    std::pair<bool, uint64_t> Filter::get_estimated_output_num_rows(){
        std::pair<bool, uint64_t> total_in = this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
        if (total_in.first){
            double out_so_far = (double)this->output_.total_bytes_added();
            double in_so_far = (double)this->total_input_bytes_processed;
            if (in_so_far == 0){
                return std::make_pair(false, 0);
            } else {
                return std::make_pair(true, (uint64_t)( ((double)total_in.second) *out_so_far/in_so_far) );
            }
        } else {
            return std::make_pair(false, 0);
        }
    }


Implementing a Distributed Kernel
---------------------------------

A distributing kernel implements a different interface which is inherited by the base kernel interface. It is implemented in much the same way a basic kernel 
is implemented but it has at its disposal certain utility functions. 
Here we will go over the JoinPartition kernel and how it leverages some of these utilities for execution.


do_process Function
^^^^^^^^^^^^^^^^^^^

Here is an example of a do_process function for a distributed kernel, in this case the JoinPartitionKernel. 
It calls distribution kernel primitives like ``broadcast`` and ``scatter`` to be able to send information to other nodes. 
In particular this is an example of the kinds of logical concerns which can often be seperated from execution concerns. 
Here the JoinPartitionKernel has no idea how it can scatter or broadcast information to other nodes but just uses those high level apis to do so. 

.. code-block:: cpp

    ral::execution::task_result JoinPartitionKernel::do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable>> inputs,
    	std::shared_ptr<ral::cache::CacheMachine> /*output*/,
    	cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {
    	bool input_consumed = false;
    	try{
    		auto& operation_type = args.at("operation_type");
    		auto & input = inputs[0];
    		if (operation_type == "small_table_scatter") {
    			input_consumed = true;
    			std::string small_output_cache_name = scatter_left_right.first ? "output_a" : "output_b";
    			int small_table_idx = scatter_left_right.first ? LEFT_TABLE_IDX : RIGHT_TABLE_IDX;

    			broadcast(std::move(input),
    				this->output_.get_cache(small_output_cache_name).get(),
    				"", //message_id_prefix
    				small_output_cache_name, //cache_id
    				small_table_idx //message_tracker_idx
    			);
    		} else if (operation_type == "hash_partition") {
    			bool normalize_types;
    			int table_idx;
    			std::string cache_id;
    			std::vector<cudf::size_type> column_indices;
    			if(args.at("side") == "left"){
    				normalize_types = this->normalize_left;
    				table_idx = LEFT_TABLE_IDX;
    				cache_id = "output_a";
    				column_indices = this->left_column_indices;
    			} else {
    				normalize_types = this->normalize_right;
    				table_idx = RIGHT_TABLE_IDX;
    				cache_id = "output_b";
    				column_indices = this->right_column_indices;
    			}

    			if (normalize_types) {
    				ral::utilities::normalize_types(input, join_column_common_types, column_indices);
    			}

    			auto batch_view = input->view();
    			std::unique_ptr<cudf::table> hashed_data;
    			std::vector<cudf::table_view> partitioned;
    			if (input->num_rows() > 0) {
    				// When is cross_join. `column_indices` is equal to 0, so we need all `batch` columns to apply cudf::hash_partition correctly
    				if (column_indices.size() == 0) {
    					column_indices.resize(input->num_columns());
    					std::iota(std::begin(column_indices), std::end(column_indices), 0);
    				}

    				int num_partitions = context->getTotalNodes();
    				std::vector<cudf::size_type> hased_data_offsets;
    				std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(batch_view, column_indices, num_partitions);
    				assert(hased_data_offsets.begin() != hased_data_offsets.end());

    				// the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
    				std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
    				partitioned = cudf::split(hashed_data->view(), split_indexes);
    			} else {
    				for(int i = 0; i < context->getTotalNodes(); i++){
    					partitioned.push_back(batch_view);
    				}
    			}

    			std::vector<ral::frame::BlazingTableView> partitions;
    			for(auto partition : partitioned) {
    				partitions.push_back(ral::frame::BlazingTableView(partition, input->names()));
    			}

    			scatter(partitions,
    				this->output_.get_cache(cache_id).get(),
    				"", //message_id_prefix
    				cache_id, //cache_id
    				table_idx  //message_tracker_idx
    			);
    		} else { // not an option! error
    			if (logger) {
    				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
    											"query_id"_a=context->getContextToken(),
    											"step"_a=context->getQueryStep(),
    											"substep"_a=context->getQuerySubstep(),
    											"info"_a="In JoinPartitionKernel::do_process Invalid operation_type: {}"_format(operation_type),
    											"duration"_a="");
    			}

    			return {ral::execution::task_status::FAIL, std::string("In JoinPartitionKernel::do_process Invalid operation_type"), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    		}
    	}catch(const rmm::bad_alloc& e){
    		return {ral::execution::task_status::RETRY, std::string(e.what()), input_consumed ? std::vector< std::unique_ptr<ral::frame::BlazingTable> > () : std::move(inputs)};
    	}catch(const std::exception& e){
    		return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    	}
    	return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }



Limitations of Current Approach
-------------------------------
* Kernels need to be able to target different backends
* Many kernels still use strings for transferring plan information
