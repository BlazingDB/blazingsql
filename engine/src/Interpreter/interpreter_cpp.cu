#include <cudf/column/column_device_view.cuh>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_device_view.cuh>
#include <cudf/table/table_view.hpp>
#include <cudf/table/table_device_view.cuh>
#include <stack>
#include <map>
#include <regex>
#include <random>

#include "interpreter_ops.cuh"
#include "CalciteExpressionParsing.h"
#include "error.hpp"
#include <curand_kernel.h>

namespace interops {
namespace detail {

struct allocate_device_scalar {

	template <typename T, std::enable_if_t<not cudf::is_compound<T>()> * = nullptr>
	rmm::device_buffer operator()(cudf::scalar & s, cudaStream_t stream = 0) {
		using ScalarType = cudf::scalar_type_t<T>;
		using ScalarDeviceType = cudf::scalar_device_type_t<T>;

		rmm::device_buffer ret(sizeof(ScalarDeviceType), stream);

		auto typed_scalar_ptr = static_cast<ScalarType *>(&s);
		ScalarDeviceType h_scalar{typed_scalar_ptr->type(), typed_scalar_ptr->data(), typed_scalar_ptr->validity_data()};

    CUDA_TRY(cudaMemcpyAsync(ret.data(), &h_scalar, sizeof(ScalarDeviceType), cudaMemcpyDefault, stream));

		return std::move(ret);
	}

	template <typename T, std::enable_if_t<std::is_same<T, cudf::string_view>::value> * = nullptr>
	rmm::device_buffer operator()(cudf::scalar & s, cudaStream_t stream = 0) {
		using ScalarType = cudf::scalar_type_t<T>;
		using ScalarDeviceType = cudf::scalar_device_type_t<T>;

		rmm::device_buffer ret(sizeof(ScalarDeviceType), stream);

		auto typed_scalar_ptr = static_cast<ScalarType *>(&s);
		ScalarDeviceType h_scalar{typed_scalar_ptr->type(), typed_scalar_ptr->data(), typed_scalar_ptr->validity_data(), typed_scalar_ptr->size()};

		CUDA_TRY(cudaMemcpyAsync(ret.data(), &h_scalar, sizeof(ScalarDeviceType), cudaMemcpyDefault, stream));

		return std::move(ret);
	}

	template <typename T, std::enable_if_t<std::is_same<T, cudf::dictionary32>::value> * = nullptr>
	rmm::device_buffer operator()(cudf::scalar & s, cudaStream_t stream = 0) {
		RAL_FAIL("Dictionary not yet supported");
		return rmm::device_buffer{};
	}

	template <typename T, std::enable_if_t<std::is_same<T, cudf::list_view>::value> * = nullptr>
	rmm::device_buffer operator()(cudf::scalar & s, cudaStream_t stream = 0) {
		RAL_FAIL("List not yet supported");
		return rmm::device_buffer{};
	}
};

template <int SIZE, int REGISTER_SIZE>
int calculated_shared_memory(int num_threads_per_block) {
	return SIZE * num_threads_per_block * REGISTER_SIZE;
}

// TODO: we dont know if this is fast or not we coudl store this in a pre computed map
void calculate_grid(int * min_grid_size, int * block_size, column_index_type max_output) {
	if(max_output == 1) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<1, 8>, 0));
	} else if(max_output == 2) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<2, 8>, 0));
	} else if(max_output == 3) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<3, 8>, 0));
	} else if(max_output == 4) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<4, 8>, 0));
	} else if(max_output == 5) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<5, 8>, 0));
	} else if(max_output == 6) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<6, 8>, 0));
	} else if(max_output == 7) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<7, 8>, 0));
	} else if(max_output == 8) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<8, 8>, 0));
	} else if(max_output == 9) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<9, 8>, 0));
	} else if(max_output == 10) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<10, 8>, 0));
	} else if(max_output == 11) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<11, 8>, 0));
	} else if(max_output == 12) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<12, 8>, 0));
	} else if(max_output == 13) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<13, 8>, 0));
	} else if(max_output == 14) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<14, 8>, 0));
	} else if(max_output == 15) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<15, 8>, 0));
	} else if(max_output == 16) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<16, 8>, 0));
	} else if(max_output == 17) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<17, 8>, 0));
	} else if(max_output == 18) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<18, 8>, 0));
	} else if(max_output == 19) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<19, 8>, 0));
	} else if(max_output == 20) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<20, 8>, 0));
	} else if(max_output == 21) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<21, 8>, 0));
	} else if(max_output == 22) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<22, 8>, 0));
	} else if(max_output == 23) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<23, 8>, 0));
	} else if(max_output == 24) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<24, 8>, 0));
	} else if(max_output == 25) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<25, 8>, 0));
	} else if(max_output == 26) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<26, 8>, 0));
	} else if(max_output == 27) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<27, 8>, 0));
	} else if(max_output == 28) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<28, 8>, 0));
	} else if(max_output == 29) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<29, 8>, 0));
	} else if(max_output == 30) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<30, 8>, 0));
	} else if(max_output == 31) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<31, 8>, 0));
	} else if(max_output == 32) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<32, 8>, 0));
	} else if(max_output == 33) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<33, 8>, 0));
	} else if(max_output == 34) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<34, 8>, 0));
	} else if(max_output == 35) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<35, 8>, 0));
	} else if(max_output == 36) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<36, 8>, 0));
	} else if(max_output == 37) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<37, 8>, 0));
	} else if(max_output == 38) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<38, 8>, 0));
	} else if(max_output == 39) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<39, 8>, 0));
	} else if(max_output == 40) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<40, 8>, 0));
	} else if(max_output == 41) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<41, 8>, 0));
	} else if(max_output == 42) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<42, 8>, 0));
	} else if(max_output == 43) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<43, 8>, 0));
	} else if(max_output == 44) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<44, 8>, 0));
	} else if(max_output == 45) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<45, 8>, 0));
	} else if(max_output == 46) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<46, 8>, 0));
	} else if(max_output == 47) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<47, 8>, 0));
	} else if(max_output == 48) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<48, 8>, 0));
	} else if(max_output == 49) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<49, 8>, 0));
	} else if(max_output == 50) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<50, 8>, 0));
	} else if(max_output == 51) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<51, 8>, 0));
	} else if(max_output == 52) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<52, 8>, 0));
	} else if(max_output == 53) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<53, 8>, 0));
	} else if(max_output == 54) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<54, 8>, 0));
	} else if(max_output == 55) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<55, 8>, 0));
	} else if(max_output == 56) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<56, 8>, 0));
	} else if(max_output == 57) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<57, 8>, 0));
	} else if(max_output == 58) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<58, 8>, 0));
	} else if(max_output == 59) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<59, 8>, 0));
	} else if(max_output == 60) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<60, 8>, 0));
	} else if(max_output == 61) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<61, 8>, 0));
	} else if(max_output == 62) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<62, 8>, 0));
	} else if(max_output == 63) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<63, 8>, 0));
	} else if(max_output == 64) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<64, 8>, 0));
	}
}

}  // namespace detail

struct expr_to_plan_visitor : public ral::parser::node_visitor
{
public:
	expr_to_plan_visitor(const std::map<column_index_type, column_index_type> & expr_idx_to_col_idx_map,
											cudf::size_type start_processing_position,
											std::vector<column_index_type> & left_inputs,
											std::vector<column_index_type> & right_inputs,
											std::vector<column_index_type> & outputs,
											std::vector<operator_type> & operators,
											std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
											std::vector<std::unique_ptr<cudf::scalar>> & right_scalars)
		: expr_idx_to_col_idx_map{expr_idx_to_col_idx_map},
			start_processing_position{start_processing_position},
			left_inputs{left_inputs},
			right_inputs{right_inputs},
			outputs{outputs},
			operators{operators},
			left_scalars{left_scalars},
			right_scalars{right_scalars},
			processing_space_free_(512, true)
	{
		std::fill_n(processing_space_free_.begin(), start_processing_position, false);
	}

	void visit(const ral::parser::operad_node& node) override {
		column_index_type position;
		if (is_literal(node.value)) {
			position = SCALAR_INDEX;
		} else {
			position = expr_idx_to_col_idx_map.at(static_cast<const ral::parser::variable_node&>(node).index());
		}

		node_to_processing_position_.insert({&node, position});
	}

	void visit(const ral::parser::operator_node& node) override {
		operator_type operation = map_to_operator_type(node.value);
		operators.push_back(operation);
		if(is_binary_operator(operation)) {
			const ral::parser::node * left_operand = node.children[0].get();
			column_index_type left_position = node_to_processing_position_.at(left_operand);
			if(left_operand->type != ral::parser::node_type::LITERAL) {
				if(left_position >= start_processing_position) {
					processing_space_free_[left_position] = true;
				}
			}

			const ral::parser::node * right_operand = node.children[1].get();
			column_index_type right_position = node_to_processing_position_.at(right_operand);
			if(right_operand->type != ral::parser::node_type::LITERAL) {
				if(right_position >= start_processing_position) {
					processing_space_free_[right_position] = true;
				}
			}

			if(left_operand->type == ral::parser::node_type::LITERAL && right_operand->type == ral::parser::node_type::LITERAL) {
				RAL_FAIL("Operations between literals is not supported");
			} else if(left_operand->type == ral::parser::node_type::LITERAL) {
				auto literal_node = static_cast<const ral::parser::literal_node*>(left_operand);
				std::unique_ptr<cudf::scalar> scalar_ptr;
				if (!is_null(literal_node->value)) {
				 	scalar_ptr = get_scalar_from_string(literal_node->value, literal_node->type());
				}

				left_inputs.push_back(scalar_ptr ? SCALAR_INDEX : SCALAR_NULL_INDEX);
				right_inputs.push_back(right_position);
				left_scalars.push_back(std::move(scalar_ptr));
				right_scalars.emplace_back(nullptr);
			} else if(right_operand->type == ral::parser::node_type::LITERAL) {
				auto literal_node = static_cast<const ral::parser::literal_node*>(right_operand);
				std::unique_ptr<cudf::scalar> scalar_ptr;
				if (!is_null(literal_node->value)) {
					scalar_ptr = get_scalar_from_string(literal_node->value, literal_node->type());
				}

				left_inputs.push_back(left_position);
				right_inputs.push_back(scalar_ptr ? SCALAR_INDEX : SCALAR_NULL_INDEX);
				left_scalars.emplace_back(nullptr);
				right_scalars.push_back(std::move(scalar_ptr));
			} else {
				left_inputs.push_back(left_position);
				right_inputs.push_back(right_position);
				left_scalars.emplace_back(nullptr);
				right_scalars.emplace_back(nullptr);
			}
		} else if(is_unary_operator(operation)) {  
			const ral::parser::node * left_operand = node.children[0].get();
			RAL_EXPECTS(left_operand->type != ral::parser::node_type::LITERAL, "Unary operations on literals is not supported");

			column_index_type left_position = node_to_processing_position_.at(left_operand);
			if(left_position >= start_processing_position) {
				processing_space_free_[left_position] = true;
			}

			left_inputs.push_back(left_position);
			right_inputs.push_back(UNARY_INDEX);
			left_scalars.emplace_back(nullptr);
			right_scalars.emplace_back(nullptr);
		}else{

			left_inputs.push_back(NULLARY_INDEX);
			right_inputs.push_back(NULLARY_INDEX);
			left_scalars.emplace_back(nullptr);
			right_scalars.emplace_back(nullptr);
		}

		column_index_type position = get_first_open_position(start_processing_position);
		node_to_processing_position_.insert({&node, position});
		outputs.push_back(position);
	}

private:
	column_index_type get_first_open_position(cudf::size_type start_position) {
		assert(processing_space_free_.size() <= std::numeric_limits<column_index_type>().max());

		for(size_t i = start_position; i < processing_space_free_.size(); i++) {
			if(processing_space_free_[i]) {
				processing_space_free_[i] = false;
				return static_cast<column_index_type>(i);
			}
		}
		return -1;
	}

	std::vector<bool> processing_space_free_;  // A place to store whether or not a processing space is occupied at any point in time
	std::map<const ral::parser::node*, column_index_type> node_to_processing_position_;

	const std::map<column_index_type, column_index_type> & expr_idx_to_col_idx_map;
	cudf::size_type start_processing_position;

	std::vector<column_index_type> & left_inputs;
	std::vector<column_index_type> & right_inputs;
	std::vector<column_index_type> & outputs;
	std::vector<operator_type> & operators;
	std::vector<std::unique_ptr<cudf::scalar>> & left_scalars;
	std::vector<std::unique_ptr<cudf::scalar>> & right_scalars;
};

/**
 * Creates a physical plan for the expression that can be added to the total plan
 */
void add_expression_to_interpreter_plan(const ral::parser::parse_tree & expr_tree,
	const std::map<column_index_type, column_index_type> & expr_idx_to_col_idx_map,
	cudf::size_type start_processing_position,
	cudf::size_type final_output_position,
	std::vector<column_index_type> & left_inputs,
	std::vector<column_index_type> & right_inputs,
	std::vector<column_index_type> & outputs,
	std::vector<operator_type> & operators,
	std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	std::vector<std::unique_ptr<cudf::scalar>> & right_scalars) {

	expr_to_plan_visitor visitor{expr_idx_to_col_idx_map,
															start_processing_position,
															left_inputs,
															right_inputs,
															outputs,
															operators,
															left_scalars,
															right_scalars};
	expr_tree.visit(visitor);

	// Update final output position
	outputs.back() = final_output_position;
}

void perform_interpreter_operation(cudf::mutable_table_view & out_table,
	const cudf::table_view & table,
	const std::vector<column_index_type> & left_inputs,
	const std::vector<column_index_type> & right_inputs,
	const std::vector<column_index_type> & outputs,
	const std::vector<column_index_type> & final_output_positions,
	const std::vector<operator_type> & operators,
	const std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	const std::vector<std::unique_ptr<cudf::scalar>> & right_scalars,
	cudf::size_type operation_num_rows) {
	using namespace detail;
	cudaStream_t stream = 0;

	if (final_output_positions.empty())	{
		return;
	}

	assert(!left_inputs.empty());
	assert(!right_inputs.empty());
	assert(!outputs.empty());
	assert(!operators.empty());

	auto max_left_it = std::max_element(left_inputs.begin(), left_inputs.end());
	auto max_right_it = std::max_element(right_inputs.begin(), right_inputs.end());
	auto max_out_it = std::max_element(outputs.begin(), outputs.end());

	RAL_EXPECTS(std::max(std::max(*max_left_it, *max_right_it), *max_out_it) < 64, "Interops does not support plans with an input or output index greater than 63");

	column_index_type max_output = *max_out_it;

	size_t shared_memory_per_thread = (max_output + 1) * sizeof(int64_t);

	int min_grid_size, block_size;
	calculate_grid(&min_grid_size, &block_size, max_output + 1);

	size_t temp_valids_in_size = min_grid_size * block_size * table.num_columns() * sizeof(cudf::bitmask_type);
	size_t temp_valids_out_size = min_grid_size * block_size * final_output_positions.size() * sizeof(cudf::bitmask_type);
	rmm::device_buffer temp_device_valids_in_buffer(temp_valids_in_size, stream);
	rmm::device_buffer temp_device_valids_out_buffer(temp_valids_out_size, stream);

	// device table views
	auto device_table_view = cudf::table_device_view::create(table, stream);
	auto device_out_table_view = cudf::mutable_table_device_view::create(out_table, stream);

	// device scalar views
	std::vector<rmm::device_buffer> left_device_scalars_ptrs;
	std::vector<cudf::detail::scalar_device_view_base *> left_device_scalars_raw;
	std::vector<rmm::device_buffer> right_device_scalars_ptrs;
	std::vector<cudf::detail::scalar_device_view_base *> right_device_scalars_raw;
	for (size_t i = 0; i < left_scalars.size(); i++) {
		left_device_scalars_ptrs.push_back(left_scalars[i] ? std::move(cudf::type_dispatcher(left_scalars[i]->type(), allocate_device_scalar{}, *(left_scalars[i]))) : rmm::device_buffer{});
		left_device_scalars_raw.push_back(static_cast<cudf::detail::scalar_device_view_base *>(left_device_scalars_ptrs.back().data()));

		right_device_scalars_ptrs.push_back(right_scalars[i] ? std::move(cudf::type_dispatcher(right_scalars[i]->type(), allocate_device_scalar{}, *(right_scalars[i]))) : rmm::device_buffer{});
		right_device_scalars_raw.push_back(static_cast<cudf::detail::scalar_device_view_base *>(right_device_scalars_ptrs.back().data()));
	}
	rmm::device_vector<cudf::detail::scalar_device_view_base *> left_device_scalars(left_device_scalars_raw);
	rmm::device_vector<cudf::detail::scalar_device_view_base *> right_device_scalars(right_device_scalars_raw);
	


	// device left, right and output types
	size_t num_operations = left_inputs.size();
	std::vector<cudf::type_id> left_input_types_vec(num_operations);
	std::vector<cudf::type_id> right_input_types_vec(num_operations);
	std::vector<cudf::type_id> output_types_vec(num_operations);
	std::map<column_index_type, cudf::type_id> output_map_type;
	for(size_t i = 0; i < num_operations; i++) {
		column_index_type left_index = left_inputs[i];
		column_index_type right_index = right_inputs[i];
		column_index_type output_index = outputs[i];

		if(left_index >= 0 && left_index < table.num_columns()) {
			left_input_types_vec[i] = table.column(left_index).type().id();
		} else if(left_index == SCALAR_NULL_INDEX) {
			left_input_types_vec[i] = cudf::type_id::EMPTY;
		} else if(left_index == SCALAR_INDEX) {
			left_input_types_vec[i] = left_scalars[i]->type().id();
		} else if(left_index == UNARY_INDEX) {
			// not possible
			assert(false);
		} else {
			// have to get it from the output that generated it
			left_input_types_vec[i] = output_map_type[left_index];
		}

		if(right_index >= 0 && right_index < table.num_columns()) {
			right_input_types_vec[i] = table.column(right_index).type().id();
		} else if(right_index == SCALAR_NULL_INDEX) {
			right_input_types_vec[i] = cudf::type_id::EMPTY;
		} else if(right_index == SCALAR_INDEX) {
			right_input_types_vec[i] = right_scalars[i]->type().id();
		} else if(right_index == UNARY_INDEX) {
			// wont be used its a unary operation
			right_input_types_vec[i] = cudf::type_id::EMPTY;
		} else {
			// have to get it from the output that generated it
			right_input_types_vec[i] = output_map_type[right_index];
		}

		if(right_index == UNARY_INDEX){
			output_types_vec[i] =  get_output_type(operators[i], left_input_types_vec[i]);
		}else if(right_index == NULLARY_INDEX){
			output_types_vec[i] = get_output_type(operators[i]);
		}else{
			output_types_vec[i] = get_output_type(operators[i], left_input_types_vec[i], right_input_types_vec[i]);
		}
		

		output_map_type[output_index] = output_types_vec[i];
	}
	rmm::device_vector<cudf::type_id> left_device_input_types(left_input_types_vec);
	rmm::device_vector<cudf::type_id> right_device_input_types(right_input_types_vec);

	rmm::device_vector<column_index_type> left_device_inputs(left_inputs);
	rmm::device_vector<column_index_type> right_device_inputs(right_inputs);
	rmm::device_vector<column_index_type> device_outputs(outputs);
	rmm::device_vector<column_index_type> final_device_output_positions(final_output_positions);
	rmm::device_vector<operator_type> device_operators(operators);



	InterpreterFunctor op(*device_out_table_view,
												*device_table_view,
												static_cast<cudf::size_type>(left_device_inputs.size()),
												left_device_inputs.data().get(),
												right_device_inputs.data().get(),
												device_outputs.data().get(),
												final_device_output_positions.data().get(),
												left_device_input_types.data().get(),
												right_device_input_types.data().get(),
												device_operators.data().get(),
												left_device_scalars.data().get(),
												right_device_scalars.data().get(),
												temp_device_valids_in_buffer.data(),
												temp_device_valids_out_buffer.data());

rmm::device_vector<curandState> states(min_grid_size * block_size);
	 std::random_device rd;
	  std::default_random_engine generator(rd());
  std::uniform_int_distribution<long long unsigned> distribution(0,0xFFFFFFFFFFFFFFFF);
	unsigned long long seed = distribution(generator);
	setup_rand_kernel<<<min_grid_size,
		block_size,
		shared_memory_per_thread * block_size,
		stream>>>(states.data().get(),seed);
	if (operation_num_rows == 0){
		operation_num_rows = table.num_rows();
	}
	transformKernel<<<min_grid_size,
		block_size,
		shared_memory_per_thread * block_size,
		stream>>>(op, operation_num_rows, states.data().get());
	CUDA_TRY(cudaStreamSynchronize(stream));
}

}  // namespace interops
