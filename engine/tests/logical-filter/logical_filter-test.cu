#include <cstdlib>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"
#include <DataFrame.h>
#include <GDFColumn.cuh>
#include <LogicalFilter.h>
#include <Utils.cuh>


class TestEnvironment : public testing::Environment {
public:
  virtual ~TestEnvironment() {}
  virtual void SetUp() { rmmInitialize(nullptr); }

  void TearDown() { cudaDeviceReset(); }
};

struct logical_filter_TEST : public ::testing::Test {

  void SetUp() {

    input1 = new char[num_values];
    input2 = new char[num_values];
    input3 = new char[num_values];
    input4 = new char[num_values];

    for (std::size_t i = 0; i < num_values; i++) {
      if (i % 2 == 0) {
        input1[i] = 1;
      } else {
        input1[i] = i;
      }
      input2[i] = i;
      input3[i] = i % 2;
      input4[i] = i + 1;
    }

    inputs.resize(4);
 	gdf_dtype_extra_info extra_info{TIME_UNIT_NONE};
    inputs[0].create_gdf_column(GDF_INT8, extra_info, num_values,
                                (void *)input1, 1, std::string{});
    inputs[1].create_gdf_column(GDF_INT8, extra_info, num_values,
                                (void *)input2, 1, std::string{});
    inputs[2].create_gdf_column(GDF_INT8, extra_info, num_values,
                                (void *)input3, 1, std::string{});
    inputs[3].create_gdf_column(GDF_INT8, extra_info, num_values,
                                (void *)input4, 1, std::string{});

    blzframe.add_table(inputs);

    output.create_gdf_column(GDF_INT8, extra_info, num_values, nullptr, 1, "");
    temp.create_gdf_column(GDF_INT8, extra_info, num_values, nullptr, 1, "");

    host_output = new char[num_values];
    device_output = new char[num_values];
  }

  void TearDown() {

    cudaMemcpy(device_output, output.data(), num_values * WIDTH_PER_VALUE,
               cudaMemcpyDeviceToHost);
    bool all_equal = true;
    for (std::size_t i = 0; i < num_values; i++) {
      // EXPECT_TRUE(host_output[i] == device_output[i]);
      if (host_output[i] != device_output[i]) {
        all_equal = false;
        std::cout << "inputs " << (int)input1[i] << " , " << (int)input2[i]
                  << " , " << (int)input3[i] << " , " << (int)input4[i]
                  << std::endl;
        std::cout << "row: " << i << " " << (int)host_output[i]
                  << " != " << (int)device_output[i] << std::endl;
      }
    }
    EXPECT_TRUE(all_equal);
    // print_column(output.get_gdf_column());
  }

  std::vector<gdf_column_cpp> inputs;

  size_t num_values = 32;

  char *input1;
  char *input2;
  char *input3;
  char *input4;

  blazing_frame blzframe;

  gdf_column_cpp output, temp;

  char *host_output;
  char *device_output;

  const int WIDTH_PER_VALUE = 1;
};

TEST_F(logical_filter_TEST, processing_expressions0) {

  {
    std::string expression = ">($1, 5)";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = input2[i] > 5 ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_expressions1) {

  {
    std::string expression = "+(*($1, $0), $2)";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = ((input1[i] * input2[i]) + input3[i]);
    }
  }
}

TEST_F(logical_filter_TEST, processing_expressions2) {

  {
    std::string expression = "=(=($1, $0), $2)";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = ((input1[i] == input2[i]) == input3[i]) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_expressions3) {

  {
    std::string expression = "*($0, $0)";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = input1[i] * input1[i];
    }
  }
}

TEST_F(logical_filter_TEST, processing_expressions4) {

  {
    std::string expression = "=(*($0, $0), 1))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = ((input1[i] * input1[i]) == 1) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_expressions5) {

  {
    std::string expression = "=($1, $2)";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = (input2[i] == input3[i]) ? 1 : 0;
    }
  }
}

// AND could process only logical operands
/*TEST_F(logical_filter_TEST, processing_logical_expressions0) {

        {
                std::string expression = "AND($0, $1)";

                evaluate_expression(
                                blzframe,
                                expression,
                                output,
                                temp);

                for(int i = 0; i < num_values; i++){
                        host_output[i] = (input1[i] && input2[i]) ? 1 : 0;
                }
    }
}*/

TEST_F(logical_filter_TEST, processing_logical_expressions1) {

  {
    std::string expression = "AND(=(*($0, $0), 1), =($1, 2))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] =
          ((input1[i] * input1[i]) == 1) && (input2[i] == 2) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions2) {

  {
    std::string expression = "AND(=(*($0, $0), 1), =($1, 2), >($2, 0))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] =
          ((input1[i] * input1[i]) == 1) && (input2[i] == 2) && (input3[i] > 0)
              ? 1
              : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions3) {

  {
    std::string expression = "AND(=($0, 1), >($1, 5), >($2, 0))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] =
          (input1[i] == 1) && (input2[i] > 5) && (input3[i] > 0) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions31) {

  {
    std::string expression = "AND(=($0, 1), >($1, 5), >($3, 10))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] =
          (input1[i] == 1) && (input2[i] > 5) && (input4[i] > 10) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions4) {

  {
    std::string expression = "AND(=($0, 1), >($1, 5))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = (input1[i] == 1) && (input2[i] > 5) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions5) {

  {
    std::string expression = "AND(>($1, 5), >($2, 0)))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = (input2[i] > 5) && (input3[i] > 0) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions51) {

  {
    std::string expression = "AND(>($1, 50), >($2, 0)))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = (input2[i] > 50) && (input3[i] > 0) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions52) {

  {
    std::string expression = "AND(>($1, 5), >($2, 100)))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = (input2[i] > 5) && (input3[i] > 100) ? 1 : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions6) {

  {
    std::string expression = "AND(=($0, 1), >($1, 5), =($2, 0), >($3, 5))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = (input1[i] == 1) && (input2[i] > 5) &&
                               (input3[i] == 0) && (input4[i] > 5)
                           ? 1
                           : 0;
    }
  }
}

TEST_F(logical_filter_TEST, processing_logical_expressions7) {

  {
    std::string expression = "AND(=($0, 1), >($1, 5), =($2, 1), >($3, 5))";

    evaluate_expression(blzframe, expression, output);

    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = (input1[i] == 1) && (input2[i] > 5) &&
                               (input3[i] == 1) && (input4[i] > 5)
                           ? 1
                           : 0;
    }
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::Environment *const env =
      ::testing::AddGlobalTestEnvironment(new TestEnvironment());
  return RUN_ALL_TESTS();
}
