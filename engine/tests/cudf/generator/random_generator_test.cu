#include "cuDF/generator/random_generator.cuh"
#include "gtest/gtest.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <numeric>
#include <random>
#include <GDFColumn.cuh>

namespace {
struct RandomGeneratorTest : public testing::Test {
  RandomGeneratorTest() {}

  ~RandomGeneratorTest() {}

  void SetUp() override { rmmInitialize(nullptr); }

  void TearDown() override {}

  void generate_population_data() {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine generator(seed);
    std::uniform_int_distribution<uint64_t> distribution(min_value, max_value);

    population_data.resize(size_of_samples);
    std::generate(
        population_data.begin(), population_data.end(),
        [&distribution, &generator]() { return distribution(generator); });
  }

  template <typename T> double perform_mean(std::vector<T> &&sample) {
    return perform_mean<T>(sample);
  }

  template <typename T> double perform_mean(const std::vector<T> &sample) {
    T sum = std::accumulate(sample.begin(), sample.end(), 0);
    return ((double)sum / (double)sample.size());
  }

  uint64_t min_value{0};
  uint64_t max_value{1000};
  uint64_t size_of_samples{1000};
  uint64_t number_of_samples{1000};

  std::vector<uint64_t> population_data;
  std::vector<double> mean_of_samples;
};

TEST_F(RandomGeneratorTest, MeanAccurateOfSamples) {
  generate_population_data();

  mean_of_samples.resize(number_of_samples);
  std::generate_n(mean_of_samples.begin(), number_of_samples, [this]() {
    cudf::generator::RandomVectorGenerator<uint64_t> generator(min_value,
                                                               max_value);
    std::vector<uint64_t> random = generator(size_of_samples);

    std::vector<uint64_t> sample(size_of_samples);
    std::transform(random.begin(), random.end(), sample.begin(),
                   [&](uint64_t data) { return population_data[data]; });

    return perform_mean(sample);
  });

  double samples_mean_value = perform_mean(mean_of_samples);
  double population_mean_value = perform_mean(population_data);
  double result =
      abs(population_mean_value - samples_mean_value) / population_mean_value;

  std::cout << "Population Mean: " << population_mean_value << "\n";
  std::cout << "Samples Mean: " << samples_mean_value << "\n";
  std::cout << "Error Function: " << result << std::endl;

  ASSERT_TRUE(result < 0.1);
}

} // namespace
