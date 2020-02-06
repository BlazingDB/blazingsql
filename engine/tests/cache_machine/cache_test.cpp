#include <cudf/cudf.h>
#include <cudf/types.hpp>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include "data_builder.h"

struct CacheMachineTest : public cudf::test::BaseFixture {
  CacheMachineTest() {

  }

  ~CacheMachineTest() {
  }

};

TEST_F(CacheMachineTest, Init) {
    unsigned long long  gpuMemory = 1024;
    std::vector<unsigned long long > memoryPerCache = {INT_MAX};
    std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
    ral::cache::CacheMachine cacheMachine(gpuMemory, memoryPerCache, cachePolicyTypes);

    for (int i = 0; i < 1000; ++i) {
        auto table = build_custom_table();
        std::cout << ">> " << i << "|" <<  table->sizeInBytes() << std::endl;
        cacheMachine.addToCache(std::move(table));
        if (i % 5 == 0) {
            auto cacheTable = cacheMachine.pullFromCache();
        }
    }
    std::this_thread::sleep_for (std::chrono::seconds(1));
}