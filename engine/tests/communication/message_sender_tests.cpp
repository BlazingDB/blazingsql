#include "tests/utilities/BlazingUnitTest.h"
#include "communication/CommunicationInterface/protocols.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"

#include <memory>
#include <cudf_test/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf_test/column_utilities.hpp>
#include <cudf_test/type_lists.hpp>
#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/table_utilities.hpp>

using namespace ral::frame;

struct MessageSenderTest : public BlazingUnitTest {
  MessageSenderTest() {}

  ~MessageSenderTest() {}
};

TEST_F(MessageSenderTest, construct_test) {
  
}
