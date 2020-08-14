#include "tests/utilities/BlazingUnitTest.h"
#include "communication/CommunicationInterface/protocols.hpp"
#include "execution_graph/logic_controllers/CacheMachine.h"

#include <memory>
#include <tests/utilities/base_fixture.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/column/column_factories.hpp>
#include <tests/utilities/column_utilities.hpp>
#include <tests/utilities/type_lists.hpp>
#include <tests/utilities/column_wrapper.hpp>
#include <tests/utilities/table_utilities.hpp>

using namespace ral::frame;

struct MessageSenderTest : public BlazingUnitTest {
  MessageSenderTest() {}

  ~MessageSenderTest() {}
};

TEST_F(MessageSenderTest, construct_test) {
  
}
