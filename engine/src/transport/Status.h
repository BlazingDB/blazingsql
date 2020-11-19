#pragma once

#include <string>

namespace blazingdb {
namespace transport {

class Status {
public:
  Status(bool ok = false) : ok_{ok} {}
  inline bool IsOk() const noexcept { return ok_; }

private:
  bool ok_{false};
};

}  // namespace transport
}  // namespace blazingdb
