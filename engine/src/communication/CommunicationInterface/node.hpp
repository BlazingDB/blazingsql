#pragma once

#include <string>
#include <ucp/api/ucp.h>

namespace comm {

/// \brief A Node is the representation of a RAL component used in the transport
/// process.
class node {
public:
  node(int idx, std::string id, ucp_ep_h ucp_ep, ucp_worker_h ucp_worker);

  int index() const;
  std::string id() const;

  ucp_ep_h get_ucp_endpoint() const;
  ucp_worker_h get_ucp_worker() const;

protected:
  int _idx;
  std::string _id;

  ucp_ep_h _ucp_ep;
  ucp_worker_h _ucp_worker;
};

}  // namespace comm
