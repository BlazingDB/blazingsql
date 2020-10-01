#include "node.hpp"

namespace comm{

node::node(int idx, std::string id, ucp_ep_h ucp_ep, ucp_worker_h ucp_worker)
	: _idx{idx}, _id{id}, _ucp_ep{ucp_ep}, _ucp_worker{ucp_worker} {}

int node::index() const { return _idx; }

std::string node::id() const { return _id; };

ucp_ep_h node::get_ucp_endpoint() const { return _ucp_ep; }

std::string node::ip() const { return _ip; }

int node::port() const { return _port; }

ucp_worker_h node::get_ucp_worker() const { return _ucp_worker; }

} // namespace comm
