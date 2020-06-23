#pragma once

#include "port.h"
#include "kernel.h"

namespace ral {
namespace cache {

/**
	@brief A temporary object to represent a pair of two kernels linked into the execution graph.
*/
class kpair {
public:
	kpair(std::shared_ptr<kernel> a, std::shared_ptr<kernel> b, const cache_settings & config = cache_settings{}) : cache_machine_config(config) {
		src = a;
		dst = b;
	}
	kpair(std::shared_ptr<kernel> a, std::shared_ptr<kernel> b, const std::string & port_b, const cache_settings & config = cache_settings{}) : cache_machine_config(config) {
		src = a;
		dst = b;
		dst_port_name = port_b;
	}
	kpair(std::shared_ptr<kernel> a, const std::string & port_a, std::shared_ptr<kernel> b, const std::string & port_b, const cache_settings & config = cache_settings{})
		: cache_machine_config(config) {
		src = a;
		src_port_name = port_a;
		dst = b;
		dst_port_name = port_b;
	}

	kpair(const kpair &) = default;
	kpair(kpair &&) = default;

	bool has_custom_source() const { return not src_port_name.empty(); }
	bool has_custom_target() const { return not dst_port_name.empty(); }

	std::shared_ptr<kernel> src;
	std::shared_ptr<kernel> dst;
	const cache_settings cache_machine_config;
	std::string src_port_name;
	std::string dst_port_name;
};

}  // namespace cache
}  // namespace ral
