#pragma once

#include "port.h"
#include "kernel.h"

namespace ral {
namespace cache {

/// \brief An enum type that  represent a cache machine type
/// `SIMPLE` is used to identify a CacheMachine class.
/// `CONCATENATING` is used to identify a ConcatenatingCacheMachine class.
/// `FOR_EACH` is used to identify a graph execution with kernels that need to send many partitions at once,
/// for example for kernels PartitionSingleNodeKernel and MergeStreamKernel.
enum class CacheType {SIMPLE, CONCATENATING, FOR_EACH };

/// \brief An object that  represent a cache machine configuration (type and num_partitions)
/// used in create_cache_machine and create_cache_machine functions.
struct cache_settings {
	CacheType type = CacheType::SIMPLE;
	int num_partitions = 1;
	std::shared_ptr<Context> context;
	std::size_t flow_control_bytes_threshold = std::numeric_limits<std::size_t>::max();
	bool concat_all = false; //applicable only for concatenating caches
};

using kernel_pair = std::pair<kernel *, std::string>;

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
