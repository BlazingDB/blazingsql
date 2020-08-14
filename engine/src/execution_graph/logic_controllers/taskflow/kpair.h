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
	std::size_t concat_cache_num_bytes = 400000000;
	bool concat_all = false; ///< Applicable only for concatenating caches
};

using kernel_pair = std::pair<kernel *, std::string>;

/**
	@brief A temporary object to represent a pair of two kernels linked into the execution graph.
*/
class kpair {
public:
	kpair(kernel & a, kernel & b, const cache_settings & config = cache_settings{}) : cache_machine_config(config) {
		src = &a;
		dst = &b;
	}
	kpair(kernel & a, kernel_pair b, const cache_settings & config = cache_settings{}) : cache_machine_config(config) {
		src = &a;
		dst = b.first;
		dst_port_name = b.second;
	}
	kpair(kernel_pair a, kernel & b, const cache_settings & config = cache_settings{}) : cache_machine_config(config) {
		src = a.first;
		src_port_name = a.second;
		dst = &b;
	}
	kpair(kernel_pair a, kernel_pair b, const cache_settings & config = cache_settings{})
		: cache_machine_config(config) {
		src = a.first;
		src_port_name = a.second;
		dst = b.first;
		dst_port_name = b.second;
	}

	kpair(const kpair &) = default;
	kpair(kpair &&) = default;

	bool has_custom_source() const { return not src_port_name.empty(); }
	bool has_custom_target() const { return not dst_port_name.empty(); }

	kernel * src = nullptr;
	kernel * dst = nullptr;
	const cache_settings cache_machine_config;
	std::string src_port_name;
	std::string dst_port_name;
};

static kpair operator>>(kernel & a, kernel & b) { return kpair(a, b); }

static kpair operator>>(kernel & a, kernel_pair b) { return kpair(a, std::move(b)); }
static kpair operator>>(kernel_pair a, kernel & b) { return kpair(std::move(a), b); }

static kpair operator>>(kernel_pair a, kernel_pair b) { return kpair(std::move(a), std::move(b)); }

static kpair link(kernel & a, kernel & b, const cache_settings & config = cache_settings{}) {
	return kpair(a, b, config);
}

static kpair link(kernel & a, kernel_pair b, const cache_settings & config = cache_settings{}) {
	return kpair(a, std::move(b), config);
}

static kpair link(kernel_pair a, kernel & b, const cache_settings & config = cache_settings{}) {
	return kpair(std::move(a), b, config);
}

static kpair link(kernel_pair a, kernel_pair b, const cache_settings & config = cache_settings{}) {
	return kpair(std::move(a), std::move(b), config);
}

}  // namespace cache
}  // namespace ral
