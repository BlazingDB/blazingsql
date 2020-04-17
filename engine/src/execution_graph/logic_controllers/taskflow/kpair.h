#pragma once

#include "port.h"
#include "kernel.h"

namespace ral {
namespace cache { 

using kernel_pair = std::pair<kernel *, std::string>;
enum class CacheType { NON_WAITING, SIMPLE, CONCATENATING, FOR_EACH };

struct cache_settings {
	CacheType type = CacheType::SIMPLE;
	const int num_partitions = 1;
};

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
 
}  // namespace cache
}  // namespace ral