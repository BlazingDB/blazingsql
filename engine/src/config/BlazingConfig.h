#ifndef RAL_CONFIG_BLAZINGCONFIG_H
#define RAL_CONFIG_BLAZINGCONFIG_H

#include <string>

namespace ral {
namespace config {

class BlazingConfig {
public:
	static BlazingConfig & getInstance() {
		static BlazingConfig config;
		return config;
	}

public:
	const std::string & getLogName() const;

	BlazingConfig & setLogName(std::string && value);

	BlazingConfig & setLogName(const std::string & value);

public:
	const std::string & getSocketPath() const;

	BlazingConfig & setSocketPath(std::string && value);

	BlazingConfig & setSocketPath(const std::string & value);

private:
	BlazingConfig();

	BlazingConfig(BlazingConfig &&) = delete;

	BlazingConfig(const BlazingConfig &) = delete;

	BlazingConfig operator=(BlazingConfig &&) = delete;

	BlazingConfig operator=(const BlazingConfig &) = delete;

private:
	std::string log_name{};
	std::string socket_path{};
};

}  // namespace config
}  // namespace ral

#endif
