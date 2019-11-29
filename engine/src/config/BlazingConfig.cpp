#include "config/BlazingConfig.h"

namespace ral {
namespace config {

    BlazingConfig::BlazingConfig()
    { }

    const std::string& BlazingConfig::getLogName() const {
        return log_name;
    }

    BlazingConfig& BlazingConfig::setLogName(std::string&& value) {
        log_name = std::move(value);
        return *this;
    }

    BlazingConfig& BlazingConfig::setLogName(const std::string& value) {
        log_name = std::move(value);
        return *this;
    }

    const std::string& BlazingConfig::getSocketPath() const {
        return socket_path;
    }

    BlazingConfig& BlazingConfig::setSocketPath(std::string&& value) {
        socket_path = std::move(value);
        return *this;
    }

    BlazingConfig& BlazingConfig::setSocketPath(const std::string& value) {
        socket_path = value;
        return *this;
    }

} // namespace config
} // namespace ral
