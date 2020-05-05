#include "engine/static.h"

#include <vector>

#include "bsqlengine_config.h"

std::map<std::string, std::string> getProductDetails() {
	std::map<std::string, std::string> ret;
	std::vector<std::pair<std::string, std::string>> descriptiveMetadata = BLAZINGSQL_DESCRIPTIVE_METADATA;
	for (const auto description : descriptiveMetadata) {
		ret[description.first] = description.second;
	}
	return ret;
}
