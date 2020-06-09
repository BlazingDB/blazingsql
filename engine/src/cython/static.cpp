#include "engine/static.h"

#include <vector>

#include "bsqlengine_config.h"
#include "Util/StringUtil.h"

std::map<std::string, std::string> getProductDetails() {
	std::map<std::string, std::string> ret;
	std::vector<std::pair<std::string, std::string>> descriptiveMetadata = BLAZINGSQL_DESCRIPTIVE_METADATA;
	for (auto description : descriptiveMetadata) {
		ret[description.first] = StringUtil::trim(description.second);
	}
	return ret;
}
