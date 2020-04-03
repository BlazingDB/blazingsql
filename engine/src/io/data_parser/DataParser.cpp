#include "DataParser.h"

namespace ral {
namespace io {

//Common function for Orc and Parquet parsers
std::pair<std::vector<int>, std::vector<int> > get_groups(const Schema & schema) {
	std::vector<int> groups = schema.get_rowgroup_ids(0);
    // because the Schema we are using here was already filtered for a specific file by Schema::fileSchema we are simply getting the first set of rowgroup_ids
	// now lets get these groups in batches of consecutive rowgroups because that is how the reader will want them
	std::vector<int> consecutive_stripe_start(1, groups.at(0));
	std::vector<int> consecutive_stripe_length;
	int length_count = 1;
	int last_rowgroup = consecutive_stripe_start.back();
	for (int i = 1; i < groups.size(); i++){
		if (last_rowgroup + 1 == groups[i]){ // consecutive
			length_count++;
			last_rowgroup = groups[i];
		} else {
			consecutive_stripe_length.push_back(length_count);
			consecutive_stripe_start.push_back(groups[i]);
			last_rowgroup = groups[i];
			length_count = 1;
		}
	}
	consecutive_stripe_length.push_back(length_count);
	return std::make_pair(consecutive_stripe_start, consecutive_stripe_length);
}

} /* namespace io */
} /* namespace ral */