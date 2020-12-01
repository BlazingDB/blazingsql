#include "CommunicationData.h"

namespace ral {
namespace communication {

CommunicationData::CommunicationData() {}

CommunicationData & CommunicationData::getInstance() {
	static CommunicationData communicationData;
	return communicationData;
}

void CommunicationData::initialize(const std::string & worker_id, const std::string& cache_directory) {
	_selfNode = blazingdb::transport::Node( worker_id);
	_cache_directory = cache_directory;
}

const blazingdb::transport::Node & CommunicationData::getSelfNode() { return _selfNode; }

std::string CommunicationData::get_cache_directory() { return _cache_directory; }

}  // namespace communication
}  // namespace ral
