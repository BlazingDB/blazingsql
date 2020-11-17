#include "CommunicationData.h"

namespace ral {
namespace communication {

CommunicationData::CommunicationData() {}

CommunicationData & CommunicationData::getInstance() {
	static CommunicationData communicationData;
	return communicationData;
}

void CommunicationData::initialize(const std::string & worker_id) {

	_selfNode = blazingdb::transport::Node( worker_id);
}

const blazingdb::transport::Node & CommunicationData::getSelfNode() { return _selfNode; }

}  // namespace communication
}  // namespace ral
