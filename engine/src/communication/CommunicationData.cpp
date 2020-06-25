#include "CommunicationData.h"

namespace ral {
namespace communication {

CommunicationData::CommunicationData() {}

CommunicationData & CommunicationData::getInstance() {
	static CommunicationData communicationData;
	return communicationData;
}

void CommunicationData::initialize(const std::string & worker_id,
	const std::string & selfRalIp,
	int16_t selfRalCommunicationPort) {

	auto address = blazingdb::transport::Address::TCP(selfRalIp, selfRalCommunicationPort, 0);

	selfNode = blazingdb::transport::Node(address, worker_id);
}

const blazingdb::transport::Node & CommunicationData::getSelfNode() { return selfNode; }

}  // namespace communication
}  // namespace ral
