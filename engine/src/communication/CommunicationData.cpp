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
	int16_t selfRalCommunicationPort,
	const std::map<std::string, ucp_ep_h> & worker_id_to_ucp_ep) {

	auto address = blazingdb::transport::Address::TCP(selfRalIp, selfRalCommunicationPort, 0);

	_selfNode = blazingdb::transport::Node(address, worker_id);
	_worker_id_to_ucp_ep = worker_id_to_ucp_ep;
}

const blazingdb::transport::Node & CommunicationData::getSelfNode() { return _selfNode; }

const std::map<std::string, ucp_ep_h> &  CommunicationData::getWorkerToUcpEndpointMap() { return _worker_id_to_ucp_ep; }

}  // namespace communication
}  // namespace ral
