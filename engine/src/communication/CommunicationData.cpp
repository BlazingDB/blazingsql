#include "CommunicationData.h"
#include <blazingdb/transport/Address.h>
#include <blazingdb/transport/Node.h>

namespace ral {
namespace communication {

CommunicationData::CommunicationData() : orchestratorPort{0} {}

CommunicationData & CommunicationData::getInstance() {
	static CommunicationData communicationData;
	return communicationData;
}

void CommunicationData::initialize(int unixSocketId,
	const std::string & orchIp,
	int16_t orchCommunicationPort,
	const std::string & selfRalIp,
	int16_t selfRalCommunicationPort,
	int16_t selfRalProtocolPort) {
	orchestratorIp = orchIp;
	orchestratorPort = orchCommunicationPort;

	auto address = blazingdb::transport::Address::TCP(selfRalIp, selfRalCommunicationPort, selfRalProtocolPort);

	selfNode = blazingdb::transport::Node::Make(address);
}

const blazingdb::transport::Node & CommunicationData::getSelfNode() { return *selfNode; }

std::shared_ptr<blazingdb::transport::Node> CommunicationData::getSharedSelfNode() { return selfNode; }

std::string CommunicationData::getOrchestratorIp() { return orchestratorIp; }

int16_t CommunicationData::getOrchestratorPort() { return orchestratorPort; }

}  // namespace communication
}  // namespace ral