#include "CommunicationData.h"
#include <blazingdb/transport/Address.h>
#include <blazingdb/transport/Node.h>

namespace ral {
namespace communication {
namespace experimental {

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

	auto address = blazingdb::transport::experimental::Address::TCP(selfRalIp, selfRalCommunicationPort, selfRalProtocolPort);

	selfNode = blazingdb::transport::experimental::Node(address);
}

const blazingdb::transport::experimental::Node & CommunicationData::getSelfNode() { return selfNode; }

std::string CommunicationData::getOrchestratorIp() { return orchestratorIp; }

int16_t CommunicationData::getOrchestratorPort() { return orchestratorPort; }

}  // namespace experimental
}  // namespace communication
}  // namespace ral