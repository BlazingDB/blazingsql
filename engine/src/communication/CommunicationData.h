#pragma once 

#include <blazingdb/transport/Node.h>
#include <memory>
#include <string>

namespace ral {
namespace communication {

class CommunicationData {
public:
	static CommunicationData & getInstance();

	void initialize(int unixSocketId,
		const std::string & orchIp,
		int16_t orchCommunicationPort,
		const std::string & selfRalIp,
		int16_t selfRalCommunicationPort,
		int16_t selfRalProtocolPort);

	const blazingdb::transport::experimental::Node & getSelfNode();

	std::string getOrchestratorIp();
	int16_t getOrchestratorPort();

	CommunicationData(CommunicationData &&) = delete;
	CommunicationData(const CommunicationData &) = delete;
	CommunicationData & operator=(CommunicationData &&) = delete;
	CommunicationData & operator=(const CommunicationData &) = delete;

private:
	CommunicationData();

	std::string orchestratorIp;
	int16_t orchestratorPort;
	blazingdb::transport::experimental::Node selfNode;
};

}  // namespace communication
}  // namespace ral
