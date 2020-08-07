#pragma once

#include <ucp/api/ucp.h>
#include <blazingdb/transport/Node.h>
#include <memory>
#include <string>
#include <map>

namespace ral {
namespace communication {

class CommunicationData {
public:
	static CommunicationData & getInstance();

	void initialize(const std::string & worker_id,
		const std::string & selfRalIp,
		int16_t selfRalCommunicationPort,
		const std::map<std::string, ucp_ep_h> & worker_id_to_ucp_ep);

	const blazingdb::transport::Node & getSelfNode();

	const std::map<std::string, ucp_ep_h> & getWorkerToUcpEndpointMap();

	CommunicationData(CommunicationData &&) = delete;
	CommunicationData(const CommunicationData &) = delete;
	CommunicationData & operator=(CommunicationData &&) = delete;
	CommunicationData & operator=(const CommunicationData &) = delete;

private:
	CommunicationData();

	blazingdb::transport::Node _selfNode;

	std::map<std::string, ucp_ep_h> _worker_id_to_ucp_ep;
};

}  // namespace communication
}  // namespace ral
