#pragma once

#include <blazingdb/transport/Node.h>
#include <memory>
#include <string>

namespace ral {
namespace communication {

class CommunicationData {
public:
	static CommunicationData & getInstance();

	void initialize(const std::string & worker_id,
		const std::string & selfRalIp,
		int16_t selfRalCommunicationPort);

	const blazingdb::transport::Node & getSelfNode();

	CommunicationData(CommunicationData &&) = delete;
	CommunicationData(const CommunicationData &) = delete;
	CommunicationData & operator=(CommunicationData &&) = delete;
	CommunicationData & operator=(const CommunicationData &) = delete;

private:
	CommunicationData();

	blazingdb::transport::Node selfNode;
};

}  // namespace communication
}  // namespace ral
