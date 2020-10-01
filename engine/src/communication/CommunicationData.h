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

	void initialize(const std::string & worker_id);

	const blazingdb::transport::Node & getSelfNode();

	CommunicationData(CommunicationData &&) = delete;
	CommunicationData(const CommunicationData &) = delete;
	CommunicationData & operator=(CommunicationData &&) = delete;
	CommunicationData & operator=(const CommunicationData &) = delete;

private:
	CommunicationData();

	blazingdb::transport::Node _selfNode;
};

}  // namespace communication
}  // namespace ral
