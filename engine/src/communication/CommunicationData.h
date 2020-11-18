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

	void initialize(const std::string & worker_id, const std::string& cache_directory);

	const blazingdb::transport::Node & getSelfNode();

	std::string get_cache_directory();

	CommunicationData(CommunicationData &&) = delete;
	CommunicationData(const CommunicationData &) = delete;
	CommunicationData & operator=(CommunicationData &&) = delete;
	CommunicationData & operator=(const CommunicationData &) = delete;

private:
	CommunicationData();

	blazingdb::transport::Node _selfNode;
	std::string _cache_directory;
};

}  // namespace communication
}  // namespace ral
