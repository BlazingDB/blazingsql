#ifndef COMMUNICATION_DATA_H_
#define COMMUNICATION_DATA_H_

#include <memory>
#include <string>
#include <blazingdb/transport/Node.h>
#include <blazingdb/transport/Node.h>

namespace ral {
namespace communication {

class CommunicationData
{
public:
  static CommunicationData& getInstance();
  
  void initialize(int unixSocketId, const std::string& orchIp, int16_t orchCommunicationPort, const std::string& selfRalIp, int16_t selfRalCommunicationPort, int16_t selfRalProtocolPort);
  
  const blazingdb::transport::Node& getSelfNode();
 
  std::shared_ptr<blazingdb::transport::Node> getSharedSelfNode();

  std::string getOrchestratorIp();
  int16_t getOrchestratorPort();

  CommunicationData(CommunicationData&&) = delete;
  CommunicationData(const CommunicationData&) = delete;
  CommunicationData& operator=(CommunicationData&&) = delete;
  CommunicationData& operator=(const CommunicationData&) = delete;

private:
  CommunicationData();

  std::string orchestratorIp;
  int16_t orchestratorPort;
  std::shared_ptr<blazingdb::transport::Node> selfNode;
};

} // namespace communication
} // namespace ral

#endif /* COMMUNICATION_DATA_H_ */