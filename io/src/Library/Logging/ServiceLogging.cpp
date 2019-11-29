#include "Library/Logging/ServiceLogging.h"
#include "CoutOutput.h"
#include "Library/Logging/GenericOutput.h"
#include <stdlib.h>

namespace Library {
namespace Logging {
ServiceLogging::ServiceLogging() {
	auto value = new Library::Logging::CoutOutput();
	this->output = value;
	srand(time(NULL));
	this->nodeInd = (unsigned int) ((rand() + 1) * 1000);  // times 1000 so that it is obvious that its not an actual
														   // nodeInd but a temporary unique id
}

ServiceLogging::~ServiceLogging() {
	if(output) {
		delete output;
		output = nullptr;
	}
}

void ServiceLogging::setLogData(std::string && data) { output->flush(data); }

void ServiceLogging::setLogData(const std::string & data) { output->flush(data); }

void ServiceLogging::setLogData(const std::string & datetime, const std::string & level, const std::string & message) {
	output->flush(this->nodeInd, datetime, level, message);
}

void ServiceLogging::setLogOutput(GenericOutput * value) {
	if(output) {
		delete output;
	}
	output = value;
}

void ServiceLogging::setNodeIdentifier(const int nodeInd) {
	std::string message = "Node index " + std::to_string(nodeInd) + " was using temporary node index identifier " +
						  std::to_string(this->nodeInd);
	this->nodeInd = nodeInd;
	this->output->flush(message);
}
}  // namespace Logging
}  // namespace Library
