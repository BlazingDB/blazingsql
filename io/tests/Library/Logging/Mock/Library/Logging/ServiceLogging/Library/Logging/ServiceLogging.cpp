#include "Library/Logging/ServiceLogging.h"
#include "Library/Logging/GenericOutput.h"

namespace Library {
namespace Logging {
ServiceLogging::ServiceLogging() {
	if(mock) {
		delete mock;
		mock = nullptr;
	}
	mock = new BlazingTest::Library::Logging::ServiceLoggingMock();
}

ServiceLogging::~ServiceLogging() {
	if(mock) {
		delete mock;
		mock = nullptr;
	}
}

void ServiceLogging::setLogData(std::string && data) { mock->setLogData(std::move(data)); }

void ServiceLogging::setLogData(const std::string & data) { mock->setLogData(data); }

void ServiceLogging::setLogOutput(GenericOutput * output) { mock->setLogOutput(output); }
}  // namespace Logging
}  // namespace Library
