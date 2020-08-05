/*
 * BlazingContext.cpp
 *
 *  Created on: Dec 5, 2017
 *      Author: felipe
 */

#ifdef S3_SUPPORT
#include <aws/core/Aws.h>
#endif

#include "Config/BlazingContext.h"
#include "Library/Logging/Logger.h"
#include "Util/FileUtil.h"
#include "Util/StringUtil.h"
#include "arrow/status.h"
namespace Logging = Library::Logging;

BlazingContext * BlazingContext::instance = nullptr;

void BlazingContext::initExternalSystems() {
#ifdef S3_SUPPORT
	Aws::SDKOptions sdkOptions;
	Aws::InitAPI(sdkOptions);
#endif
}

void BlazingContext::shutDownExternalSystems() {
#ifdef S3_SUPPORT
	Aws::SDKOptions sdkOptions;
	Aws::ShutdownAPI(sdkOptions);
#endif
}

BlazingContext::BlazingContext() : fileSystemManager(new FileSystemManager()) {}

BlazingContext::~BlazingContext() {
	if(instance != nullptr) {
		delete instance;
	}
}


std::shared_ptr<FileSystemManager> BlazingContext::getFileSystemManager() { return this->fileSystemManager; }


BlazingContext * BlazingContext::getInstance() {
	if(instance == NULL) {
		instance = new BlazingContext();
	}
	return instance;
}
