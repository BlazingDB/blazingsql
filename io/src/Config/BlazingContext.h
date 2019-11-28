
#ifndef BLAZINGCONTEXT_H_
#define BLAZINGCONTEXT_H_


#include "FileSystem/FileSystemManager.h"
#include <string>
#include <mutex> //TODO:remove



class BlazingContext {
	public:
		static void initExternalSystems();
		static void shutDownExternalSystems();

		std::shared_ptr<FileSystemManager> getFileSystemManager();
		static BlazingContext* getInstance();
		virtual ~BlazingContext();


	private:
		BlazingContext();
		static BlazingContext* instance;

		std::shared_ptr<FileSystemManager> fileSystemManager;
};

#endif
