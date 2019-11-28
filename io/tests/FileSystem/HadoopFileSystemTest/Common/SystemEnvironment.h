#ifndef _TEST_FILESYSTEM_HADOOP_HADOOP_TEST_SYSTEMENVIRONMENT_H
#define _TEST_FILESYSTEM_HADOOP_HADOOP_TEST_SYSTEMENVIRONMENT_H

#include "FileSystem/FileSystemConnection.h"

using namespace HadoopFileSystemConnection;

namespace SystemEnvironment {
	//TODO percy : move this function to a common place so S3FS can use it too
	std::string getConnectionPropertyEnvValue(const std::string &connectionPropertyEnvName);

	std::string getConnectionPropertyEnvValue(ConnectionProperty connectionProperty);

	const std::string getHostEnvValue();

	const int getPortEnvValue();

	const std::string getUserEnvValue();

	const DriverType getDriverTypeEnvValue();

	const std::string getkerberosTicketEnvValue();

	//Will use the system env in order to generate the FileSystemConnection
	const FileSystemConnection getLocalHadoopFileSystemConnection();
}

#endif // _TEST_FILESYSTEM_HADOOP_HADOOP_TEST_SYSTEMENVIRONMENT_H
