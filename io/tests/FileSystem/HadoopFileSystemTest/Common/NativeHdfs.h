#ifndef _TEST_FILESYSTEM_HADOOP_HDFS_UTILS_H
#define _TEST_FILESYSTEM_HADOOP_HDFS_UTILS_H

#include "arrow/buffer.h"
#include "arrow/io/hdfs.h"

// NOTE this class will be used along with TestResources to setup hdfs test folder
// Wrapper of arrow::io::HadoopFileSystem or any native HDFS API
class NativeHdfs {
public:
	NativeHdfs();
	~NativeHdfs();

	void connect(const std::string & host,
		int port,
		const std::string & user,
		arrow::io::HdfsDriver driver,
		const std::string & kerberosTicket);
	void connectFromSystemEnvironment();
	void disconnect();

	void createFile(const char * path) const;
	void createFile(const char * path, const void * buffer, int64_t nbytes) const;
	void remove(const char * path, bool recursive = false) const;
	void removeDirectory(const char * path) const;
	bool exists(const char * path) const;
	void makeDirectory(const char * path) const;
	const std::vector<std::string> listDirectory(const char * path) const;
	const std::shared_ptr<arrow::Buffer> readFile(const char * path) const;
	const std::string readTextFile(const char * path) const;  // TODO why this return string?

private:
	void createFileOpen(std::shared_ptr<arrow::io::HdfsOutputStream> & file, const char * path) const;
	void createFileSync(std::shared_ptr<arrow::io::HdfsOutputStream> & file) const;

	// TODO(feature): input stream (v.g. to add path)
	void failWhenNotOk(const arrow::Status status, const char * message) const;

private:
	std::shared_ptr<arrow::io::HadoopFileSystem> hdfs;
};

#endif  // _TEST_FILESYSTEM_HADOOP_HDFS_UTILS_H
