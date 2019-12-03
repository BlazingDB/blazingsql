#include "NativeHdfs.h"

#include <iostream>

#include "arrow/status.h"

#include "SystemEnvironment.h"

NativeHdfs::NativeHdfs() {}

NativeHdfs::~NativeHdfs() {}

void NativeHdfs::connect(const std::string & host,
	int port,
	const std::string & user,
	arrow::io::HdfsDriver driver,
	const std::string & kerberosTicket) {
	arrow::io::HdfsConnectionConfig config;
	config.host = host;
	config.port = port;
	config.user = user;
	config.driver = driver;
	config.kerb_ticket = kerberosTicket;

	this->failWhenNotOk(
		// Possible BUG: loop after request
		// Info: raise LIBNativeHdfs::NativeHdfs3 internal HdfsNetworkConnectException
		arrow::io::HadoopFileSystem::Connect(&config, &this->hdfs),
		"NativeHdfs::NativeHdfs Connect");
}

void NativeHdfs::connectFromSystemEnvironment() {
	const std::string host = SystemEnvironment::getHostEnvValue();
	const int port = SystemEnvironment::getPortEnvValue();
	const std::string user = SystemEnvironment::getUserEnvValue();
	const arrow::io::HdfsDriver driver = arrow::io::HdfsDriver::LIBHDFS3;  // TODO percy hdfs driver fixed
	const std::string kerberosTicket = SystemEnvironment::getkerberosTicketEnvValue();

	connect(host, port, user, driver, kerberosTicket);
}

void NativeHdfs::disconnect() { this->failWhenNotOk(this->hdfs->Disconnect(), "NativeHdfs::NativeHdfs Disconnect"); }

void NativeHdfs::createFile(const char * path) const {
	std::shared_ptr<arrow::io::HdfsOutputStream> file;

	this->createFileOpen(file, path);
	this->createFileSync(file);
}

void NativeHdfs::createFile(const char * path, const void * buffer, int64_t nbytes) const {
	std::shared_ptr<arrow::io::HdfsOutputStream> file;

	this->createFileOpen(file, path);

	this->failWhenNotOk(
		file->Write(static_cast<const uint8_t *>(buffer), nbytes), "NativeHdfs::NativeHdfs Write no bytes");

	this->createFileSync(file);
}

void NativeHdfs::remove(const char * path, bool recursive) const {
	this->failWhenNotOk(this->hdfs->Delete(path, recursive), "NativeHdfs::NativeHdfs Delete");
}

void NativeHdfs::removeDirectory(const char * path) const {
	this->failWhenNotOk(this->hdfs->DeleteDirectory(path), "NativeHdfs::NativeHdfs Delete diretory");
}

bool NativeHdfs::exists(const char * path) const { return this->hdfs->Exists(path); }

void NativeHdfs::makeDirectory(const char * path) const {
	this->failWhenNotOk(this->hdfs->MakeDirectory(path), "NativeHdfs::NativeHdfs Make directory");
}

const std::vector<std::string> NativeHdfs::listDirectory(const char * path) const {
	std::vector<arrow::io::HdfsPathInfo> listing;

	this->failWhenNotOk(this->hdfs->ListDirectory(path, &listing), "NativeHdfs::NativeHdfs List directory");

	std::vector<std::string> filenames;
	for(const arrow::io::HdfsPathInfo & path_info : listing) {
		filenames.push_back(path_info.name);
	}

	return filenames;
}

const std::shared_ptr<arrow::Buffer> NativeHdfs::readFile(const char * path) const {
	std::shared_ptr<arrow::io::HdfsReadableFile> file;
	std::shared_ptr<arrow::Buffer> out;
	int64_t nbytes;

	this->failWhenNotOk(this->hdfs->OpenReadable(path, &file), "NativeHdfs::NativeHdfs Open readable");
	this->failWhenNotOk(file->GetSize(&nbytes), "NativeHdfs::NativeHdfs Buffer get size");
	this->failWhenNotOk(file->Read(nbytes, &out), "NativeHdfs::NativeHdfs Buffer read");

	return out;
}

const std::string NativeHdfs::readTextFile(const char * path) const {
	std::shared_ptr<arrow::Buffer> out = readFile(path);
	return std::string(reinterpret_cast<const char *>(out->data()), out->size());
}

void NativeHdfs::createFileOpen(std::shared_ptr<arrow::io::HdfsOutputStream> & file, const char * path) const {
	this->failWhenNotOk(this->hdfs->OpenWriteable(path, false, &file), "NativeHdfs::NativeHdfs Open writeable");
}

void NativeHdfs::createFileSync(std::shared_ptr<arrow::io::HdfsOutputStream> & file) const {
	this->failWhenNotOk(file->Flush(), "NativeHdfs::NativeHdfs Flush");
	this->failWhenNotOk(file->Close(), "NativeHdfs::NativeHdfs Close");
}

// TODO(feature): input stream (v.g. to add path)
void NativeHdfs::failWhenNotOk(const arrow::Status status, const char * message) const {
	if(!status.ok()) {
		// TODO thrown system exeption here to goolge test can stop the test execution
		std::cout << message << " failed - " << status.ToString() << std::endl;
	}
}
