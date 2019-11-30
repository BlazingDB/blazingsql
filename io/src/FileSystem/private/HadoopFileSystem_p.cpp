/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "HadoopFileSystem_p.h"

#include <iostream>

#include "arrow/buffer.h"
#include "arrow/status.h"

#include "Util/StringUtil.h"

HadoopFileSystem::Private::Private(const FileSystemConnection & fileSystemConnection, const Path & root)
	: hdfs(nullptr), connected(false), root(root) {
	// TODO percy improve & error handling
	const bool connected = this->connect(fileSystemConnection);
}

HadoopFileSystem::Private::~Private() {
	// TODO percy improve & error handling
	const bool disconnected = this->disconnect();
}

bool HadoopFileSystem::Private::connect(const FileSystemConnection & fileSystemConnection) {
	using namespace HadoopFileSystemConnection;

	if(fileSystemConnection.isValid() == false) {
		// TODO percy error handling
		return false;
	}

	const std::string host = fileSystemConnection.getConnectionProperty(ConnectionProperty::HOST);
	const int port = atoi(fileSystemConnection.getConnectionProperty(ConnectionProperty::PORT).c_str());
	const std::string user = fileSystemConnection.getConnectionProperty(ConnectionProperty::USER);

	DriverType driver;

	// TODO percy too long maybe we should use direct string constants here instead of const map
	if(fileSystemConnection.getConnectionProperty(ConnectionProperty::DRIVER_TYPE) == "LIBHDFS3") {
		driver = DriverType::LIBHDFS3;
	} else if(fileSystemConnection.getConnectionProperty(ConnectionProperty::DRIVER_TYPE) == "LIBHDFS") {
		driver = DriverType::LIBHDFS;
	}

	const std::string kerberosTicket = fileSystemConnection.getConnectionProperty(ConnectionProperty::KERBEROS_TICKET);

	std::cout << "Connecting to HDFS server ..." << std::endl;
	std::cout << fileSystemConnection.toString() << std::endl;

	arrow::io::HdfsConnectionConfig hdfsConfig;
	hdfsConfig.host = host;
	hdfsConfig.port = port;
	hdfsConfig.user = user;
	hdfsConfig.driver = driver == DriverType::LIBHDFS ? arrow::io::HdfsDriver::LIBHDFS
													  : arrow::io::HdfsDriver::LIBHDFS3;  // TODO better code
	hdfsConfig.kerb_ticket = kerberosTicket;

	const arrow::Status connectionStat = arrow::io::HadoopFileSystem::Connect(&hdfsConfig, &this->hdfs);

	if(connectionStat.ok() == false) {
		// TODO percy raise error and clean invalid class state
		this->disconnect();  // try to disconnect and clean every invalid state
		throw std::runtime_error(connectionStat.message() + ". Filesystem " + fileSystemConnection.toString());
		return false;
	}

	this->fileSystemConnection = fileSystemConnection;

	std::vector<arrow::io::HdfsPathInfo> listing;
	auto status = this->hdfs->ListDirectory("/", &listing);

	if(status.ok() == false) {
		this->fileSystemConnection = FileSystemConnection();
		throw std::runtime_error(status.message() + ". Filesystem " + fileSystemConnection.toString());
		return false;
	}

	this->connected = true;

	return true;
}

bool HadoopFileSystem::Private::disconnect() {
	if(this->connected == false) {
		auto temphdfs = std::move(this->hdfs);
		this->hdfs = nullptr;

		return true;
	}

	const arrow::Status connectionStat = this->hdfs->Disconnect();

	if(connectionStat.ok()) {
		this->fileSystemConnection = FileSystemConnection();
		auto temphdfs = std::move(this->hdfs);
		this->hdfs = nullptr;
		this->connected = false;

		return true;
	} else {
		return false;
	}
}

FileSystemConnection HadoopFileSystem::Private::getFileSystemConnection() const noexcept {
	return this->fileSystemConnection;
}

bool HadoopFileSystem::Private::exists(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy raise error
		return false;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	return hdfs->Exists(path.toString());
}

FileStatus HadoopFileSystem::Private::getFileStatus(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy raise error
		return FileStatus();
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	arrow::io::FileStatistics stat_buf;

	const arrow::Status result = this->hdfs->Stat(path.toString(), &stat_buf);

	if(result.ok()) {
		FileType fileType;

		switch(stat_buf.kind) {
		case arrow::io::ObjectType::type::FILE: fileType = FileType::FILE; break;

		case arrow::io::ObjectType::type::DIRECTORY: fileType = FileType::DIRECTORY; break;

		default: fileType = FileType::UNDEFINED; break;
		}

		return FileStatus(uri, fileType, stat_buf.size);
	} else {
		// TODO percy error handling
	}

	return FileStatus();
}

std::vector<FileStatus> HadoopFileSystem::Private::list(const Uri & uri, const FileFilter & filter) const {
	std::vector<FileStatus> response;

	if(uri.isValid() == false) {
		// TODO percy raise error
		return response;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	std::vector<arrow::io::HdfsPathInfo> listing;
	auto status = this->hdfs->ListDirectory(path.toString(true), &listing);

	// TODO percy check exception arrow::Status (status)
	if(status.ok()) {
		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			for(auto hdfsPathInfo : listing) {
				const Uri listedPath(uri.getScheme(), uri.getAuthority(), hdfsPathInfo.name);
				const FileStatus fileStatus = this->getFileStatus(listedPath);
				const bool pass = filter(fileStatus);

				if(pass) {
					response.push_back(fileStatus);
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			for(auto hdfsPathInfo : listing) {
				const Path fullPath(hdfsPathInfo.name);
				const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);

				const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
				const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);

				const FileStatus relativeFileStatus = this->getFileStatus(relativeUri);
				const FileStatus fullFileStatus(
					fullUri, relativeFileStatus.getFileType(), relativeFileStatus.getFileSize());

				const bool pass = filter(fullFileStatus);  // filter must use the full path

				if(pass) {
					response.push_back(relativeFileStatus);
				}
			}
		}
	} else {
		// TODO percy thrown exception
	}

	return response;
}

std::vector<Uri> HadoopFileSystem::Private::list(const Uri & uri, const std::string & wildcard) const {
	std::vector<Uri> response;

	if(uri.isValid() == false) {
		// TODO percy raise error
		return response;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	std::vector<arrow::io::HdfsPathInfo> listing;
	auto status = this->hdfs->ListDirectory(path.toString(true), &listing);

	// TODO percy check exception arrow::Status (status)
	if(status.ok()) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcardPath = wildcardPath.toString(false);
		const std::string host =
			this->fileSystemConnection.getConnectionProperty(HadoopFileSystemConnection::ConnectionProperty::HOST);
		const std::string port =
			this->fileSystemConnection.getConnectionProperty(HadoopFileSystemConnection::ConnectionProperty::PORT);
		const std::string auth = host + ":" + port;
		const Uri finalWildcardUri(FileSystemType::HDFS, auth, finalWildcardPath);
		const std::string finalWildcard = finalWildcardUri.toString(true);
		const std::string ignore = finalWildcardUri.getScheme() + "://" + auth;

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			for(auto hdfsPathInfo : listing) {
				const std::string fpath = hdfsPathInfo.name;
				const bool pass = WildcardFilter::match(fpath, finalWildcard);

				if(pass) {
					const std::string onlyPath = StringUtil::replace(fpath, ignore, "");
					const Uri listedPath(uri.getScheme(), uri.getAuthority(), onlyPath);
					response.push_back(listedPath);
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			for(auto hdfsPathInfo : listing) {
				const std::string fpath = hdfsPathInfo.name;
				const bool pass = WildcardFilter::match(fpath, finalWildcard);  // filter must use the full path

				if(pass) {
					const std::string onlyPath = StringUtil::replace(fpath, ignore, "");
					Path relativePath(onlyPath);
					relativePath = relativePath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());

					const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);

					response.push_back(relativeUri);
				}
			}
		}
	} else {
		// TODO percy thrown exception
	}

	return response;
}

std::vector<std::string> HadoopFileSystem::Private::listResourceNames(
	const Uri & uri, FileType fileType, const std::string & wildcard) const {
	std::vector<std::string> response;

	if(uri.isValid() == false) {
		// TODO percy raise error
		return response;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	std::vector<arrow::io::HdfsPathInfo> listing;
	auto status = this->hdfs->ListDirectory(path.toString(false), &listing);

	// TODO percy check exception arrow::Status (status)
	if(status.ok()) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);
		const FileTypeWildcardFilter filter(fileType, finalWildcard);

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			for(auto hdfsPathInfo : listing) {
				const Uri listedPath(uri.getScheme(), uri.getAuthority(), hdfsPathInfo.name);
				const FileStatus fileStatus = this->getFileStatus(listedPath);

				const bool pass = filter(fileStatus);

				if(pass) {
					response.push_back(listedPath.getPath().getResourceName());
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			for(auto hdfsPathInfo : listing) {
				const Path fullPath(hdfsPathInfo.name);
				const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);

				const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
				const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);

				const FileStatus relativeFileStatus = this->getFileStatus(relativeUri);
				const FileStatus fullFileStatus(
					fullUri, relativeFileStatus.getFileType(), relativeFileStatus.getFileSize());

				const bool pass = filter(fullFileStatus);  // filter must use the full path

				if(pass) {
					response.push_back(
						fullPath.getResourceName());  // resource name is the same for full paths or relative paths
				}
			}
		}
	} else {
		// TODO percy thrown exception
	}

	return response;
}

std::vector<std::string> HadoopFileSystem::Private::listResourceNames(
	const Uri & uri, const std::string & wildcard) const {
	std::vector<std::string> response;

	if(uri.isValid() == false) {
		// TODO percy raise error
		return response;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	std::vector<arrow::io::HdfsPathInfo> listing;
	auto status = this->hdfs->ListDirectory(path.toString(true), &listing);

	// TODO percy check exception arrow::Status (status)
	if(status.ok()) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			for(auto hdfsPathInfo : listing) {
				const bool pass = WildcardFilter::match(hdfsPathInfo.name, finalWildcard);

				if(pass) {
					const Uri listedPath(uri.getScheme(), uri.getAuthority(), hdfsPathInfo.name);
					response.push_back(listedPath.getPath().getResourceName());
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			for(auto hdfsPathInfo : listing) {
				const bool pass =
					WildcardFilter::match(hdfsPathInfo.name, finalWildcard);  // filter must use the full path

				if(pass) {
					const Path fullPath(hdfsPathInfo.name);

					response.push_back(
						fullPath.getResourceName());  // resource name is the same for full paths or relative paths
				}
			}
		}
	} else {
		// TODO percy thrown exception
	}

	return response;
}

bool HadoopFileSystem::Private::makeDirectory(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy raise error
		// return false;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	auto stat = this->hdfs->MakeDirectory(path.toString());

	// TODO check stat and thrown ex in case fail
	bool ret = stat.ok();
	return ret;
}

// TODO add bool recursive flag to FileSystemInterface
bool HadoopFileSystem::Private::remove(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy raise error
		return false;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	auto stat = this->hdfs->Delete(path.toString());

	// TODO check stat and thrown ex in case fail
	bool ret = stat.ok();
	return ret;
}

bool HadoopFileSystem::Private::move(const Uri & src, const Uri & dst) const {
	const Uri srcWithRoot(src.getScheme(), src.getAuthority(), this->root + src.getPath().toString());
	const Uri dstWithRoot(dst.getScheme(), dst.getAuthority(), this->root + dst.getPath().toString());

	const std::string srcFullPath = srcWithRoot.getPath().toString();
	const std::string dstFullPath = dstWithRoot.getPath().toString();

	auto stat = this->hdfs->Rename(srcFullPath, dstFullPath);

	// TODO check stat and thrown ex in case fail
	bool ret = stat.ok();
	return ret;
}

bool HadoopFileSystem::Private::truncateFile(const Uri & uri, long long length) const {
	const FileStatus fileStatus = this->getFileStatus(uri);

	if(fileStatus.getFileSize() == length) {
		return true;
	}

	if(fileStatus.getFileSize() > length) {  // truncate
		auto file = this->openReadable(uri);

		if(file == nullptr) {
			// TODO percy error handling
			return false;
		}

		// from object [         ] get [******|  ]
		const long long nbytes = length;

		std::shared_ptr<arrow::Buffer> buffer;
		file->Read(nbytes, &buffer);

		const auto closeFileOk = file->Close();

		if(closeFileOk.ok() == false) {
			// TODO percy error handling
			// TODO rollback changes
			return false;
		}

		// remove object [         ]
		const bool removeOk = this->remove(uri);

		if(removeOk == false) {
			// TODO percy error handling
			return false;
		}

		// move [******|  ] into a new object [******] and put [******] in the same path of original object [         ]
		auto outFile = this->openWriteable(uri);

		if(outFile == nullptr) {
			// TODO percy error handling
			// TODO rollback changes
			return false;
		}

		const auto writeOk = outFile->Write(buffer->data(), buffer->size());

		if(writeOk.ok() == false) {
			// TODO percy error handling
			// TODO rollback changes
			return false;
		}

		const auto flushOk = outFile->Flush();

		if(flushOk.ok() == false) {
			// TODO percy error handling
			// TODO rollback changes
			return false;
		}

		const auto closeOk = outFile->Close();

		if(closeOk.ok() == false) {
			// TODO percy error handling
			// TODO rollback changes
			return false;
		}

		return true;
	} else if(fileStatus.getFileSize() < length) {  // expand
													// TODO percy this use case is not needed yet
	}

	return false;
}

std::shared_ptr<arrow::io::RandomAccessFile> HadoopFileSystem::Private::openReadable(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy raise error
		// return false;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	std::shared_ptr<arrow::io::HdfsReadableFile> in_file;

	// path is the location on hdfs
	arrow::Status stat = this->hdfs->OpenReadable(path.toString(), &in_file);

	// TODO check stat and thrown FileSystemException

	return in_file;
}

std::shared_ptr<arrow::io::OutputStream> HadoopFileSystem::Private::openWriteable(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy raise error
		// return false;
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	std::shared_ptr<arrow::io::HdfsOutputStream> out_file;

	// path is the location on hdfs
	arrow::Status stat = hdfs->OpenWriteable(path.toString(), false, &out_file);

	// TODO check stat and thrown FileSystemException

	return out_file;
}
