/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "GoogleCloudStorage_p.h"

#include <iostream>

#include "arrow/buffer.h"

#include "ExceptionHandling/BlazingException.h"
#include "Util/StringUtil.h"

#include "ExceptionHandling/BlazingThread.h"
#include <chrono>

#include "Library/Logging/Logger.h"

// NOTE it seems all the directory objects in the Google Cloud storage has size 11
const long long SIZE_OF_OBJECT_DIRECTORY = 11;

namespace Logging = Library::Logging;

GoogleCloudStorage::Private::Private(const FileSystemConnection & fileSystemConnection, const Path & root)
	: root(root), gcsClient(nullptr) {
	// TODO percy improve & error handling
//	const bool connected = this->connect(fileSystemConnection);
	this->connect(fileSystemConnection);
}

GoogleCloudStorage::Private::~Private() {
	// TODO percy improve & error handling
//	const bool disconnected = this->disconnect();
	this->disconnect();
}

bool GoogleCloudStorage::Private::connect(const FileSystemConnection & fileSystemConnection) {
	using namespace GoogleCloudStorageConnection;

	if(fileSystemConnection.isValid() == false) {
		// TODO percy error handling
		return false;
	}

	this->fileSystemConnection = fileSystemConnection;

	const std::string projectId = fileSystemConnection.getConnectionProperty(ConnectionProperty::PROJECT_ID);
	const std::string useDefaultAdcJsonFile =
		fileSystemConnection.getConnectionProperty(ConnectionProperty::USE_DEFAULT_ADC_JSON_FILE);
	const std::string adcJsonFile = fileSystemConnection.getConnectionProperty(ConnectionProperty::ADC_JSON_FILE);

	google::cloud::StatusOr<std::shared_ptr<gcs::oauth2::Credentials>> credentials;
    if (useDefaultAdcJsonFile == "true") {
        credentials = gcs::oauth2::GoogleDefaultCredentials();
    }
    else {
        credentials = gcs::oauth2::CreateServiceAccountCredentialsFromFilePath(adcJsonFile);
    }

    if (!credentials) {
        const std::string error = "Couldn't create gcs::credentials for Project ID " + projectId + " status=" + credentials.status().message();
        std::cerr << error << std::endl;
        throw BlazingS3Exception(error);
    }

	gcs::ClientOptions opts(*credentials);

	auto connConf = opts.set_project_id(projectId);

	this->gcsClient = std::make_shared<gcs::Client>(connConf);

	const std::string bucket = this->getBucketName();
	bool validBucket = false;
	for(auto && bucket_metadata : this->gcsClient->ListBucketsForProject(projectId)) {
		if(!bucket_metadata) {
			this->fileSystemConnection = FileSystemConnection();
			throw std::runtime_error("Couldn't register the filesystem " + fileSystemConnection.toString() + " status=" + bucket_metadata.status().message());
		} else {
			const std::string element = bucket_metadata->name();
			if(element == bucket) {
				validBucket = true;
				break;
			}
		}
	}

	if(validBucket == false) {
		throw std::runtime_error("Invalid bucket, couldn't register the filesystem " + fileSystemConnection.toString());
	}

	return true;
}

bool GoogleCloudStorage::Private::disconnect() {
	// TODO percy
	return true;
}

FileSystemConnection GoogleCloudStorage::Private::getFileSystemConnection() const noexcept {
	return this->fileSystemConnection;
}

bool GoogleCloudStorage::Private::exists(const Uri & uri) const {
	using ::google::cloud::StatusOr;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	const std::string bucketName = this->getBucketName();
	// TODO here we are removing the first "/" char so we create a Google Cloud Storage object key using the path ...
	// improve this code
	std::string objectName = path.toString(true).substr(1, path.toString(true).size());

	StatusOr<gcs::ObjectMetadata> objectMetadata = this->gcsClient->GetObjectMetadata(bucketName, objectName);

	if(objectMetadata) {
		return true;
	} else {
		if(objectName[objectName.size() - 1] == '/') {  // if contains / at the end
			const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
			const Path folderPath = uriWithRoot.getPath();
		
			// NOTE only files is always true basically so we dont check it this is until we implement bucket listing
			// NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does
		
			// TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
			const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
			const std::string bucket = this->getBucketName();
		
			auto objectsOutcome = this->gcsClient->ListObjects(bucket, gcs::Prefix(objectKey));
		
			if(objectsOutcome.begin() != objectsOutcome.end()) {
				return true;
			} else {
				return false;
			}
		}
	}

	return true;
}

FileStatus GoogleCloudStorage::Private::getFileStatus(const Uri & uri) const {
	using ::google::cloud::StatusOr;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const std::string bucketName = this->getBucketName();
	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();
	const std::string bucket = this->getBucketName();
	std::string objectKey = path.toString(true).substr(1, path.toString(true).size());

	StatusOr<gcs::ObjectMetadata> objectMetadata = this->gcsClient->GetObjectMetadata(bucketName, objectKey);

	if(objectMetadata) {  // if success
		std::string contentType = objectMetadata->content_type();
		const long long contentLength = objectMetadata->size();
		FileType fileType = FileType::UNDEFINED;

		if((contentLength == SIZE_OF_OBJECT_DIRECTORY) || (contentLength == 0)) {  // may be a directory
			if(contentType == "text/plain") {									   // may be a text file
				gcs::ObjectReadStream stream = this->gcsClient->ReadObject(bucketName, objectKey);

				int count = 0;
				std::string line;
				while(std::getline(stream, line, '\n')) {
					++count;
				}

				// WARNING percy this test can fail if you have a text file with the word "placeholder" inside of it ...
				// TODO
				if(line == "placeholder") {  // apparently Google Cloud Storage put this string into a directory object
					fileType = FileType::DIRECTORY;
				} else {  // is a normal text file of size 11
					fileType = FileType::FILE;
				}
			} else {
				fileType = FileType::DIRECTORY;
			}

			const FileStatus fileStatus(uri, fileType, contentLength);
			return fileStatus;
		} else {  // is probably a file (e.g. application/octet-stream or text/x-python and so on ...
			const FileStatus fileStatus(uri, FileType::FILE, contentLength);
			return fileStatus;
		}
	} else {
		if(objectKey[objectKey.size() - 1] == '/') {  // if contains / at the end
			const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
			const Path folderPath = uriWithRoot.getPath();

			// NOTE only files is always true basically so we dont check it this is until we implement bucket listing
			// NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does
		
			// TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
			const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
			const std::string bucket = this->getBucketName();
		
			auto objectsOutcome = this->gcsClient->ListObjects(bucket, gcs::Prefix(objectKey));
		
			if(objectsOutcome.begin() != objectsOutcome.end()) {
				const FileStatus fileStatus(uri, FileType::DIRECTORY, 0);
				return fileStatus;
			}
		}
	}
	
	const FileStatus fileStatus(uri, FileType::UNDEFINED, 0);
	return fileStatus;
}

std::vector<FileStatus> GoogleCloudStorage::Private::list(const Uri & /*uri*/, const FileFilter & /*filter*/) const {
	//	std::vector<FileStatus> response;

	//	if (uri.isValid() == false) {
	//		throw BlazingInvalidPathException(uri);
	//	}

	//	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	//	const Path pathWithRoot = uriWithRoot.getPath();
	//	const Path folderPath = pathWithRoot;

	//	//NOTE only files is always true basically so we dont check it this is until we implement bucket listing
	//	//NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does

	//	//TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
	//	const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
	//	const std::string bucket = this->getBucketName();

	//	Aws::S3::Model::ListObjectsV2Request request;
	//	request.WithBucket(bucket);
	//	request.WithDelimiter("/"); //NOTE percy since we control how to create files in S3 we should use this
	//convention 	request.WithPrefix(objectKey);

	//	auto objectsOutcome = this->s3Client->ListObjectsV2(request);

	//	if (objectsOutcome.IsSuccess()) {
	//		if (this->root.isRoot()) { // if root is '/' then we don't need to replace the uris to relative paths
	//			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

	//			for (auto const &s3Object : objects) {
	//				//WARNING TODO percy there is no folders concept in S3 ... we should change Path::isFile::bool to
	//Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC 				const Path path("/" + s3Object.GetKey(), true); //TODO percy avoid
	//hardcoded string

	//				if (path != folderPath) {
	//					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
	//					const FileStatus fileStatus(entry, FileType::FILE, s3Object.GetSize());
	//					const bool pass = filter(fileStatus);

	//					if (pass) {
	//						response.push_back(fileStatus);
	//					}
	//				}
	//			}

	//			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders =
	//objectsOutcome.GetResult().GetCommonPrefixes();

	//			for (auto const &s3Folder : folders) {
	//				//WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
	//Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC 				const Path path("/" + s3Folder.GetPrefix(), true); //TODO percy
	//avoid hardcoded string 				const Uri entry(uri.getScheme(), uri.getAuthority(), path); 				const FileStatus
	//fileStatus(entry, FileType::DIRECTORY, 0); //TODO percy get info about the size of this kind of object 				const bool
	//pass = filter(fileStatus);

	//				if (pass) {
	//					response.push_back(fileStatus);
	//				}
	//			}
	//		} else { // if root is not '/' then we need to replace the uris to relative paths
	//			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

	//			for (auto const &s3Object : objects) {
	//				//WARNING TODO percy there is no folders concept in S3 ... we should change Path::isFile::bool to
	//Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC 				const Path fullPath("/" + s3Object.GetKey(), true); //TODO percy
	//avoid hardcoded string

	//				if (fullPath != folderPath) {
	//					const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
	//					const FileStatus fullFileStatus(fullUri, FileType::FILE, s3Object.GetSize());

	//					const bool pass = filter(fullFileStatus); //filter must use the full path

	//					if (pass) {
	//						const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
	//						const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);
	//						const FileStatus relativeFileStatus(relativeUri, fullFileStatus.getFileType(),
	//fullFileStatus.getFileSize());

	//						response.push_back(relativeFileStatus);
	//					}
	//				}
	//			}

	//			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders =
	//objectsOutcome.GetResult().GetCommonPrefixes();

	//			for (auto const &s3Folder : folders) {
	//				//WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
	//Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC 				const Path fullPath("/" + s3Folder.GetPrefix(), true); //TODO percy
	//avoid hardcoded string 				const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath); 				const FileStatus
	//fullFileStatus(fullUri, FileType::DIRECTORY, 0);

	//				const bool pass = filter(fullFileStatus); //filter must use the full path

	//				if (pass) {
	//					const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
	//					const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);
	//					const FileStatus relativeFileStatus(relativeUri, fullFileStatus.getFileType(),
	//fullFileStatus.getFileSize());

	//					response.push_back(relativeFileStatus);
	//				}
	//			}
	//		}
	//	} else {
	//		Logging::Logger().logError("GoogleCloudStorage::Private::list failed for URI: " + uri.toString());
	//		bool shouldRetry = objectsOutcome.GetError().ShouldRetry();
	//		if (shouldRetry){
	//			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
	//objectsOutcome.GetError().GetMessage() + "  SHOULD RETRY"); 		} else {
	//			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
	//objectsOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
	//		}
	//		throw BlazingFileSystemException("Could not list files found at " + uriWithRoot.toString() + ". Problem was "
	//+ objectsOutcome.GetError().GetExceptionName() + " : " + objectsOutcome.GetError().GetMessage());
	//	}

	//	return response;
	return {};
}

std::vector<Uri> GoogleCloudStorage::Private::list(const Uri & uri, const std::string & wildcard) const {
	std::vector<Uri> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path folderPath = uriWithRoot.getPath();

	// NOTE only files is always true basically so we dont check it this is until we implement bucket listing
	// NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does

	// TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
	const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
	const std::string bucket = this->getBucketName();

	auto objectsOutcome = this->gcsClient->ListObjects(bucket, gcs::Prefix(objectKey));

	if(objectsOutcome.begin() != objectsOutcome.end()) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);

		for(auto && object_metadata : objectsOutcome) {
			// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
			// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
			const Path path = Path("/" + object_metadata->name(), true); // TODO percy avoid hardcoded string

			if(path != folderPath) {
				const bool pass = WildcardFilter::match(path.toString(true), finalWildcard);

				if(pass) {
					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
					response.push_back(entry);
				}
			}
		}
		
		if (response.empty()) { // try again it might need to use a non prefix call
			objectsOutcome = this->gcsClient->ListObjects(bucket); // NOTE HERE -> non prefix call
			
			for(auto && object_metadata : objectsOutcome) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path = Path("/" + object_metadata->name(), true);  // TODO percy avoid hardcoded string
	
				if(path != folderPath) {
					const bool pass = WildcardFilter::match(path.toString(true), finalWildcard);
	
					if(pass) {
						const Uri entry(uri.getScheme(), uri.getAuthority(), path);
						response.push_back(entry);
					}
				}
			}
		}
	} else {
		// TODO percy
		//        Logging::Logger().logError("GoogleCloudStorage::Private::list failed for URI: " + uri.toString());
		//        bool shouldRetry = objectsOutcome.GetError().ShouldRetry();
		//        if (shouldRetry){
		//            Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
		//            objectsOutcome.GetError().GetMessage() + "  SHOULD RETRY");
		//        } else {
		//            Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
		//            objectsOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
		//        }
		//        throw BlazingFileSystemException("Could not list files found at " + uriWithRoot.toString() + ".
		//        Problem was " + objectsOutcome.GetError().GetExceptionName() + " : " +
		//        objectsOutcome.GetError().GetMessage());
	}

	return response;
}

std::vector<std::string> GoogleCloudStorage::Private::listResourceNames(
	const Uri & /*uri*/, FileType /*fileType*/, const std::string & /*wildcard*/) const {
	//	std::vector<std::string> response;

	//	if (uri.isValid() == false) {
	//		throw BlazingInvalidPathException(uri);
	//	}

	//	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	//	const Path folderPath = uriWithRoot.getPath();

	//	//NOTE only files is always true basically so we dont check it this is until we implement bucket listing
	//	//NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does

	//	//TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
	//	const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
	//	const std::string bucket = this->getBucketName();

	//	Aws::S3::Model::ListObjectsV2Request request;
	//	request.WithBucket(bucket);
	//	request.WithDelimiter("/"); //NOTE percy since we control how to create files in S3 we should use this
	//convention 	request.WithPrefix(objectKey);

	//	auto objectsOutcome = this->s3Client->ListObjectsV2(request);

	//	if (objectsOutcome.IsSuccess()) {
	//		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
	//		const std::string finalWildcard = wildcardPath.toString(false);
	//		const FileTypeWildcardFilter filter(fileType, finalWildcard);

	//		if (this->root.isRoot()) { // if root is '/' then we don't need to replace the uris to relative paths
	//			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

	//			for (auto const &s3Object : objects) {
	//				//WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
	//Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC 				const Path path("/" + s3Object.GetKey(), true); //TODO percy avoid
	//hardcoded string

	//				if (path != folderPath) {
	//					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
	//					const FileStatus fileStatus(entry, FileType::FILE, s3Object.GetSize());
	//					const bool pass = filter(fileStatus);

	//					if (pass) {
	//						response.push_back(fileStatus.getUri().getPath().getResourceName());
	//					}
	//				}
	//			}

	//			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders =
	//objectsOutcome.GetResult().GetCommonPrefixes();

	//			for (auto const &s3Folder : folders) {
	//				//WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
	//Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC 				const Path path("/" + s3Folder.GetPrefix(), true); //TODO percy
	//avoid hardcoded string

	//				if (path != folderPath) {
	//					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
	//					const FileStatus fileStatus(entry, FileType::DIRECTORY, 0); //TODO percy get info about the size of this
	//kind of object 					const bool pass = filter(fileStatus);

	//					if (pass) {
	//						response.push_back(fileStatus.getUri().getPath().getResourceName());
	//					}
	//				}
	//			}
	//		} else { // if root is not '/' then we need to replace the uris to relative paths
	//			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

	//			for (auto const &s3Object : objects) {
	//				//WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
	//Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC 				const Path fullPath("/" + s3Object.GetKey(), true); //TODO percy
	//avoid hardcoded string

	//				if (fullPath != folderPath) {
	//					const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
	//					const FileStatus fullFileStatus(fullUri, FileType::FILE, s3Object.GetSize());

	//					const bool pass = filter(fullFileStatus); //filter must use the full path

	//					if (pass) {
	//						response.push_back(fullFileStatus.getUri().getPath().getResourceName()); // resource name is the same
	//for full paths or relative paths
	//					}
	//				}
	//			}

	//			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders =
	//objectsOutcome.GetResult().GetCommonPrefixes();

	//			for (auto const &s3Folder : folders) {
	//				//WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
	//Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC 				const Path fullPath("/" + s3Folder.GetPrefix(), true); //TODO percy
	//avoid hardcoded string

	//				if (fullPath != folderPath) {
	//					const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
	//					const FileStatus fullFileStatus(fullUri, FileType::DIRECTORY, 0); //TODO percy get info about the size
	//of this kind of object 					const bool pass = filter(fullFileStatus);

	//					if (pass) {
	//						response.push_back(fullFileStatus.getUri().getPath().getResourceName()); // resource name is the same
	//for full paths or relative paths
	//					}
	//				}
	//			}
	//		}
	//	} else {
	//		Logging::Logger().logError("GoogleCloudStorage::Private::listResourceNames failed for URI: " +
	//uri.toString()); 		bool shouldRetry = objectsOutcome.GetError().ShouldRetry(); 		if (shouldRetry){
	//			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
	//objectsOutcome.GetError().GetMessage() + "  SHOULD RETRY"); 		} else {
	//			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
	//objectsOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
	//		}
	//		throw BlazingFileSystemException("Could not list resources found at " + uriWithRoot.toString() + ". Problem
	//was " + objectsOutcome.GetError().GetExceptionName() + " : " + objectsOutcome.GetError().GetMessage());
	//	}

	//	return response;
	return {};
}

std::vector<std::string> GoogleCloudStorage::Private::listResourceNames(
	const Uri & uri, const std::string & wildcard) const {
	std::vector<std::string> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path folderPath = uriWithRoot.getPath();

	// NOTE only files is always true basically so we dont check it this is until we implement bucket listing
	// NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does

	// TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
	const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
	const std::string bucket = this->getBucketName();

	// TODO percy see how gcs manage the delimiters
	// request.WithDelimiter("/"); //NOTE percy since we control how to create files in S3 we should use this convention

	auto objectsOutcome = this->gcsClient->ListObjects(bucket, gcs::Prefix(objectKey));

	if(objectsOutcome.begin() != objectsOutcome.end()) {
		const Path wildcardPath = Path(uriWithRoot.getPath() + wildcard);
		const std::string finalWildcard = wildcardPath.toString(true);

		for(auto && object_metadata : objectsOutcome) {
			// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
			// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
			const Path path = Path("/" + object_metadata->name(), true);  // TODO percy avoid hardcoded string

			if(path != folderPath) {
				const Uri entry(uri.getScheme(), uri.getAuthority(), path);
				const bool pass = WildcardFilter::match(path.toString(true), finalWildcard);

				if(pass) {
					response.push_back(entry.getPath().getResourceName());
				}
			}
		}
	} else {
		// TODO percy
		// Logging::Logger().logError("GoogleCloudStorage::Private::listResourceNames failed for URI: " +
		// uri.toString()); bool shouldRetry = objectsOutcome.GetError().ShouldRetry(); if (shouldRetry){
		//    Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
		//    objectsOutcome.GetError().GetMessage() + "  SHOULD RETRY");
		//} else {
		//    Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
		//    objectsOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
		//}
		// throw BlazingFileSystemException("Could not list resources found at " + uriWithRoot.toString() + ". Problem
		// was " + objectsOutcome.GetError().GetExceptionName() + " : " + objectsOutcome.GetError().GetMessage());
	}

	return response;
}

bool GoogleCloudStorage::Private::makeDirectory(const Uri & uri) const {
	using ::google::cloud::StatusOr;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();
	const std::string bucket = this->getBucketName();

	std::string mutablePath = path.toString(true);  // TODO ugly ... fix StringUtil api
	std::string rootChar = std::string("/");
	bool isFolder = StringUtil::endsWith(mutablePath, rootChar);

	if(isFolder == false) {
		throw BlazingS3Exception("Failed to make " + uri.toString() + " because path does not end in a slash");
	}

	mutablePath = mutablePath.substr(1, mutablePath.size());

	StatusOr<gcs::ObjectMetadata> object_metadata = this->gcsClient->InsertObject(bucket, mutablePath, std::move(""));

	// TODO percy support encrypted for Google Cloud Storage
	//    if (this->isEncrypted()) {
	//        request.WithServerSideEncryption(this->serverSideEncryption());

	//        if (this->isAWSKMSEncrypted()) {
	//            request.WithSSEKMSKeyId(this->getSSEKMSKeyId());
	//        }
	//    }

	if(object_metadata) {  // if success
		return true;
	} else {
		// TODO percy
		//        Logging::Logger().logError("GoogleCloudStorage::Private::makeDirectory failed for URI: " +
		//        uri.toString()); bool shouldRetry = result.GetError().ShouldRetry(); if (shouldRetry){
		//            Logging::Logger().logError(result.GetError().GetExceptionName() + " : " +
		//            result.GetError().GetMessage() + "  SHOULD RETRY");
		//        } else {
		//            Logging::Logger().logError(result.GetError().GetExceptionName() + " : " +
		//            result.GetError().GetMessage() + "  SHOULD NOT RETRY");
		//        }
		auto result = object_metadata.status();
		throw BlazingS3Exception(
			"Could not make directory " + uriWithRoot.toString() + ". Problem was " + result.message());
	}

	return false;
}

bool GoogleCloudStorage::Private::remove(const Uri & /*uri*/) const {
	//	if (uri.isValid() == false) {
	//		throw BlazingInvalidPathException(uri);
	//	}

	//	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	//	const Path path = uriWithRoot.getPath();
	//	const std::string bucket = this->getBucketName();

	//	std::string objectKey = path.toString().substr(1, path.toString().size());

	//	if (objectKey[objectKey.size() - 1] != '/') { // if we are not sure if its actually a directory, lets find out
	//		FileStatus status = this->getFileStatus(uri);

	//		if (status.isDirectory()) {
	//			objectKey += '/';
	//		}
	//	}

	//	Aws::S3::Model::DeleteObjectRequest request;
	//	request.WithBucket(bucket);
	//	request.WithKey(objectKey);

	//	auto result = this->s3Client->DeleteObject(request);

	//	if (result.IsSuccess()) {
	//		return true;
	//	} else {
	//		Logging::Logger().logError("GoogleCloudStorage::Private::remove failed for URI: " + uri.toString());
	//		bool shouldRetry = result.GetError().ShouldRetry();
	//		if (shouldRetry){
	//			Logging::Logger().logError(result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage() + "
	//SHOULD RETRY"); 		} else { 			Logging::Logger().logError(result.GetError().GetExceptionName() + " : " +
	//result.GetError().GetMessage() + "  SHOULD NOT RETRY");
	//		}
	//		throw BlazingS3Exception("Could not remove " + uriWithRoot.toString() + ". Problem was " +
	//result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage());

	//		return false;
	//	}
	return false;
}

bool GoogleCloudStorage::Private::move(const Uri & /*src*/, const Uri & /*dst*/) const {
	//	if (src.isValid() == false) {
	//		throw BlazingInvalidPathException(src);
	//	}

	//	if (dst.isValid() == false) {
	//		throw BlazingInvalidPathException(dst);
	//	}
	//	const std::string bucket = this->getBucketName();

	//	const Uri srcWithRoot(src.getScheme(), src.getAuthority(), this->root + src.getPath().toString());
	//	const Uri dstWithRoot(dst.getScheme(), dst.getAuthority(), this->root + dst.getPath().toString());

	//	const std::string srcFullPath = srcWithRoot.getPath().toString();
	//	const std::string dstFullPath = dstWithRoot.getPath().toString();

	//	std::string fromKey = srcFullPath.substr(1, src.toString().size());
	//	std::string toKey = dstFullPath.substr(1, dst.toString().size());

	//	std::string source = bucket + "/" + fromKey;

	//	Aws::S3::Model::CopyObjectRequest request;

	//	request.WithBucket(bucket);
	//	request.WithKey(toKey);
	//	request.WithCopySource(source);

	//	if (this->isEncrypted()) {
	//		request.WithServerSideEncryption(this->serverSideEncryption());

	//		if (this->isAWSKMSEncrypted()) {
	//			request.WithSSEKMSKeyId(this->getSSEKMSKeyId());
	//		}
	//	}

	//	auto result = this->s3Client->CopyObject(request);

	//	if (result.IsSuccess()) {
	//		const bool deleted = this->remove(src);

	//		if (deleted) {
	//			return true;
	//		} else {

	//			throw BlazingS3Exception("Could not remove " + src.toString() + " after making copy to " +
	//dst.toString());
	//		}
	//	} else {
	//		Logging::Logger().logError("GoogleCloudStorage::Private::move failed for src URI: " + src.toString() + "  and
	//dst UTI: " + dst.toString()); 		bool shouldRetry = result.GetError().ShouldRetry(); 		if (shouldRetry){
	//			Logging::Logger().logError(result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage() + "
	//SHOULD RETRY"); 		} else { 			Logging::Logger().logError(result.GetError().GetExceptionName() + " : " +
	//result.GetError().GetMessage() + "  SHOULD NOT RETRY");
	//		}
	//		throw BlazingS3Exception("Could not move " + src.toString() + " to " + dst.toString() + ". Problem was " +
	//result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage());

	//		return false;
	//	}
	return false;
}

// TODO: truncate file can't be rolled back easily as it stands
// an easier way  might be to use move instead of remove
bool GoogleCloudStorage::Private::truncateFile(const Uri & /*uri*/, long long /*length*/) const {
	//	if (uri.isValid() == false) {
	//		throw BlazingInvalidPathException(uri);
	//	}

	//	const FileStatus fileStatus = this->getFileStatus(uri);

	//	if (fileStatus.getFileSize() == length) {
	//		return true;
	//	}

	//	if (fileStatus.getFileSize() > length) { // truncate
	//		std::shared_ptr<S3ReadableFile> file;
	//		const bool ok = this->openReadable(uri, &file);

	//		if (ok == false) {
	//			throw BlazingS3Exception("Could not open " + uri.toString() + " for truncating.");
	//		}

	//		//from object [         ] get [******|  ]
	//		const long long nbytes = length;

	//		std::shared_ptr<arrow::Buffer> buffer;
	//		file->Read(nbytes, &buffer);

	//		const auto closeFileOk = file->Close();

	//		if (closeFileOk.ok() == false) {
	//			throw BlazingS3Exception("Could not close " + uri.toString() + " for truncating.");
	//			return false;
	//		}

	//		//remove object [         ]
	//		const bool removeOk = this->remove(uri);

	//		if (removeOk == false) {
	//			throw BlazingS3Exception(
	//				"Could not remove previous " + uri.toString() + " for truncating (S3 has no truncate so we must copy
	//delete old put in truncated version).");
	//		}

	//		//move [******|  ] into a new object [******] and put [******] in the same path of original object [ ]
	//		std::shared_ptr<S3OutputStream> outFile;

	//		const bool outOk = this->openWriteable(uri, &outFile);

	//		if (outOk == false) {
	//			throw BlazingS3Exception("Could not open " + uri.toString() + " for writing truncated data");
	//		}

	//		const auto writeOk = outFile->Write(buffer->data(), buffer->size());

	//		if (writeOk.ok() == false) {
	//			throw BlazingS3Exception("Could not write to " + uri.toString() + " for writing truncated data");
	//		}

	//		const auto flushOk = outFile->Flush();

	//		if (flushOk.ok() == false) {
	//			throw BlazingS3Exception("Could not flush " + uri.toString() + " for writing truncated data");
	//		}

	//		const auto closeOk = outFile->Close();

	//		if (closeOk.ok() == false) {
	//			throw BlazingS3Exception("Could not close " + uri.toString() + " after writing truncated data");
	//		}

	//		return true;
	//	} else if (fileStatus.getFileSize() < length) { // expand
	//		//TODO percy this use case is not needed yet
	//	}

	return false;
}

bool GoogleCloudStorage::Private::openReadable(
	const Uri & uri, std::shared_ptr<GoogleCloudStorageReadableFile> * file) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();
	const std::string objectKey = path.toString(true).substr(1, path.toString(true).size());
	const std::string bucketName = this->getBucketName();
	// TODO: S3ReadableFile currentl has no validity check add it and throw errors here
	*file = std::make_shared<GoogleCloudStorageReadableFile>(this->gcsClient, bucketName, objectKey);
	return (*file)->isValid();
}

bool GoogleCloudStorage::Private::openWriteable(
	const Uri & /*uri*/, std::shared_ptr<GoogleCloudStorageOutputStream> * /*file*/) const {
	//	if (uri.isValid() == false) {
	//		throw BlazingInvalidPathException(uri);
	//	}

	//	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	//	const Path path = uriWithRoot.getPath();
	//	const std::string objectKey = path.toString(true).substr(1, path.toString(true).size());
	//	const std::string bucketName = this->getBucketName();
	//	//TODO: S3Outputstream currentl has no validity check add it and throw errors here
	//	*file = std::make_shared < S3OutputStream > (bucketName, objectKey, this->s3Client);

	return true;
}

const std::string GoogleCloudStorage::Private::getProjectId() const {
	using namespace GoogleCloudStorageConnection;
	return this->fileSystemConnection.getConnectionProperty(ConnectionProperty::PROJECT_ID);
}

const std::string GoogleCloudStorage::Private::getBucketName() const {
	using namespace GoogleCloudStorageConnection;
	return this->fileSystemConnection.getConnectionProperty(ConnectionProperty::BUCKET_NAME);
}

bool GoogleCloudStorage::Private::useDefaultAdcJsonFile() const {
	using namespace GoogleCloudStorageConnection;
	//	return (this->encryptionType() != EncryptionType::NONE) && (this->encryptionType() !=
	//EncryptionType::UNDEFINED);
	return false;
}

const std::string GoogleCloudStorage::Private::getAdcJsonFile() const {
	using namespace GoogleCloudStorageConnection;
	//	const std::string kmsKey =
	//this->fileSystemConnection.getConnectionProperty(ConnectionProperty::KMS_KEY_AMAZON_RESOURCE_NAME); 	return kmsKey;
	return "";
}
