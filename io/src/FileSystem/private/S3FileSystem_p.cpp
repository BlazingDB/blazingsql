/*
 * Copyright 2017-2020 BlazingDB, Inc.
 *     Copyright 2018 Felipe Aramburu <felipe@blazingdb.com>
 *     Copyright 2018-2020 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 *     Copyright 2018 William Malpica <william@blazingdb.com>
 */

#include "S3FileSystem_p.h"

#include <iostream>

#include "arrow/buffer.h"

#include "aws/s3/model/HeadObjectRequest.h"
#include <aws/core/Aws.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>

#include "aws/s3/model/HeadBucketRequest.h"
#include "aws/s3/model/ListObjectsRequest.h"
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/s3-encryption/CryptoConfiguration.h>
#include <aws/s3-encryption/S3EncryptionClient.h>
#include <aws/s3-encryption/materials/KMSEncryptionMaterials.h>
#include <aws/s3-encryption/materials/SimpleEncryptionMaterials.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/GetBucketLocationRequest.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "ExceptionHandling/BlazingException.h"
#include "Util/StringUtil.h"

#include "ExceptionHandling/BlazingThread.h"
#include <chrono>

#include "Library/Logging/Logger.h"
namespace Logging = Library::Logging;

struct RegionResult {
	bool valid;
	std::string regionName;
	std::string errorMessage;
};

std::shared_ptr<Aws::S3::S3Client> make_s3_client(
		const std::string & region,
		std::shared_ptr< Aws::Auth::AWSCredentials> credentials,
		const std::string & endpointOverride,
		long connectTimeoutMs = -1,
		long requestTimeoutMs = -1) {
	
	// TODO Percy Rommel use configuration files instead of magic numbers/strings
	// here we can make changes to the client configuration
	
	auto clientConfig = Aws::Client::ClientConfiguration();
	clientConfig.region = region;
	
	if (connectTimeoutMs > 0) {
		clientConfig.connectTimeoutMs = connectTimeoutMs;
	}
	
	if (requestTimeoutMs > 0) {
		clientConfig.requestTimeoutMs = requestTimeoutMs;
	}
	
	bool useVirtualAddressing = true;
	
	if (endpointOverride.empty() == false) {
		clientConfig.endpointOverride = endpointOverride;
		
		// NOTE When use endpointOverride we need to set useVirtualAddressing to true for the client ctor
		useVirtualAddressing = false;
	}
	
	std::shared_ptr<Aws::S3::S3Client> s3Client;
	Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;
	
	if(credentials == nullptr){
		s3Client = std::make_shared<Aws::S3::S3Client>(clientConfig, signPayloads, useVirtualAddressing);
	}else{
		s3Client = std::make_shared<Aws::S3::S3Client>(*credentials, clientConfig, signPayloads, useVirtualAddressing);
	}
	
	return s3Client;
}

RegionResult getRegion(std::string bucketName,std::shared_ptr< Aws::Auth::AWSCredentials> credentials, const std::string & endpointOverride) {
	RegionResult regionResult;

	// NEVER change the region value for this getRegion method | use US-Standard region (us-east-1) to get any bucket region
	std::shared_ptr<Aws::S3::S3Client> s3Client = make_s3_client("us-east-1", credentials, endpointOverride, 60000, 30000);

	Aws::S3::Model::GetBucketLocationRequest location_request;
	location_request.WithBucket(bucketName);

	const auto locationRequestOutcome = s3Client->GetBucketLocation(location_request);

	if(locationRequestOutcome.IsSuccess()) {
		const Aws::S3::Model::BucketLocationConstraint regionEnum =
			locationRequestOutcome.GetResult().GetLocationConstraint();
		regionResult.regionName =
			Aws::S3::Model::BucketLocationConstraintMapper::GetNameForBucketLocationConstraint(regionEnum);
		regionResult.valid = true;

		// NOTE empty string (for the US East (N. Virginia) region)
		// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETlocation.html
		if(regionResult.regionName.empty()) {
			regionResult.regionName = "us-east-1";
			//			std::cout << "Bucket region: US East (N. Virginia) - us-east-1" << std::endl;
		} else {
			//			std::cout << "Bucket region: " << regionResult.regionName << std::endl;
		}
	} else {
		const std::string error = locationRequestOutcome.GetError().GetMessage();
		const std::string errorPattern =
			"Unable to parse ExceptionName: AuthorizationHeaderMalformed Message: The authorization header is "
			"malformed; the region 'us-east-1' is wrong; expecting '";
		const bool errorContainsRegion = StringUtil::beginsWith(std::string(error), errorPattern);

		if(errorContainsRegion) {
			regionResult.valid = true;
			const std::vector<std::string> errorTokens = StringUtil::split(error, "'");
			regionResult.regionName = errorTokens[errorTokens.size() - 2];
		} else {
			regionResult.errorMessage =
				"Could not determine the region of the bucket " + bucketName +
				" You MUST be the owner for a bucket to not specify its region. Received error: " + error;
			regionResult.valid = false;

			Logging::Logger().logError(regionResult.errorMessage);
		}
	}
	//	std::cout<<"Region for bucket name"<<bucketName<<" is: "<<regionResult.regionName<<std::endl;

	return regionResult;
}

S3FileSystem::Private::Private(const FileSystemConnection & fileSystemConnection, const Path & root)
	: s3Client(nullptr), root(root) {
	// TODO percy improve & error handling
	const bool connected = this->connect(fileSystemConnection);
}

S3FileSystem::Private::~Private() {
	// TODO percy improve & error handling
	const bool disconnected = this->disconnect();
}

bool S3FileSystem::Private::connect(const FileSystemConnection & fileSystemConnection) {
	using namespace S3FileSystemConnection;

	if(fileSystemConnection.isValid() == false) {
		// TODO percy error handling
		return false;
	}

	const auto & connectionProperties = fileSystemConnection.getConnectionProperties();

	const std::string bucketName = fileSystemConnection.getConnectionProperty(ConnectionProperty::BUCKET_NAME);
	const EncryptionType encryptionType =
		encryptionTypeFromName(fileSystemConnection.getConnectionProperty(ConnectionProperty::ENCRYPTION_TYPE));
	const std::string kmsKeyAmazonResourceName =
		fileSystemConnection.getConnectionProperty(ConnectionProperty::KMS_KEY_AMAZON_RESOURCE_NAME);
	const std::string accessKeyId = fileSystemConnection.getConnectionProperty(ConnectionProperty::ACCESS_KEY_ID);
	const std::string secretKey = fileSystemConnection.getConnectionProperty(ConnectionProperty::SECRET_KEY);
	const std::string sessionToken = fileSystemConnection.getConnectionProperty(ConnectionProperty::SESSION_TOKEN);
	const std::string endpointOverride = fileSystemConnection.getConnectionProperty(ConnectionProperty::ENDPOINT_OVERRIDE);

	// TODO percy check valid conn properties: do this in the fs::isValidConn or inside fsConnection (get rid of strings
	// map: that is a bad pattern)
	std::shared_ptr<Aws::Auth::AWSCredentials> credentials = nullptr;
	if(accessKeyId != ""){
		credentials = std::make_shared<Aws::Auth::AWSCredentials>(accessKeyId, secretKey, sessionToken);
	}

	RegionResult regionResult;

	if(credentials != nullptr){
		regionResult = getRegion(bucketName, credentials, endpointOverride);
	}else{
		regionResult.regionName = "us-east-1";
		regionResult.valid = true;
	}

	// TODO percy error handling
	if(regionResult.valid == false) {
		this->disconnect();
		throw std::runtime_error(
			"Error getting region from bucket " + bucketName + ". Filesystem " + fileSystemConnection.toString());
	}

	this->s3Client = make_s3_client(regionResult.regionName, credentials, endpointOverride, 60000, 30000);

	// TODO NOTE This code is when we need to support client side encryption (currently only support server side
	// encryption) 	switch (encryptionType) { 		case EncryptionType::NONE: { 			this->s3Client =
	//std::make_shared<Aws::S3::S3Client>(credentials, clientConfig);
	//		}
	//		break;
	//
	//		case EncryptionType::AES_256: {
	//			const auto masterKey = Aws::Utils::Crypto::SymmetricCipher::GenerateKey();
	//			const auto simpleMaterials =
	//Aws::MakeShared<Aws::S3Encryption::Materials::SimpleEncryptionMaterials>("s3Encryption", masterKey); 			const
	//Aws::S3Encryption::CryptoConfiguration cryptoConfiguration(Aws::S3Encryption::StorageMethod::METADATA,
	//Aws::S3Encryption::CryptoMode::AUTHENTICATED_ENCRYPTION);
	//
	//			this->s3Client = std::make_shared<Aws::S3Encryption::S3EncryptionClient>(simpleMaterials,
	//cryptoConfiguration, credentials, clientConfig);
	//		}
	//		break;
	//
	//		case EncryptionType::AWS_KMS: {
	//			if (kmsKeyAmazonResourceName.empty()) {
	//				//TODO percy error handling, invalid master key for aws kms encryption
	//
	//				return false;
	//			}
	//
	//			auto kmsMaterials = Aws::MakeShared<Aws::S3Encryption::Materials::KMSEncryptionMaterials>("s3Encryption",
	//kmsKeyAmazonResourceName); 			const Aws::S3Encryption::CryptoConfiguration
	//cryptoConfiguration(Aws::S3Encryption::StorageMethod::INSTRUCTION_FILE,
	//Aws::S3Encryption::CryptoMode::STRICT_AUTHENTICATED_ENCRYPTION);
	//
	//			this->s3Client = std::make_shared<Aws::S3Encryption::S3EncryptionClient>(kmsMaterials, cryptoConfiguration,
	//credentials, clientConfig);
	//		}
	//		break;
	//
	//		case EncryptionType::UNDEFINED: {
	//			//TODO percy error handling, notify the user the fs connection may be invalid
	//		}
	//		break;
	//
	//		default: {
	//			//TODO percy error handling (if we get here we must thrown an exception
	//		}
	//		break;
	//	}

	this->regionName = regionResult.regionName;
	this->fileSystemConnection = fileSystemConnection;

	return true;
}

bool S3FileSystem::Private::disconnect() {
	// TODO percy delete the s3client
	return true;
}

FileSystemConnection S3FileSystem::Private::getFileSystemConnection() const noexcept {
	return this->fileSystemConnection;
}

bool S3FileSystem::Private::exists(const Uri & uri) const {

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());

	const Path path = uriWithRoot.getPath();
	const std::string bucket = this->getBucketName();

	// TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
	std::string objectKey = path.toString(true).substr(1, path.toString(true).size());

	Aws::S3::Model::HeadObjectRequest request;
	request.WithBucket(bucket);
	request.WithKey(objectKey);

	Aws::S3::Model::HeadObjectOutcome result = this->s3Client->HeadObject(request);

	if(result.IsSuccess()) {
		return true;
	} else {
		if(objectKey[objectKey.size() - 1] !=
			'/') {  // lets see if it was actually a directory and we simply did not have the slash at the end
			objectKey += '/';

			request.WithKey(objectKey);

			Aws::S3::Model::HeadObjectOutcome result2 = this->s3Client->HeadObject(request);

			if(result2.IsSuccess()) {
				return true;
			} else {
				return false;
			}
		} else {  // if contains / at the end
			Aws::S3::Model::ListObjectsV2Request request;
			request.WithBucket(bucket);
			request.WithDelimiter(
				"/");  // NOTE percy since we control how to create files in S3 we should use this convention
			request.WithPrefix(objectKey);

			auto objectsOutcome = this->s3Client->ListObjectsV2(request);

			if(objectsOutcome.IsSuccess()) {
				return true;
			}
		}
	}

	return false;
}

FileStatus S3FileSystem::Private::getFileStatus(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();
	const std::string bucket = this->getBucketName();
	std::string objectKey = path.toString(true).substr(1, path.toString(true).size());

	Aws::S3::Model::HeadObjectRequest request;
	request.WithBucket(bucket);
	request.WithKey(objectKey);

	Aws::S3::Model::HeadObjectOutcome outcome = this->s3Client->HeadObject(request);

	if(outcome.IsSuccess()) {
		// is an object

		Aws::S3::Model::HeadObjectResult result = outcome.GetResult();

		std::string contentType = result.GetContentType();
		long long contentLength = result.GetContentLength();

		if(objectKey[objectKey.size() - 1] == '/' || contentType == "application/x-directory") {
			const FileStatus fileStatus(uri, FileType::DIRECTORY, contentLength);
			return fileStatus;
		} else {
			const FileStatus fileStatus(uri, FileType::FILE, contentLength);
			return fileStatus;
		}
	} else {
		if(objectKey[objectKey.size() - 1] !=
			'/') {  // lets see if it was actually a directory and we simply did not have the slash at the end
			objectKey += '/';

			request.WithKey(objectKey);

			Aws::S3::Model::HeadObjectOutcome outcome = this->s3Client->HeadObject(request);

			if(outcome.IsSuccess()) {
				const FileStatus fileStatus(uri, FileType::DIRECTORY, 0);
				return fileStatus;
			} else {
				//				bool shouldRetry = outcome.GetError().ShouldRetry();
				//				if (shouldRetry){
				//					std::cout<<"ERROR: "<<outcome.GetError().GetExceptionName()<<" :
				//"<<outcome.GetError().GetMessage()<<"  SHOULD RETRY"<<std::endl; 				} else { 					std::cout<<"ERROR:
				//"<<outcome.GetError().GetExceptionName()<<" : "<<outcome.GetError().GetMessage()<<"  SHOULD NOT
				//RETRY"<<std::endl;
				//				}
				throw BlazingFileNotFoundException(uriWithRoot);
			}
		} else {  // if contains / at the end
			Aws::S3::Model::ListObjectsV2Request request;
			request.WithBucket(bucket);
			request.WithDelimiter(
				"/");  // NOTE percy since we control how to create files in S3 we should use this convention
			request.WithPrefix(objectKey);

			auto objectsOutcome = this->s3Client->ListObjectsV2(request);

			if(objectsOutcome.IsSuccess()) {
				const FileStatus fileStatus(uri, FileType::DIRECTORY, 0);
				return fileStatus;
			}
		}
		//			bool shouldRetry = outcome.GetError().ShouldRetry();
		//			if (shouldRetry){
		//				std::cout<<"ERROR: "<<outcome.GetError().GetExceptionName()<<" :
		//"<<outcome.GetError().GetMessage()<<"  SHOULD RETRY"<<std::endl; 			} else { 				std::cout<<"ERROR:
		//"<<outcome.GetError().GetExceptionName()<<" : "<<outcome.GetError().GetMessage()<<"  SHOULD NOT
		//RETRY"<<std::endl;
		//			}
		throw BlazingFileNotFoundException(uriWithRoot);
	}
}

std::vector<FileStatus> S3FileSystem::Private::list(const Uri & uri, const FileFilter & filter) const {
	std::vector<FileStatus> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path pathWithRoot = uriWithRoot.getPath();
	const Path folderPath = pathWithRoot.getPathWithNormalizedFolderConvention();

	// NOTE only files is always true basically so we dont check it this is until we implement bucket listing
	// NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does

	// TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
	const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
	const std::string bucket = this->getBucketName();

	Aws::S3::Model::ListObjectsV2Request request;
	request.WithBucket(bucket);
	request.WithDelimiter("/");  // NOTE percy since we control how to create files in S3 we should use this convention
	request.WithPrefix(objectKey);

	auto objectsOutcome = this->s3Client->ListObjectsV2(request);

	if(objectsOutcome.IsSuccess()) {
		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

			for(auto const & s3Object : objects) {
				// WARNING TODO percy there is no folders concept in S3 ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path("/" + s3Object.GetKey(), true);  // TODO percy avoid hardcoded string

				if(path != folderPath) {
					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
					const FileStatus fileStatus(entry, FileType::FILE, s3Object.GetSize());
					const bool pass = filter(fileStatus);

					if(pass) {
						response.push_back(fileStatus);
					}
				}
			}

			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders = objectsOutcome.GetResult().GetCommonPrefixes();

			for(auto const & s3Folder : folders) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path("/" + s3Folder.GetPrefix(), true);  // TODO percy avoid hardcoded string
				const Uri entry(uri.getScheme(), uri.getAuthority(), path);
				const FileStatus fileStatus(
					entry, FileType::DIRECTORY, 0);  // TODO percy get info about the size of this kind of object
				const bool pass = filter(fileStatus);

				if(pass) {
					response.push_back(fileStatus);
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

			for(auto const & s3Object : objects) {
				// WARNING TODO percy there is no folders concept in S3 ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path fullPath("/" + s3Object.GetKey(), true);  // TODO percy avoid hardcoded string

				if(fullPath != folderPath) {
					const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
					const FileStatus fullFileStatus(fullUri, FileType::FILE, s3Object.GetSize());

					const bool pass = filter(fullFileStatus);  // filter must use the full path

					if(pass) {
						const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
						const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);
						const FileStatus relativeFileStatus(
							relativeUri, fullFileStatus.getFileType(), fullFileStatus.getFileSize());

						response.push_back(relativeFileStatus);
					}
				}
			}

			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders = objectsOutcome.GetResult().GetCommonPrefixes();

			for(auto const & s3Folder : folders) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path fullPath("/" + s3Folder.GetPrefix(), true);  // TODO percy avoid hardcoded string
				const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
				const FileStatus fullFileStatus(fullUri, FileType::DIRECTORY, 0);

				const bool pass = filter(fullFileStatus);  // filter must use the full path

				if(pass) {
					const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
					const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);
					const FileStatus relativeFileStatus(
						relativeUri, fullFileStatus.getFileType(), fullFileStatus.getFileSize());

					response.push_back(relativeFileStatus);
				}
			}
		}
	} else {
		Logging::Logger().logError("S3FileSystem::Private::list failed for URI: " + uri.toString());
		bool shouldRetry = objectsOutcome.GetError().ShouldRetry();
		if(shouldRetry) {
			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
									   objectsOutcome.GetError().GetMessage() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
									   objectsOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}
		throw BlazingFileSystemException("Could not list files found at " + uriWithRoot.toString() + ". Problem was " +
										 objectsOutcome.GetError().GetExceptionName() + " : " +
										 objectsOutcome.GetError().GetMessage());
	}

	return response;
}

std::vector<Uri> S3FileSystem::Private::list(const Uri & uri, const std::string & wildcard) const {
	std::vector<Uri> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path pathWithRoot = uriWithRoot.getPath();
	const Path folderPath = pathWithRoot.getPathWithNormalizedFolderConvention();

	// NOTE only files is always true basically so we dont check it this is until we implement bucket listing
	// NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does

	// TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
	const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
	const std::string bucket = this->getBucketName();

	Aws::S3::Model::ListObjectsV2Request request;
	request.WithBucket(bucket);
	request.WithDelimiter("/");  // NOTE percy since we control how to create files in S3 we should use this convention
	request.WithPrefix(objectKey);

	auto objectsOutcome = this->s3Client->ListObjectsV2(request);

	if(objectsOutcome.IsSuccess()) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

			for(auto const & s3Object : objects) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path("/" + s3Object.GetKey(), true);  // TODO percy avoid hardcoded string

				if(path != folderPath) {
					const bool pass = WildcardFilter::match(path.toString(true), finalWildcard);
					if(pass) {
						const Uri entry(uri.getScheme(), uri.getAuthority(), path);
						response.push_back(entry);
					}
				}
			}

			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders = objectsOutcome.GetResult().GetCommonPrefixes();

			for(auto const & s3Folder : folders) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path("/" + s3Folder.GetPrefix(), true);  // TODO percy avoid hardcoded string
				const bool pass = WildcardFilter::match(path.toString(true), finalWildcard);

				if(pass) {
					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
					response.push_back(entry);
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

			for(auto const & s3Object : objects) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path fullPath("/" + s3Object.GetKey(), true);  // TODO percy avoid hardcoded string

				if(fullPath != folderPath) {
					const bool pass =
						WildcardFilter::match(fullPath.toString(true), finalWildcard);  // filter must use the full path

					if(pass) {
						const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
						const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);

						response.push_back(relativeUri);
					}
				}
			}

			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders = objectsOutcome.GetResult().GetCommonPrefixes();

			for(auto const & s3Folder : folders) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path fullPath("/" + s3Folder.GetPrefix(), true);  // TODO percy avoid hardcoded string
				const bool pass =
					WildcardFilter::match(fullPath.toString(true), finalWildcard);  // filter must use the full path

				if(pass) {
					const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
					const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);

					response.push_back(relativeUri);
				}
			}
		}
	} else {
		Logging::Logger().logError("S3FileSystem::Private::list failed for URI: " + uri.toString());
		bool shouldRetry = objectsOutcome.GetError().ShouldRetry();
		if(shouldRetry) {
			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
									   objectsOutcome.GetError().GetMessage() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
									   objectsOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}
		throw BlazingFileSystemException("Could not list files found at " + uriWithRoot.toString() + ". Problem was " +
										 objectsOutcome.GetError().GetExceptionName() + " : " +
										 objectsOutcome.GetError().GetMessage());
	}

	return response;
}

std::vector<std::string> S3FileSystem::Private::listResourceNames(
	const Uri & uri, FileType fileType, const std::string & wildcard) const {
	std::vector<std::string> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path folderPath = uriWithRoot.getPath().getPathWithNormalizedFolderConvention();

	// NOTE only files is always true basically so we dont check it this is until we implement bucket listing
	// NOTE we want to get buckets bya filter, the sdk does not currently fully support that, the rest api does

	// TODO here we are removing the first "/" char so we create a S3 object key using the path ... improve this code
	const std::string objectKey = folderPath.toString(true).substr(1, folderPath.toString(true).size());
	const std::string bucket = this->getBucketName();

	Aws::S3::Model::ListObjectsV2Request request;
	request.WithBucket(bucket);
	request.WithDelimiter("/");  // NOTE percy since we control how to create files in S3 we should use this convention
	request.WithPrefix(objectKey);

	auto objectsOutcome = this->s3Client->ListObjectsV2(request);

	if(objectsOutcome.IsSuccess()) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);
		const FileTypeWildcardFilter filter(fileType, finalWildcard);

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

			for(auto const & s3Object : objects) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path("/" + s3Object.GetKey(), true);  // TODO percy avoid hardcoded string

				if(path != folderPath) {
					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
					const FileStatus fileStatus(entry, FileType::FILE, s3Object.GetSize());
					const bool pass = filter(fileStatus);

					if(pass) {
						response.push_back(fileStatus.getUri().getPath().getResourceName());
					}
				}
			}

			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders = objectsOutcome.GetResult().GetCommonPrefixes();

			for(auto const & s3Folder : folders) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path("/" + s3Folder.GetPrefix(), true);  // TODO percy avoid hardcoded string

				if(path != folderPath) {
					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
					const FileStatus fileStatus(
						entry, FileType::DIRECTORY, 0);  // TODO percy get info about the size of this kind of object
					const bool pass = filter(fileStatus);

					if(pass) {
						response.push_back(fileStatus.getUri().getPath().getResourceName());
					}
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			const Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

			for(auto const & s3Object : objects) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path fullPath("/" + s3Object.GetKey(), true);  // TODO percy avoid hardcoded string

				if(fullPath != folderPath) {
					const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
					const FileStatus fullFileStatus(fullUri, FileType::FILE, s3Object.GetSize());

					const bool pass = filter(fullFileStatus);  // filter must use the full path

					if(pass) {
						response.push_back(
							fullFileStatus.getUri()
								.getPath()
								.getResourceName());  // resource name is the same for full paths or relative paths
					}
				}
			}

			const Aws::Vector<Aws::S3::Model::CommonPrefix> folders = objectsOutcome.GetResult().GetCommonPrefixes();

			for(auto const & s3Folder : folders) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path fullPath("/" + s3Folder.GetPrefix(), true);  // TODO percy avoid hardcoded string

				if(fullPath != folderPath) {
					const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
					const FileStatus fullFileStatus(
						fullUri, FileType::DIRECTORY, 0);  // TODO percy get info about the size of this kind of object
					const bool pass = filter(fullFileStatus);

					if(pass) {
						response.push_back(
							fullFileStatus.getUri()
								.getPath()
								.getResourceName());  // resource name is the same for full paths or relative paths
					}
				}
			}
		}
	} else {
		Logging::Logger().logError("S3FileSystem::Private::listResourceNames failed for URI: " + uri.toString());
		bool shouldRetry = objectsOutcome.GetError().ShouldRetry();
		if(shouldRetry) {
			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
									   objectsOutcome.GetError().GetMessage() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
									   objectsOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}
		throw BlazingFileSystemException("Could not list resources found at " + uriWithRoot.toString() +
										 ". Problem was " + objectsOutcome.GetError().GetExceptionName() + " : " +
										 objectsOutcome.GetError().GetMessage());
	}

	return response;
}

std::vector<std::string> S3FileSystem::Private::listResourceNames(const Uri & uri, const std::string & wildcard) const {
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

	Aws::S3::Model::ListObjectsV2Request request;
	request.WithBucket(bucket);
	request.WithDelimiter("/");  // NOTE percy since we control how to create files in S3 we should use this convention
	request.WithPrefix(objectKey);

	auto objectsOutcome = this->s3Client->ListObjectsV2(request);

	if(objectsOutcome.IsSuccess()) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

			for(auto const & s3Object : objects) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path("/" + s3Object.GetKey(), true);  // TODO percy avoid hardcoded string

				if(path != folderPath) {
					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
					const bool pass = WildcardFilter::match(uri.toString(true), finalWildcard);

					if(pass) {
						response.push_back(entry.getPath().getResourceName());
					}
				}
			}

			Aws::Vector<Aws::S3::Model::CommonPrefix> folders = objectsOutcome.GetResult().GetCommonPrefixes();

			for(auto const & s3Folder : folders) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path path("/" + s3Folder.GetPrefix(), true);  // TODO percy avoid hardcoded string

				if(path != folderPath) {
					const Uri entry(uri.getScheme(), uri.getAuthority(), path);
					const bool pass = WildcardFilter::match(uri.toString(true), finalWildcard);

					if(pass) {
						response.push_back(entry.getPath().getResourceName());
					}
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			Aws::Vector<Aws::S3::Model::Object> objects = objectsOutcome.GetResult().GetContents();

			for(auto const & s3Object : objects) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path fullPath("/" + s3Object.GetKey(), true);  // TODO percy avoid hardcoded string

				if(fullPath != folderPath) {
					const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
					const bool pass =
						WildcardFilter::match(fullUri.toString(true), finalWildcard);  // filter must use the full path

					if(pass) {
						response.push_back(
							fullUri.getPath()
								.getResourceName());  // resource name is the same for full paths or relative paths
					}
				}
			}

			Aws::Vector<Aws::S3::Model::CommonPrefix> folders = objectsOutcome.GetResult().GetCommonPrefixes();

			for(auto const & s3Folder : folders) {
				// WARNING TODO percy there is no folders concept in S# ... we should change Path::isFile::bool to
				// Path::ObjectType::Unkwnow,DIR,FILE,SYMLIN,ETC
				const Path fullPath("/" + s3Folder.GetPrefix(), true);  // TODO percy avoid hardcoded string

				if(fullPath != folderPath) {
					const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);
					const bool pass =
						WildcardFilter::match(fullUri.toString(true), finalWildcard);  // filter must use the full path

					if(pass) {
						response.push_back(
							fullUri.getPath()
								.getResourceName());  // resource name is the same for full paths or relative paths
					}
				}
			}
		}
	} else {
		Logging::Logger().logError("S3FileSystem::Private::listResourceNames failed for URI: " + uri.toString());
		bool shouldRetry = objectsOutcome.GetError().ShouldRetry();
		if(shouldRetry) {
			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
									   objectsOutcome.GetError().GetMessage() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(objectsOutcome.GetError().GetExceptionName() + " : " +
									   objectsOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}
		throw BlazingFileSystemException("Could not list resources found at " + uriWithRoot.toString() +
										 ". Problem was " + objectsOutcome.GetError().GetExceptionName() + " : " +
										 objectsOutcome.GetError().GetMessage());
	}

	return response;
}

bool S3FileSystem::Private::makeDirectory(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath().getPathWithNormalizedFolderConvention();
	const std::string bucket = this->getBucketName();

	std::string mutablePath = path.toString(true);  // TODO ugly ... fix StringUtil api
	std::string rootChar = std::string("/");
	bool isFolder = StringUtil::endsWith(mutablePath, rootChar);

	if(isFolder == false) {
		throw BlazingS3Exception("Failed to make " + uri.toString() + " because path does not end in a slash");
	}

	mutablePath = mutablePath.substr(1, mutablePath.size());

	Aws::S3::Model::PutObjectRequest request;
	request.WithBucket(bucket);
	request.WithKey(mutablePath);

	if(this->isEncrypted()) {
		request.WithServerSideEncryption(this->serverSideEncryption());

		if(this->isAWSKMSEncrypted()) {
			request.WithSSEKMSKeyId(this->getSSEKMSKeyId());
		}
	}

	auto result = this->s3Client->PutObject(request);

	if(result.IsSuccess()) {
		return true;
	} else {
		Logging::Logger().logError("S3FileSystem::Private::makeDirectory failed for URI: " + uri.toString());
		bool shouldRetry = result.GetError().ShouldRetry();
		if(shouldRetry) {
			Logging::Logger().logError(
				result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(
				result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}
		throw BlazingS3Exception("Could not make directory " + uriWithRoot.toString() + ". Problem was " +
								 result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage());
	}
}

bool S3FileSystem::Private::remove(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();
	const std::string bucket = this->getBucketName();

	std::string objectKey = path.toString().substr(1, path.toString().size());

	if(objectKey[objectKey.size() - 1] != '/') {  // if we are not sure if its actually a directory, lets find out
		FileStatus status = this->getFileStatus(uri);

		if(status.isDirectory()) {
			objectKey += '/';
		}
	}

	Aws::S3::Model::DeleteObjectRequest request;
	request.WithBucket(bucket);
	request.WithKey(objectKey);

	auto result = this->s3Client->DeleteObject(request);

	if(result.IsSuccess()) {
		return true;
	} else {
		Logging::Logger().logError("S3FileSystem::Private::remove failed for URI: " + uri.toString());
		bool shouldRetry = result.GetError().ShouldRetry();
		if(shouldRetry) {
			Logging::Logger().logError(
				result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(
				result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}
		throw BlazingS3Exception("Could not remove " + uriWithRoot.toString() + ". Problem was " +
								 result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage());

		return false;
	}
}

bool S3FileSystem::Private::move(const Uri & src, const Uri & dst) const {
	if(src.isValid() == false) {
		throw BlazingInvalidPathException(src);
	}

	if(dst.isValid() == false) {
		throw BlazingInvalidPathException(dst);
	}
	const std::string bucket = this->getBucketName();

	const Uri srcWithRoot(src.getScheme(), src.getAuthority(), this->root + src.getPath().toString());
	const Uri dstWithRoot(dst.getScheme(), dst.getAuthority(), this->root + dst.getPath().toString());

	const std::string srcFullPath = srcWithRoot.getPath().toString();
	const std::string dstFullPath = dstWithRoot.getPath().toString();

	std::string fromKey = srcFullPath.substr(1, src.toString().size());
	std::string toKey = dstFullPath.substr(1, dst.toString().size());

	std::string source = bucket + "/" + fromKey;

	Aws::S3::Model::CopyObjectRequest request;

	request.WithBucket(bucket);
	request.WithKey(toKey);
	request.WithCopySource(source);

	if(this->isEncrypted()) {
		request.WithServerSideEncryption(this->serverSideEncryption());

		if(this->isAWSKMSEncrypted()) {
			request.WithSSEKMSKeyId(this->getSSEKMSKeyId());
		}
	}

	auto result = this->s3Client->CopyObject(request);

	if(result.IsSuccess()) {
		const bool deleted = this->remove(src);

		if(deleted) {
			return true;
		} else {
			throw BlazingS3Exception("Could not remove " + src.toString() + " after making copy to " + dst.toString());
		}
	} else {
		Logging::Logger().logError(
			"S3FileSystem::Private::move failed for src URI: " + src.toString() + "  and dst UTI: " + dst.toString());
		bool shouldRetry = result.GetError().ShouldRetry();
		if(shouldRetry) {
			Logging::Logger().logError(
				result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(
				result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}
		throw BlazingS3Exception("Could not move " + src.toString() + " to " + dst.toString() + ". Problem was " +
								 result.GetError().GetExceptionName() + " : " + result.GetError().GetMessage());

		return false;
	}
}

// TODO: truncate file can't be rolled back easily as it stands
// an easier way  might be to use move instead of remove
bool S3FileSystem::Private::truncateFile(const Uri & uri, long long length) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const FileStatus fileStatus = this->getFileStatus(uri);

	if(fileStatus.getFileSize() == length) {
		return true;
	}

	if(fileStatus.getFileSize() > length) {  // truncate
		std::shared_ptr<S3ReadableFile> file;
		const bool ok = this->openReadable(uri, &file);

		if(ok == false) {
			throw BlazingS3Exception("Could not open " + uri.toString() + " for truncating.");
		}

		// from object [         ] get [******|  ]
		const long long nbytes = length;

		std::shared_ptr<arrow::Buffer> buffer;
		file->Read(nbytes, &buffer);

		const auto closeFileOk = file->Close();

		if(closeFileOk.ok() == false) {
			throw BlazingS3Exception("Could not close " + uri.toString() + " for truncating.");
			return false;
		}

		// remove object [         ]
		const bool removeOk = this->remove(uri);

		if(removeOk == false) {
			throw BlazingS3Exception(
				"Could not remove previous " + uri.toString() +
				" for truncating (S3 has no truncate so we must copy delete old put in truncated version).");
		}

		// move [******|  ] into a new object [******] and put [******] in the same path of original object [         ]
		std::shared_ptr<S3OutputStream> outFile;

		const bool outOk = this->openWriteable(uri, &outFile);

		if(outOk == false) {
			throw BlazingS3Exception("Could not open " + uri.toString() + " for writing truncated data");
		}

		const auto writeOk = outFile->Write(buffer->data(), buffer->size());

		if(writeOk.ok() == false) {
			throw BlazingS3Exception("Could not write to " + uri.toString() + " for writing truncated data");
		}

		const auto flushOk = outFile->Flush();

		if(flushOk.ok() == false) {
			throw BlazingS3Exception("Could not flush " + uri.toString() + " for writing truncated data");
		}

		const auto closeOk = outFile->Close();

		if(closeOk.ok() == false) {
			throw BlazingS3Exception("Could not close " + uri.toString() + " after writing truncated data");
		}

		return true;
	} else if(fileStatus.getFileSize() < length) {  // expand
													// TODO percy this use case is not needed yet
	}

	return false;
}

bool S3FileSystem::Private::openReadable(const Uri & uri, std::shared_ptr<S3ReadableFile> * file) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();
	const std::string objectKey = path.toString(true).substr(1, path.toString(true).size());
	const std::string bucketName = this->getBucketName();
	// TODO: S3ReadableFile currentl has no validity check add it and throw errors here
	*file = std::make_shared<S3ReadableFile>(this->s3Client, bucketName, objectKey);
	return (*file)->isValid();
}

bool S3FileSystem::Private::openWriteable(const Uri & uri, std::shared_ptr<S3OutputStream> * file) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();
	const std::string objectKey = path.toString(true).substr(1, path.toString(true).size());
	const std::string bucketName = this->getBucketName();
	// TODO: S3Outputstream currentl has no validity check add it and throw errors here
	*file = std::make_shared<S3OutputStream>(bucketName, objectKey, this->s3Client);

	return true;
}

const std::string S3FileSystem::Private::getBucketName() const {
	using namespace S3FileSystemConnection;
	return this->fileSystemConnection.getConnectionProperty(ConnectionProperty::BUCKET_NAME);
}

const S3FileSystemConnection::EncryptionType S3FileSystem::Private::encryptionType() const {
	using namespace S3FileSystemConnection;
	const std::string encryptionTypeName =
		this->fileSystemConnection.getConnectionProperty(ConnectionProperty::ENCRYPTION_TYPE);
	const EncryptionType encryptionType = encryptionTypeFromName(encryptionTypeName);
	return encryptionType;
}

bool S3FileSystem::Private::isEncrypted() const {
	using namespace S3FileSystemConnection;
	return (this->encryptionType() != EncryptionType::NONE) && (this->encryptionType() != EncryptionType::UNDEFINED);
}

const Aws::S3::Model::ServerSideEncryption S3FileSystem::Private::serverSideEncryption() const {
	using namespace S3FileSystemConnection;

	switch(this->encryptionType()) {
	case EncryptionType::AES_256: return Aws::S3::Model::ServerSideEncryption::AES256; break;
	case EncryptionType::AWS_KMS: return Aws::S3::Model::ServerSideEncryption::aws_kms; break;
	}

	return Aws::S3::Model::ServerSideEncryption::NOT_SET;
}

bool S3FileSystem::Private::isAWSKMSEncrypted() const {
	using namespace S3FileSystemConnection;
	return (this->encryptionType() == EncryptionType::AWS_KMS);
}

const std::string S3FileSystem::Private::getSSEKMSKeyId() const {
	using namespace S3FileSystemConnection;
	const std::string kmsKey =
		this->fileSystemConnection.getConnectionProperty(ConnectionProperty::KMS_KEY_AMAZON_RESOURCE_NAME);
	return kmsKey;
}
