/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemConnection.h"

#include "Util/StringUtil.h"

namespace HadoopFileSystemConnection {

const std::string driverTypeName(DriverType driverType) {
	switch(driverType) {
	case DriverType::LIBHDFS: return "LIBHDFS"; break;
	case DriverType::LIBHDFS3: return "LIBHDFS3"; break;
	}

	return "UNDEFINED";
}

DriverType driverTypeFromName(const std::string & driverTypeName) {
	if(driverTypeName == "LIBHDFS") {
		return DriverType::LIBHDFS;
	}

	if(driverTypeName == "LIBHDFS3") {
		return DriverType::LIBHDFS3;
	}

	return DriverType::UNDEFINED;
}

const std::string connectionPropertyName(ConnectionProperty connectionProperty) {
	switch(connectionProperty) {
	case ConnectionProperty::HOST: return "hdfs.host"; break;
	case ConnectionProperty::PORT: return "hdfs.port"; break;
	case ConnectionProperty::USER: return "hdfs.user"; break;
	case ConnectionProperty::DRIVER_TYPE: return "hdfs.driver.type"; break;
	case ConnectionProperty::KERBEROS_TICKET: return "hdfs.kerberos.ticket"; break;
	}

	return "UNDEFINED";
}

const std::string connectionPropertyEnvName(ConnectionProperty connectionProperty) {
	std::string property = "BLAZING_";
	property += connectionPropertyName(connectionProperty);
	property = StringUtil::replace(property, ".", "_");
	property = StringUtil::toUpper(property);

	return property;
}

// returns false if some argument is invalid, true otherwise
bool verifyConnectionProperties(const std::string & host,
	int port,
	const std::string & user,
	DriverType driverType,
	const std::string & kerberosTicket) {
	// TODO percy more checks, use regular expressions here

	if(host.empty()) {
		return false;
	}

	if(port < 0) {
		return false;
	}

	if(user.empty()) {
		return false;
	}

	if(driverType == DriverType::UNDEFINED) {
		return false;
	}

	// NOTE kerberosTicket can be empty

	return true;
}

}  // END namespace HadoopFileSystemConnection

namespace S3FileSystemConnection {

const std::string connectionPropertyName(ConnectionProperty connectionProperty) {
	switch(connectionProperty) {
	case ConnectionProperty::BUCKET_NAME: return "s3.bucket_name"; break;
	case ConnectionProperty::ENCRYPTION_TYPE: return "s3.encryption_type"; break;
	case ConnectionProperty::KMS_KEY_AMAZON_RESOURCE_NAME: return "s3.kms_key_amazon_resource_name"; break;
	case ConnectionProperty::ACCESS_KEY_ID: return "s3.access_key_id"; break;
	case ConnectionProperty::SECRET_KEY: return "s3.secret_key"; break;
	case ConnectionProperty::SESSION_TOKEN: return "s3.session_token"; break;
	}

	return "UNDEFINED";
}

const std::string connectionPropertyEnvName(ConnectionProperty connectionProperty) {
	std::string property = "BLAZING_";
	property += connectionPropertyName(connectionProperty);
	property = StringUtil::replace(property, ".", "_");
	property = StringUtil::toUpper(property);

	return property;
}

const std::string encryptionTypeName(EncryptionType encryptionType) {
	switch(encryptionType) {
	case EncryptionType::NONE: return "None"; break;
	case EncryptionType::AES_256: return "AES-256"; break;
	case EncryptionType::AWS_KMS: return "AWS-KMS"; break;
	}

	return "UNDEFINED";
}

EncryptionType encryptionTypeFromName(const std::string & encryptionTypeName) {
	if(encryptionTypeName == "None") {
		return EncryptionType::NONE;
	}

	if(encryptionTypeName == "AES-256") {
		return EncryptionType::AES_256;
	}

	if(encryptionTypeName == "AWS-KMS") {
		return EncryptionType::AWS_KMS;
	}

	return EncryptionType::UNDEFINED;  // return invalid
}

// returns false if some argument is invalid, true otherwise
bool verifyConnectionProperties(const std::string & bucketName,
	S3FileSystemConnection::EncryptionType encryptionType,
	const std::string & kmsKeyAmazonResourceName,
	const std::string & accessKeyId,
	const std::string & secretKey,
	const std::string & sessionToken) {
	// TODO percy more checks, use regular expressions here

	if(bucketName.empty()) {
		return false;
	}

	if(encryptionType == EncryptionType::UNDEFINED) {
		return false;
	}

	if((encryptionType == EncryptionType::AWS_KMS) && kmsKeyAmazonResourceName.empty()) {
		return false;
	}

	

	//	if (sessionToken.empty()) {
	//		return false;
	//	}

	return true;
}

}  // END namespace S3FileSystemConnection

namespace GoogleCloudStorageConnection {

const std::string connectionPropertyName(ConnectionProperty connectionProperty) {
	switch(connectionProperty) {
	case ConnectionProperty::PROJECT_ID: return "gcs.project_id"; break;
	case ConnectionProperty::BUCKET_NAME: return "gcs.bucket_name"; break;
	case ConnectionProperty::USE_DEFAULT_ADC_JSON_FILE: return "gcs.use_default_adc_json_file"; break;
	case ConnectionProperty::ADC_JSON_FILE: return "gcs.adc_json_file"; break;
	}

	return "UNDEFINED";
}

const std::string connectionPropertyEnvName(ConnectionProperty connectionProperty) {
	std::string property = "BLAZING_";
	property += connectionPropertyName(connectionProperty);
	property = StringUtil::replace(property, ".", "_");
	property = StringUtil::toUpper(property);

	return property;
}

// returns false if some argument is invalid, true otherwise
bool verifyConnectionProperties(const std::string & projectId,
	const std::string & bucketName,
	bool useDefaultAdcJsonFile,
	const std::string & adcJsonFile) {
	// TODO percy throw error details here?

	if(projectId.empty()) {
		return false;
	}

	if(bucketName.empty()) {
		return false;
	}

	if((useDefaultAdcJsonFile == false) && adcJsonFile.empty()) {
		return false;
	}

	return true;
}

}  // END namespace GoogleCloudStorageConnection

// BEGIN FileSystemConnection

FileSystemConnection::FileSystemConnection() : fileSystemType(FileSystemType::UNDEFINED) {}

FileSystemConnection::FileSystemConnection(FileSystemType fileSystemType) {
	const bool isUndefined = (fileSystemType == FileSystemType::UNDEFINED);
	const bool isLocal = (fileSystemType == FileSystemType::LOCAL);

	if(isUndefined || (isLocal == false)) {
		invalidate();
		return;
	}

	this->fileSystemType = fileSystemType;
}

FileSystemConnection::FileSystemConnection(const std::string & host,
	int port,
	const std::string & user,
	HadoopFileSystemConnection::DriverType driverType,
	const std::string & kerberosTicket) {
	using namespace HadoopFileSystemConnection;

	const bool valid = verifyConnectionProperties(host, port, user, driverType, kerberosTicket);

	if(valid == false) {
		this->invalidate();
		return;
	}

	const std::string portString = std::to_string(port);
	const std::string driverString = driverTypeName(driverType);

	// NOTE never change this insertion order
	const std::map<std::string, std::string> connectionProperties = {
		{connectionPropertyName(ConnectionProperty::HOST), host},
		{connectionPropertyName(ConnectionProperty::PORT), portString},
		{connectionPropertyName(ConnectionProperty::USER), user},
		{connectionPropertyName(ConnectionProperty::DRIVER_TYPE), driverString},
		{connectionPropertyName(ConnectionProperty::KERBEROS_TICKET), kerberosTicket}};

	this->fileSystemType = FileSystemType::HDFS;
	this->connectionProperties = std::move(connectionProperties);
}

FileSystemConnection::FileSystemConnection(const std::string & bucketName,
	S3FileSystemConnection::EncryptionType encryptionType,
	const std::string & kmsKeyAmazonResourceName,
	const std::string & accessKeyId,
	const std::string & secretKey,
	const std::string & sessionToken) {
	using namespace S3FileSystemConnection;

	const bool valid = verifyConnectionProperties(
		bucketName, encryptionType, kmsKeyAmazonResourceName, accessKeyId, secretKey, sessionToken);

	if(valid == false) {
		this->invalidate();
		return;
	}

	const std::string encryptionString = encryptionTypeName(encryptionType);

	// NOTE never change this insertion order
	const std::map<std::string, std::string> connectionProperties = {
		{connectionPropertyName(ConnectionProperty::BUCKET_NAME), bucketName},
		{connectionPropertyName(ConnectionProperty::ENCRYPTION_TYPE), encryptionString},
		{connectionPropertyName(ConnectionProperty::KMS_KEY_AMAZON_RESOURCE_NAME), kmsKeyAmazonResourceName},
		{connectionPropertyName(ConnectionProperty::ACCESS_KEY_ID), accessKeyId},
		{connectionPropertyName(ConnectionProperty::SECRET_KEY), secretKey},
		{connectionPropertyName(ConnectionProperty::SESSION_TOKEN), sessionToken}};

	this->fileSystemType = FileSystemType::S3;
	this->connectionProperties = std::move(connectionProperties);
}

FileSystemConnection::FileSystemConnection(const std::string & projectId,
	const std::string & bucketName,
	bool useDefaultAdcJsonFile,
	const std::string & adcJsonFile) {
	using namespace GoogleCloudStorageConnection;

	const bool valid = verifyConnectionProperties(projectId, bucketName, useDefaultAdcJsonFile, adcJsonFile);

	if(valid == false) {
		this->invalidate();
		return;
	}

	std::string useDefaultAdcJsonFileStr = "true";

	if(useDefaultAdcJsonFile == false) {
		useDefaultAdcJsonFileStr = "false";
	}

	// NOTE never change this insertion order
	const std::map<std::string, std::string> connectionProperties = {
		{connectionPropertyName(ConnectionProperty::PROJECT_ID), projectId},
		{connectionPropertyName(ConnectionProperty::BUCKET_NAME), bucketName},
		{connectionPropertyName(ConnectionProperty::USE_DEFAULT_ADC_JSON_FILE), useDefaultAdcJsonFileStr},
		{connectionPropertyName(ConnectionProperty::ADC_JSON_FILE), adcJsonFile}};

	this->fileSystemType = FileSystemType::GOOGLE_CLOUD_STORAGE;
	this->connectionProperties = std::move(connectionProperties);
}

FileSystemConnection::FileSystemConnection(const std::string & fileSystemConnectionString) {
	const std::vector<std::string> fileSystemTypeSplit = StringUtil::split(fileSystemConnectionString, " : ");

	if(fileSystemTypeSplit.size() == 2) {
		const std::string fileSystemType = fileSystemTypeSplit[0];

		if(fileSystemType == fileSystemTypeName(FileSystemType::LOCAL)) {
			this->fileSystemType = FileSystemType::LOCAL;
		} else if(fileSystemType == fileSystemTypeName(FileSystemType::HDFS)) {
			this->fileSystemType = FileSystemType::HDFS;
		} else if(fileSystemType == fileSystemTypeName(FileSystemType::S3)) {
			this->fileSystemType = FileSystemType::S3;
		} else if(fileSystemType == fileSystemTypeName(FileSystemType::GOOGLE_CLOUD_STORAGE)) {
			this->fileSystemType = FileSystemType::GOOGLE_CLOUD_STORAGE;
		} else {
			this->invalidate();
			return;
		}

		if(requireConnectionProperties()) {
			const std::vector<std::string> connectionPropertiesSplit = StringUtil::split(fileSystemTypeSplit[1], ",");

			for(const std::string & property : connectionPropertiesSplit) {
				const std::vector<std::string> propSplit = StringUtil::split(property, "|");

				if(propSplit.size() == 2) {
					this->connectionProperties[propSplit[0]] = propSplit[1];
				} else {
					// malformed text
					this->invalidate();
					return;
				}
			}
		}
	} else {
		this->invalidate();
		return;
	}
}

FileSystemConnection::FileSystemConnection(const FileSystemConnection & other)
	: fileSystemType(other.fileSystemType), connectionProperties(other.connectionProperties) {}

FileSystemConnection::FileSystemConnection(FileSystemConnection && other)
	: fileSystemType(std::move(other.fileSystemType)), connectionProperties(std::move(other.connectionProperties)) {}

FileSystemConnection::~FileSystemConnection() {}

bool FileSystemConnection::isValid() const noexcept {
	if(this->fileSystemType == FileSystemType::UNDEFINED) {
		return false;
	}

	if(this->requireConnectionProperties() && this->connectionProperties.empty()) {
		return false;
	}

	return true;
}

FileSystemType FileSystemConnection::getFileSystemType() const noexcept { return this->fileSystemType; }

const std::map<std::string, std::string> FileSystemConnection::getConnectionProperties() const noexcept {
	return this->connectionProperties;
}

const std::string FileSystemConnection::getConnectionProperty(
	HadoopFileSystemConnection::ConnectionProperty connectionProperty) const noexcept {
	using namespace HadoopFileSystemConnection;

	if(this->isValid() == false) {
		return std::string();
	}

	if(this->fileSystemType != FileSystemType::HDFS) {
		return std::string();
	}

	const std::string propertyName = connectionPropertyName(connectionProperty);

	return this->connectionProperties.at(propertyName);
}

const std::string FileSystemConnection::getConnectionProperty(
	S3FileSystemConnection::ConnectionProperty connectionProperty) const noexcept {
	using namespace S3FileSystemConnection;

	if(this->isValid() == false) {
		return std::string();
	}

	if(this->fileSystemType != FileSystemType::S3) {
		return std::string();
	}

	const std::string propertyName = connectionPropertyName(connectionProperty);

	return this->connectionProperties.at(propertyName);
}

const std::string FileSystemConnection::getConnectionProperty(
	GoogleCloudStorageConnection::ConnectionProperty connectionProperty) const noexcept {
	using namespace GoogleCloudStorageConnection;

	if(this->isValid() == false) {
		return std::string();
	}

	if(this->fileSystemType != FileSystemType::GOOGLE_CLOUD_STORAGE) {
		return std::string();
	}

	const std::string propertyName = connectionPropertyName(connectionProperty);

	return this->connectionProperties.at(propertyName);
}

std::string FileSystemConnection::toString() const {
	if(this->fileSystemType == FileSystemType::UNDEFINED) {
		return std::string();
	}

	std::string connectionProperties = "";
	for(const auto & connectionProperty : this->connectionProperties) {
		if(connectionProperties == "") {
			connectionProperties += connectionProperty.first + ":" + connectionProperty.second;
		} else {
			connectionProperties += "|" + connectionProperty.first + ":" + connectionProperty.second;
		}
	}

	const std::string result = fileSystemTypeName(this->fileSystemType) + "=>" + connectionProperties;

	return result;
}

FileSystemConnection & FileSystemConnection::operator=(const FileSystemConnection & other) {
	this->fileSystemType = other.fileSystemType;
	this->connectionProperties = other.connectionProperties;

	return *this;
}

FileSystemConnection & FileSystemConnection::operator=(FileSystemConnection && other) {
	this->fileSystemType = std::move(other.fileSystemType);
	this->connectionProperties = std::move(other.connectionProperties);

	return *this;
}

bool FileSystemConnection::operator==(const FileSystemConnection & other) const {
	const bool fileSystemTypeEquals = (this->fileSystemType == other.fileSystemType);
	const bool connectionPropertiesEquals = (this->connectionProperties == other.connectionProperties);
	const bool connectionPropertiesSizeEquals =
		(this->connectionProperties.size() == other.connectionProperties.size());

	const bool equals = (fileSystemTypeEquals && connectionPropertiesEquals && connectionPropertiesSizeEquals);

	return equals;
}

bool FileSystemConnection::operator!=(const FileSystemConnection & other) const { return !(*this == other); }

void FileSystemConnection::invalidate() {
	this->fileSystemType = FileSystemType::UNDEFINED;
	this->connectionProperties = std::map<std::string, std::string>();
}

bool FileSystemConnection::requireConnectionProperties() const noexcept {
	bool require = false;

	switch(this->fileSystemType) {
	case FileSystemType::LOCAL: require = false; break;
	case FileSystemType::HDFS: require = true; break;
	case FileSystemType::S3: require = true; break;
	case FileSystemType::GOOGLE_CLOUD_STORAGE: require = true; break;
	}

	return require;
}

// END FileSystemConnection
