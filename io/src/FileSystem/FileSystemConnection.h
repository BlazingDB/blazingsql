/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2017-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _BLAZING_FILE_SYSTEM_CONNECTION_PROPERTIES_H_
#define _BLAZING_FILE_SYSTEM_CONNECTION_PROPERTIES_H_

#include <map>

#include "FileSystem/FileSystemType.h"

namespace HadoopFileSystemConnection {
enum class DriverType : char {
	UNDEFINED = 0,
	LIBHDFS = 1,  // LIBHDFS requires Java CLASSPATH and native HDFS in LD_LIBRARY_PATH
	LIBHDFS3 = 2  // LIBHDFS3 requires C++ pivotalrd-libhdfs3 library in LD_LIBRARY_PATH
};

enum class ConnectionProperty : char {
	UNDEFINED,
	HOST,		  // Hadoop NameNode server address
	PORT,		  // numeric value
	USER,		  // Hadoop user with privileges over HDFS
	DRIVER_TYPE,  // values can be "LIBHDFS" or "LIBHDFS3"
	// TODO DRIVER_VERSION
	KERBEROS_TICKET  // Kerberos auth token
};

const std::string driverTypeName(DriverType driverType);
DriverType driverTypeFromName(const std::string & driverTypeName);
const std::string connectionPropertyName(ConnectionProperty connectionProperty);	 // format: hdfs.property
const std::string connectionPropertyEnvName(ConnectionProperty connectionProperty);  // format: BLAZING_HDFS_PROPERTY
}  // namespace HadoopFileSystemConnection

namespace S3FileSystemConnection {
enum class EncryptionType : char {
	UNDEFINED,
	NONE,
	AES_256,
	AWS_KMS  // AWS Key Management Service
};

enum class ConnectionProperty : char {
	UNDEFINED,
	BUCKET_NAME,
	ENCRYPTION_TYPE,			   // encryption of the current root folder pointed by the namespace
	KMS_KEY_AMAZON_RESOURCE_NAME,  // if encryption type is AWS_KMS, then the user must provide the Amazon Resource Name
								   // (ARN) of the AWS KMS key to use
	ACCESS_KEY_ID,
	SECRET_KEY,
	SESSION_TOKEN,
	ENDPOINT_OVERRIDE,  // if url:port is given then we can connect to different S3 provider (e.g. Apache MinIO)
	REGION // in case the user wants to point to a specific region (this may solve some issues)
};

const std::string encryptionTypeName(EncryptionType encryptionType);
EncryptionType encryptionTypeFromName(const std::string & encryptionTypeName);
const std::string connectionPropertyName(ConnectionProperty connectionProperty);	 // format: s3.property
const std::string connectionPropertyEnvName(ConnectionProperty connectionProperty);  // format: BLAZING_S3_PROPERTY
}  // namespace S3FileSystemConnection

namespace GoogleCloudStorageConnection {
enum class ConnectionProperty : char { UNDEFINED, PROJECT_ID, BUCKET_NAME, USE_DEFAULT_ADC_JSON_FILE, ADC_JSON_FILE };

const std::string connectionPropertyName(ConnectionProperty connectionProperty);	 // format: gcs.property
const std::string connectionPropertyEnvName(ConnectionProperty connectionProperty);  // format: BLAZING_GCS_PROPERTY
}  // namespace GoogleCloudStorageConnection

// NOTE Immutable class
class FileSystemConnection {
public:
	/**
	 * @brief Constructs an invalid FileSystemConnection
	 */
	FileSystemConnection();

	/**
	 * @brief Constructs a File System connection that doesn't need connection arguments (e.g. local file system)
	 *
	 * @note
	 * Current implementation of this constructor will only accept FileSystemType::LOCAL, any other value will construct
	 * an invalid FileSystemConnection
	 */
	FileSystemConnection(FileSystemType fileSystemType);

	/**
	 * @brief Constructs a Hadoop File System connection
	 *
	 * @note
	 * If some argument is invalid then will construct an invalid FileSystemConnection
	 */
	FileSystemConnection(const std::string & host,
		int port,
		const std::string & user,
		HadoopFileSystemConnection::DriverType driverType,
		const std::string & kerberosTicket);

	/**
	 * @brief Constructs a S3 File System connection
	 *
	 * @note
	 * If some argument is invalid then will construct an invalid FileSystemConnection
	 */
	FileSystemConnection(const std::string & bucketName,
		S3FileSystemConnection::EncryptionType encryptionType,
		const std::string & kmsKeyAmazonResourceName,
		const std::string & accessKeyId,
		const std::string & secretKey,
		const std::string & sessionToken,
		const std::string & endpointOverride = "",
		const std::string & region = "");

	/**
	 * @brief Constructs a GoogleCloudStorage connection
	 *
	 * Leave useDefaultAdcFile as true when you want to use the Application Default Credentials (ADC).
	 * If you want to point to specific ADC the set the adcJsonFile path (will set the
	 * env var GOOGLE_APPLICATION_CREDENTIALS)
	 *
	 * @note
	 * If some argument is invalid then will construct an invalid FileSystemConnection
	 */
	FileSystemConnection(const std::string & projectId,
		const std::string & bucketName,
		bool useDefaultAdcJsonFile = true,
		const std::string & adcJsonFile = "");

	/**
	 * @brief Constructs a FileSystemConnection using a string from FileSystemConnection::toString
	 *
	 * @note
	 * If the string is invalid then will construct an invalid FileSystemConnection
	 */
	FileSystemConnection(const std::string & fileSystemConnectionString);

	/**
	 * @brief Copy constructor
	 */
	FileSystemConnection(const FileSystemConnection & other);

	/**
	 * @brief Move constructor
	 */
	FileSystemConnection(FileSystemConnection && other);

	/**
	 * @brief Destructor (will not release heap memory)
	 */
	~FileSystemConnection();

	// return false if FileSystemType is UNDEFINED and the connection properties are empty, otherwise will return true
	bool isValid() const noexcept;

	FileSystemType getFileSystemType() const noexcept;
	const std::map<std::string, std::string> getConnectionProperties() const noexcept;

	// is property is not present or the instance is invalid and not HDFS then return empty string
	const std::string getConnectionProperty(HadoopFileSystemConnection::ConnectionProperty connectionProperty) const
		noexcept;

	// is property is not present or the instance is invalid and not S3 then return empty string
	const std::string getConnectionProperty(S3FileSystemConnection::ConnectionProperty connectionProperty) const
		noexcept;

	// is property is not present or the instance is invalid and not GoogleCloudStorage then return empty string
	const std::string getConnectionProperty(GoogleCloudStorageConnection::ConnectionProperty connectionProperty) const
		noexcept;

	std::string toString() const;  // json format

	FileSystemConnection & operator=(const FileSystemConnection & other);
	FileSystemConnection & operator=(FileSystemConnection && other);

	bool operator==(const FileSystemConnection & other) const;
	bool operator!=(const FileSystemConnection & other) const;

private:
	// Mark this object as invalid: sets fileSystemType as UNDEFINED and connectionProperties as empty map
	void invalidate();

	// Checks if fileSystemType require connectionProperties
	bool requireConnectionProperties() const noexcept;

private:
	FileSystemType fileSystemType;
	std::map<std::string, std::string> connectionProperties;
};

#endif /* _BLAZING_FILE_SYSTEM_CONNECTION_PROPERTIES_H_ */
