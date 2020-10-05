/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_READABLEFILE_H_
#define SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_READABLEFILE_H_

#include "arrow/io/interfaces.h"
#include "arrow/status.h"

// BEGIN UGLY PATCH william jp c.gonzales if we don't do this we get a compile error: Mismatched major version (always before of the 1sr google header)
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_VERSION_INFO_H 1

#define STORAGE_CLIENT_VERSION_MAJOR 1
#define STORAGE_CLIENT_VERSION_MINOR 14
#define STORAGE_CLIENT_VERSION_PATCH 0

#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_INTERNAL_VERSION_INFO_H 1

#define GOOGLE_CLOUD_CPP_VERSION_MAJOR 1
#define GOOGLE_CLOUD_CPP_VERSION_MINOR 14
#define GOOGLE_CLOUD_CPP_VERSION_PATCH 0

// END UGLY PATCH

#include "google/cloud/storage/client.h"

namespace gcs = google::cloud::storage;

class GoogleCloudStorageReadableFile : public arrow::io::RandomAccessFile {
public:
	GoogleCloudStorageReadableFile(std::shared_ptr<gcs::Client> gcsClient, std::string bucket, std::string key);
	~GoogleCloudStorageReadableFile();

    arrow::Status Close() override;

    arrow::Result<int64_t> GetSize() override;

	arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;

	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

	arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;

	arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;

	bool supports_zero_copy() const override;

	arrow::Status Seek(int64_t position) override;
	arrow::Result<int64_t> Tell() const override;

	bool isValid() { return valid; }

	bool closed() const override;

private:
	std::shared_ptr<gcs::Client> gcsClient;
	std::string bucketName;
	std::string key;
	size_t position;
	bool valid;

	ARROW_DISALLOW_COPY_AND_ASSIGN(GoogleCloudStorageReadableFile);
};

#endif /* SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_READABLEFILE_H_ */
