/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_OUTPUTSTREAM_H_
#define SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_OUTPUTSTREAM_H_

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

class GoogleCloudStorageOutputStream : public arrow::io::OutputStream {
public:
	GoogleCloudStorageOutputStream(
		const std::string & bucketName, const std::string & objectKey, std::shared_ptr<gcs::Client> gcsClient);
	~GoogleCloudStorageOutputStream();

    arrow::Status Close() override;
	arrow::Status Write(const void * buffer, int64_t nbytes) override;
	arrow::Status Flush() override;
    arrow::Result<int64_t> Tell() const override;

	bool closed() const override;

private:
	// so we want to store the data that we are writing from there into some
	// buffer that then gets uploaded  we would like to avoid making allocations
	// so the buffer can be reusable
	class GoogleCloudStorageOutputStreamImpl;
	std::unique_ptr<GoogleCloudStorageOutputStreamImpl> impl_;

	ARROW_DISALLOW_COPY_AND_ASSIGN(GoogleCloudStorageOutputStream);
};

#endif /* SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_OUTPUTSTREAM_H_ */
