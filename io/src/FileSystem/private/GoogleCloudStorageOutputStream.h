/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_OUTPUTSTREAM_H_
#define SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_OUTPUTSTREAM_H_

#include "arrow/io/interfaces.h"
#include "arrow/status.h"

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
	arrow::Status Tell(int64_t * position) const override;

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
