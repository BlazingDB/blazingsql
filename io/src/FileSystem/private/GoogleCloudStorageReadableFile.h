/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_READABLEFILE_H_
#define SRC_UTIL_BLAZING_GOOGLECLOUDSTORAGE_READABLEFILE_H_

#include "arrow/io/interfaces.h"
#include "arrow/status.h"

#include "google/cloud/storage/client.h"

namespace gcs = google::cloud::storage;

class GoogleCloudStorageReadableFile : public arrow::io::RandomAccessFile {
	public:
                GoogleCloudStorageReadableFile(std::shared_ptr<gcs::Client> gcsClient,
					   std::string bucket,
					   std::string key);
                ~GoogleCloudStorageReadableFile();

		arrow::Status Close() override;

		arrow::Status GetSize(int64_t *size) override;

		arrow::Status
		Read(int64_t nbytes, int64_t *bytesRead, void *buffer) override;

		arrow::Status Read(int64_t nbytes,
						   std::shared_ptr<arrow::Buffer> *out) override;

		arrow::Status ReadAt(int64_t position,
							 int64_t nbytes,
							 int64_t *bytes_read,
							 void *buffer) override;

		arrow::Status ReadAt(int64_t position,
							 int64_t nbytes,
							 std::shared_ptr<arrow::Buffer> *out) override;

		bool supports_zero_copy() const override;

		arrow::Status Seek(int64_t position) override;
		arrow::Status Tell(int64_t *position) const override;

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
