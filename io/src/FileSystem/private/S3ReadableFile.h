/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Felipe Aramburu <felipe@blazingdb.com>
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef SRC_UTIL_BLAZINGS3_S3READABLEFILE_H_
#define SRC_UTIL_BLAZINGS3_S3READABLEFILE_H_

#include "arrow/io/interfaces.h"
#include "arrow/status.h"
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/S3Client.h>

class S3ReadableFile : public arrow::io::RandomAccessFile {
public:
	S3ReadableFile(std::shared_ptr<Aws::S3::S3Client> s3Client, std::string bucket, std::string key);
	~S3ReadableFile();

	arrow::Status Close() override;

	arrow::Status GetSize(int64_t * size) override;

	arrow::Status Read(int64_t nbytes, int64_t * bytesRead, void * buffer) override;

	arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer> * out) override;

	arrow::Status ReadAt(int64_t position, int64_t nbytes, int64_t * bytes_read, void * buffer) override;

	arrow::Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<arrow::Buffer> * out) override;

	bool supports_zero_copy() const override;

	arrow::Status Seek(int64_t position) override;
	arrow::Status Tell(int64_t * position) const override;

	bool isValid() { return valid; }

	bool closed() const override;

private:
	std::shared_ptr<Aws::S3::S3Client> s3Client;
	std::string bucketName;
	std::string key;
	size_t position;
	bool valid;

	ARROW_DISALLOW_COPY_AND_ASSIGN(S3ReadableFile);
};

#endif /* SRC_UTIL_BLAZINGS3_S3READABLEFILE_H_ */
