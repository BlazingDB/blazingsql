/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Felipe Aramburu <felipe@blazingdb.com>
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef SRC_UTIL_BLAZINGS3_S3OUTPUTSTREAM_H_
#define SRC_UTIL_BLAZINGS3_S3OUTPUTSTREAM_H_

#include "arrow/io/interfaces.h"
#include "arrow/status.h"

#include "aws/s3/S3Client.h"

class S3OutputStream : public arrow::io::OutputStream {
public:
	S3OutputStream(
		const std::string & bucketName, const std::string & objectKey, std::shared_ptr<Aws::S3::S3Client> s3Client);
	~S3OutputStream();

	arrow::Status Close() override;
	arrow::Status Write(const void * buffer, int64_t nbytes) override;
	arrow::Status Flush() override;
    arrow::Result<int64_t> Tell() const override;
    
	bool closed() const override;

private:
	// so we want to store the data that we are writing from there into some
	// buffer that then gets uploaded  we would like to avoid making allocations
	// so the buffer can be reusable
	class S3OutputStreamImpl;
	std::unique_ptr<S3OutputStreamImpl> impl_;

	ARROW_DISALLOW_COPY_AND_ASSIGN(S3OutputStream);
};

#endif /* SRC_UTIL_BLAZINGS3_S3OUTPUTSTREAM_H_ */
