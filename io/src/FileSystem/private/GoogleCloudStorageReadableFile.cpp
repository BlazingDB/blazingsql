/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "GoogleCloudStorageReadableFile.h"

#include <istream>
#include <streambuf>

#include <arrow/io/api.h>
#include <arrow/result.h>
#include "arrow/buffer.h"
#include <arrow/memory_pool.h>

#include "Util/StringUtil.h"

#include "Library/Logging/Logger.h"

namespace Logging = Library::Logging;

// TODO: handle the situation when not all data is read

GoogleCloudStorageReadableFile::~GoogleCloudStorageReadableFile() {}

GoogleCloudStorageReadableFile::GoogleCloudStorageReadableFile(
	std::shared_ptr<gcs::Client> gcsClient, std::string bucketName, std::string key) {
	this->key = key;
	this->bucketName = bucketName;
	this->gcsClient = gcsClient;
	position = 0;
	valid = true;
}

arrow::Status GoogleCloudStorageReadableFile::Seek(int64_t position) {
	this->position = position;
	return arrow::Status::OK();
}
arrow::Result<int64_t> GoogleCloudStorageReadableFile::Tell() const {
	return this->position;
}

arrow::Status GoogleCloudStorageReadableFile::Close() {
	// because each read is its own request we really dont have to do this
	return arrow::Status::OK();
}

arrow::Result<int64_t> GoogleCloudStorageReadableFile::GetSize() {
    int64_t size = -1;
	using ::google::cloud::StatusOr;

	StatusOr<gcs::ObjectMetadata> objectMetadata = this->gcsClient->GetObjectMetadata(this->bucketName, this->key);

	if(objectMetadata) {  // if success
		const long long contentLength = objectMetadata->size();
		size = contentLength;
	} else {
		size = -1;
		Logging::Logger().logWarn("GoogleCloudStorageReadableFile::GetSize, HeadObject failed");

		// TODO percy
		//        bool shouldRetry = results.GetError().ShouldRetry();
		//        if (shouldRetry){
		//            Logging::Logger().logError(results.GetError().GetExceptionName() + " : " +
		//            results.GetError().GetMessage() + "  SHOULD RETRY");
		//        } else {
		//            Logging::Logger().logError(results.GetError().GetExceptionName() + " : " +
		//            results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		//        }

        // TODO william jp errors for new arrow-0.17 API
		//return arrow::Status::IOError("Error: " + objectMetadata.status().message() + " with " + this->key);
        
        return size;
	}

	return size;
}

arrow::Result<int64_t> GoogleCloudStorageReadableFile::Read(int64_t nbytes, void* buffer) {
    int64_t bytesRead = -1;
	auto results = this->gcsClient->ReadObject(this->bucketName, key, gcs::ReadRange(position, position + nbytes));

	if(!results.status().ok()) {
		// TODO: Make bad arrow status here
		Logging::Logger().logWarn(
			"GoogleCloudStorageReadableFile::Read, GetObject failed for bucketName: " + bucketName + " key " + key);

		// TODO percy retry logic
		// bool shouldRetry = results.GetError().ShouldRetry();

		//        if (shouldRetry){
		//            Logging::Logger().logTrace("retrying");
		//            return this->Read(nbytes, bytesRead, buffer);
		//        } else {
		//            *bytesRead = 0;
		//            Logging::Logger().logError(results.GetError().GetExceptionName() + " : " +
		//            results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		//        }

        // TODO william jp errors for new arrow-0.17 API
		//return arrow::Status::IOError("Error: " + results.status().message() + " with " + this->key);
        
        return bytesRead;
	} else {
		// TODO percy this doesnt work so we would need to read all the content in order to get the size ... costly!
		// results.seekg (0, results.end);
		// int length = results.tellg();
		// results.seekg (0, results.beg);
		// std::cout << "OKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKY : " << length  << std::endl;

		// TODO percy will asume gsc can read all the content
		// so we avoid read first all the stuff only to get its size (results.gcount() doesnt work)
		bytesRead = nbytes;
		//*bytesRead = nbytes < *bytesRead ? nbytes : *bytesRead;
		results.read((char *) buffer, bytesRead);

		// NOTE percy check for badbit also the user should never read more bytes than the result content size
		if(results.bad()) {
			bytesRead = 0;
		}

		position += bytesRead;
		return bytesRead;
	}
    
    return bytesRead;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> GoogleCloudStorageReadableFile::Read(int64_t nbytes) {
    std::shared_ptr<arrow::Buffer> out = nullptr;
    
	//	std::cout<<"GoogleCloudStorageReadableFile::Read " + std::to_string(nbytes)<<std::endl;
	auto results = this->gcsClient->ReadObject(this->bucketName, key, gcs::ReadRange(position, position + nbytes));

	if(!results.status().ok()) {
		// TODO: Make bad arrow status here

		Logging::Logger().logWarn(
			"GoogleCloudStorageReadableFile::Read, GetObject failed for bucketName: " + bucketName + " key " + key);
		// TODO percy
		//        bool shouldRetry = results.GetError().ShouldRetry();
		//        if (shouldRetry){
		//            Logging::Logger().logTrace("retrying");
		//            return this->Read(nbytes, out);
		//        } else {
		//            Logging::Logger().logError(results.GetError().GetExceptionName() + " : " +
		//            results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		//        }

        // TODO william jp errors for new arrow-0.17 API
		//return arrow::Status::IOError("Error: " + results.status().message() + " with " + this->key);
        
        return out;
	} else {
		// old implementation
		/*
std::shared_ptr<arrow::ResizableBuffer> buffer;
AllocateResizableBuffer(arrow::default_memory_pool(), nbytes, &buffer);

int64_t bytesRead = results.GetResult().GetContentLength();

memcpy(buffer->mutable_data(), results.GetResult().GetBody().rdbuf(), bytesRead);

position += bytesRead;
*out = buffer;
return arrow::Status::OK();
*/

		// TODO percy will asume gsc can read all the content
		// so we avoid read first all the stuff only to get its size (results.gcount() doesnt work)
		int64_t bytesRead = nbytes;  // results.gcount();
		// bytesRead = nbytes < bytesRead ? nbytes : bytesRead;
		position += bytesRead;
		// if (bytesRead < nbytes){
		//    Logging::Logger().logError("Did not read all the bytes at GoogleCloudStorageReadableFile::ReadAt(int64_t
		//    position, int64_t nbytes, std::shared_ptr<arrow::Buffer>* out)");
		//}
		//		results.GetResult().GetBody().read((char*)(out->get()->mutable_data()), bytesRead);

		arrow::Result<std::unique_ptr<arrow::ResizableBuffer>> buffer;
		buffer = AllocateResizableBuffer(nbytes, arrow::default_memory_pool());

		//results.read((char *) (buffer->mutable_data()), bytesRead);  // TODO: HOW HANDLE THIS?

		// NOTE percy check for badbit also the user should never read more bytes than the result content size
		//if(results.bad()) {
		//	bytesRead = 0;
		//} else {
		//	out = buffer;
		//}

		out = *std::move(buffer);

		return out;
	}
}

arrow::Result<int64_t> GoogleCloudStorageReadableFile::ReadAt(int64_t position, int64_t nbytes, void* buffer) {
    int64_t bytesRead = -1;
    
	//	std::cout<<"GoogleCloudStorageReadableFile::ReadAt " + std::to_string(nbytes)<<std::endl;

	auto results = this->gcsClient->ReadObject(this->bucketName, key, gcs::ReadRange(position, position + nbytes));

	if(!results.status().ok()) {
		// TODO: Make bad arrow status here

		Logging::Logger().logWarn(
			"GoogleCloudStorageReadableFile::ReadAt, GetObject failed for bucketName: " + bucketName + " key " + key);

		// TODO percy
		//        bool shouldRetry = results.GetError().ShouldRetry();
		//        if (shouldRetry){
		//            Logging::Logger().logTrace("retrying");
		//            return this->ReadAt(position, nbytes, bytesRead, buffer);
		//        } else {
		//            *bytesRead = 0;
		//            Logging::Logger().logError(results.GetError().GetExceptionName() + " : " +
		//            results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		//        }

        // TODO william jp errors for new arrow-0.17 API
		//return arrow::Status::IOError("Error: " + results.status().message() + " with " + this->key);
        
        return bytesRead;
	} else {
		// TODO percy will asume gsc can read all the content
		// so we avoid read first all the stuff only to get its size (results.gcount() doesnt work)
		bytesRead = nbytes;
		//*bytesRead = nbytes < *bytesRead ? nbytes : *bytesRead;
		this->position = position + bytesRead;
		results.read((char *) buffer, bytesRead);

		// NOTE percy check for badbit also the user should never read more bytes than the result content size
		if(results.bad()) {
			bytesRead = 0;
		}

		return bytesRead;
	}
    
    return bytesRead;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> GoogleCloudStorageReadableFile::ReadAt(int64_t position, int64_t nbytes) {
    std::shared_ptr<arrow::Buffer> out = nullptr;

	//	std::cout<<"GoogleCloudStorageReadableFile::ReadAt " + std::to_string(nbytes)<<std::endl;

	auto results = this->gcsClient->ReadObject(this->bucketName, key, gcs::ReadRange(position, position + nbytes));

	if(!results.status().ok()) {
		// TODO: Make bad arrow status here

		Logging::Logger().logWarn(
			"GoogleCloudStorageReadableFile::ReadAt, GetObject failed for bucketName: " + bucketName + " key " + key);
		// TODO percy
		//        bool shouldRetry = results.GetError().ShouldRetry();
		//        if (shouldRetry){
		//            Logging::Logger().logTrace("retrying");
		//            return this->ReadAt(position, nbytes, out);
		//        } else {
		//            Logging::Logger().logError(results.GetError().GetExceptionName() + " : " +
		//            results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		//        }

        // TODO william jp errors for new arrow-0.17 API
        //return arrow::Status::IOError("Error: " + results.status().message() + " with " + this->key);		
        
        return out;
	} else {
		// old implementation
		/*
		//byutes read should always be full amount
		//should almost always be nBytes
		std::shared_ptr<arrow::ResizableBuffer> buffer;
		AllocateResizableBuffer(arrow::default_memory_pool(), nbytes, &buffer);

		int64_t bytesRead = nbytes;

		memcpy(buffer->mutable_data(), results.GetResult().GetBody().rdbuf(), bytesRead);

		*out = buffer;
		return arrow::Status::OK();
		*/

		// TODO percy will asume gsc can read all the content
		// so we avoid read first all the stuff only to get its size (results.gcount() doesnt work)
		int64_t bytesRead = nbytes;  // results.gcount();
		// bytesRead = nbytes < bytesRead ? nbytes : bytesRead;
		this->position = position + nbytes;
		// if (bytesRead < nbytes){
		//    Logging::Logger().logError("Did not read all the bytes at GoogleCloudStorageReadableFile::ReadAt(int64_t
		//    position, int64_t nbytes, std::shared_ptr<arrow::Buffer>* out)");
		//}
		//		results.GetResult().GetBody().read((char*)(out->get()->mutable_data()), bytesRead);

		arrow::Result<std::unique_ptr<arrow::ResizableBuffer>> buffer;
		buffer = AllocateResizableBuffer(nbytes, arrow::default_memory_pool());

		//results.read((char *) (buffer->mutable_data()), bytesRead);  // TODO: HOW HANDLE THIS?
		out = *std::move(buffer);

		// NOTE percy check for badbit also the user should never read more bytes than the result content size
		//if(results.bad()) {
		//	bytesRead = 0;
		//} else {
		//	out = buffer;
		//}

		return out;
	}
}

bool GoogleCloudStorageReadableFile::supports_zero_copy() const { return false; }

bool GoogleCloudStorageReadableFile::closed() const {
	// Since every file interaction is a request, then the file is never really open. This function is necesary due to
	// the Apache Arrow interface starting with v12. Depending on who or what uses this function, we may want this to
	// always return false??
	return true;
}
