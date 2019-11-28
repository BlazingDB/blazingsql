#include <iostream>
#include <limits.h>
#include <time.h>

#include "gtest/gtest.h"

#include "arrow/status.h"
#include "parquet/api/reader.h"
#include "parquet/metadata.h"

#include "FileSystem/S3FileSystem.h"

//TODO remove this (create a static method in S3Fiklesystem or ... somewhere)
#include <aws/core/Aws.h>

const std::string AUTHORITY = "jack_s3_1"; // the file system namespace used for this test

using namespace S3FileSystemConnection;

class ParquetOnS3Test : public testing::Test {
	protected:
		ParquetOnS3Test() {
			Aws::SDKOptions sdkOptions;
			Aws::InitAPI(sdkOptions);

			const std::string bucketName = getBucketNameEnvValue();
			const EncryptionType encryptionType = getEncryptionTypeEnvValue();
			const std::string kmsKeyAmazonResourceName = getKmsKeyAmazonResourceNameEnvValue();
			const std::string accessKeyId = getAccessKeyIdEnvValue();
			const std::string secretKey = getSecretKeyEnvValue();
			const std::string sessionToken = getSessionTokenEnvValue();
			const FileSystemConnection fileSystemConnection(bucketName, encryptionType, kmsKeyAmazonResourceName, accessKeyId, secretKey, sessionToken);

			Path root;

			s3FileSystem.reset(new S3FileSystem(fileSystemConnection, root));
		}

		virtual ~ParquetOnS3Test() {
			Aws::SDKOptions sdkOptions;
			Aws::ShutdownAPI(sdkOptions);
		}

		virtual void SetUp() {

		}

		virtual void TearDown() {
			//S3FileSystem->disconnect();
		}

	private:
		//TODO percy duplicated code with HadoopFileSystemTest, create common test abstraccions for this kind of stuff
		std::string getConnectionPropertyEnvValue(ConnectionProperty connectionProperty) const {
			const std::string propertyEnvName = connectionPropertyEnvName(connectionProperty);
			const char *envValue = std::getenv(propertyEnvName.c_str());
			const bool isDefined = (envValue != nullptr);

			if (isDefined == false) {
				const std::string error = "FATAL: You need to define the environment variable: " + propertyEnvName;
				throw std::invalid_argument(error);
			}

			const std::string propertyEnvValue = isDefined? std::string(envValue) : std::string();

			return propertyEnvValue;
		}

		const std::string getBucketNameEnvValue() const {
			const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::BUCKET_NAME);
			return value;
		}

		const EncryptionType getEncryptionTypeEnvValue() const {
			const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::ENCRYPTION_TYPE);
			const EncryptionType encryptionType = encryptionTypeFromName(value);
			return encryptionType;
		}

		const std::string getKmsKeyAmazonResourceNameEnvValue() const {
			const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::KMS_KEY_AMAZON_RESOURCE_NAME);
			return value;
		}

		const std::string getAccessKeyIdEnvValue() const {
			const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::ACCESS_KEY_ID);
			return value;
		}

		const std::string getSecretKeyEnvValue() const {
			const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::SECRET_KEY);
			return value;
		}

		const std::string getSessionTokenEnvValue() const {
			const std::string value = getConnectionPropertyEnvValue(ConnectionProperty::SESSION_TOKEN);
			return value;
		}

	protected:
		std::unique_ptr<S3FileSystem> s3FileSystem;
};

const std::string rootDirectoryName = "/test";

TEST_F(ParquetOnS3Test, ReadParquetOnS3) {

	Uri parquetFileLocation = Uri(FileSystemType::S3, AUTHORITY, Path("/tpch1gb-withnulls/lineitem_0_0.parquet"));

	try {

		std::shared_ptr<arrow::io::RandomAccessFile> randomAccessFile = s3FileSystem->openReadable(parquetFileLocation);
		std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::Open(randomAccessFile);

		// Get the File MetaData
		std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

		const parquet::SchemaDescriptor * schema = file_metadata->schema();
		int numRowGroups = file_metadata->num_row_groups();

		std::vector<unsigned long long> numRowsPerGroup(numRowGroups);

		for(int j = 0; j < numRowGroups; j++){

			std::shared_ptr<parquet::RowGroupReader> groupReader = parquet_reader->RowGroup(j);
			const parquet::RowGroupMetaData* rowGroupMetadata = groupReader->metadata();
			numRowsPerGroup[j] =rowGroupMetadata->num_rows();
		}
		parquet_reader->Close();
		randomAccessFile->Close();

		for(int rowGroupIndex  = 0; rowGroupIndex < numRowGroups; rowGroupIndex++){
			std::shared_ptr<parquet::RowGroupReader> groupReader = parquet_reader->RowGroup(rowGroupIndex);
			const parquet::RowGroupMetaData* rowGroupMetadata = groupReader->metadata();
			for(int blazingColumnIndex = 0; blazingColumnIndex < 15; blazingColumnIndex++){

				const parquet::ColumnDescriptor * column = schema->Column(blazingColumnIndex);

				std::unique_ptr<parquet::ColumnChunkMetaData> columnMetaData = rowGroupMetadata->ColumnChunk(blazingColumnIndex);

				parquet::Type::type type = column->physical_type();

				if(columnMetaData->is_stats_set()){
					std::shared_ptr<parquet::RowGroupStatistics> statistics = columnMetaData->statistics();
					EXPECT_TRUE(statistics->HasMinMax());
				}

				if ( type ==  parquet::Type::BYTE_ARRAY ){
					const std::shared_ptr<parquet::ColumnReader> columnReader = groupReader->Column(blazingColumnIndex);

					const std::shared_ptr<parquet::ByteArrayReader> parquetTypeReader = std::static_pointer_cast<parquet::ByteArrayReader>(columnReader);


					int64_t valid_bits_offset = 0;
					int64_t levels_read = 0;
					int64_t values_read = 0;
					int64_t null_count = -1;
					int64_t numRecords = rowGroupMetadata->num_rows();

					int64_t amountToRead = numRecords;

					std::vector<parquet::ByteArray> valuesBuffer(amountToRead);
					std::vector<int16_t> dresult(amountToRead, -1);
					std::vector<int16_t> rresult(amountToRead, -1); // repetition levels must not be nullptr in order to avoid skipping null values
					std::vector<uint8_t> valid_bits(amountToRead, 255);

					int64_t rows_read = parquetTypeReader->ReadBatchSpaced(amountToRead, dresult.data(), rresult.data(), &(valuesBuffer[0]), valid_bits.data(), valid_bits_offset, &levels_read, &values_read, &null_count);

				}

			}
		}
		EXPECT_TRUE(numRowsPerGroup.size() ==1);
	} catch (parquet::ParquetException& e) {
		std::string errorMessage = std::string(e.what());
		EXPECT_TRUE(errorMessage == "");


	} catch (std::exception& e) {
		std::string errorMessage = std::string(e.what());
		EXPECT_TRUE(errorMessage == "");
	}

}
