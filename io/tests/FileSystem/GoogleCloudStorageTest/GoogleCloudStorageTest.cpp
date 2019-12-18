#include <iostream>
#include <limits.h>
#include <time.h>

#include "gtest/gtest.h"

#include "arrow/status.h"

#include "FileSystem/GoogleCloudStorage.h"

using namespace GoogleCloudStorageConnection;

const std::string AUTHORITY = "jack_gcs_1";  // the file system namespace used for this test

class GoogleCloudStorageTest : public testing::Test {
protected:
	GoogleCloudStorageTest() {
		const std::string projectId = "blazingdb-jenkins";
		const std::string bucketName = "blazingsql-test";
		const std::string useDefaultAdcJsonFile = "true";
		const std::string adcJsonFile = "";
		const FileSystemConnection fileSystemConnection(projectId, bucketName, true, "");

		Path root;

		googleCloudStorage.reset(new GoogleCloudStorage(fileSystemConnection, root));
	}

	virtual ~GoogleCloudStorageTest() {}

	virtual void SetUp() {}

	virtual void TearDown() {
		// S3FileSystem->disconnect();
	}

protected:
	std::unique_ptr<GoogleCloudStorage> googleCloudStorage;
};

// TEST_F(GoogleCloudStorageTest, FileExistForFileThatIsThere) {
//    bool exists = googleCloudStorage->exists(Uri(FileSystemType::GOOGLE_CLOUD_STORAGE, AUTHORITY,
//    Path("/cudf.ultrabasic.py")));

//    EXPECT_TRUE(exists);
//}

// TEST_F(GoogleCloudStorageTest, DirectoryExist) {
//    bool exists = googleCloudStorage->exists(Uri(FileSystemType::GOOGLE_CLOUD_STORAGE, AUTHORITY,
//    Path("/new_folder_1/folder_child1/")));

//    EXPECT_TRUE(exists);
//}

// TEST_F(GoogleCloudStorageTest, FileExistForFileThatIsThereInSubSubDirectory) {
//    bool exists = googleCloudStorage->exists(Uri(FileSystemType::GOOGLE_CLOUD_STORAGE, AUTHORITY,
//    Path("/new_folder_1/folder_child1/orders.tbl")));

//    EXPECT_TRUE(exists);
//}

// TEST_F(GoogleCloudStorageTest, FileStatusOfPythonFile) {
//    const Uri uri = Uri(FileSystemType::GOOGLE_CLOUD_STORAGE, AUTHORITY, Path("/cudf.ultrabasic.py"));
//    const FileStatus fileStatus = googleCloudStorage->getFileStatus(uri);

//    EXPECT_TRUE(fileStatus.isFile());
//    EXPECT_FALSE(fileStatus.isDirectory());
//    EXPECT_EQ(fileStatus.getFileSize(), 175);
//}

// TEST_F(GoogleCloudStorageTest, FileStatusOfDirectory) {
//    const Uri uri = Uri(FileSystemType::GOOGLE_CLOUD_STORAGE, AUTHORITY, Path("/new_folder_1/binfiles"));
//    const FileStatus fileStatus = googleCloudStorage->getFileStatus(uri);

//    EXPECT_TRUE(fileStatus.isDirectory());
//    EXPECT_FALSE(fileStatus.isFile());
//    EXPECT_EQ(fileStatus.getFileSize(), 11);
//}

// TEST_F(GoogleCloudStorageTest, CanListResources) {
//    const std::vector<std::string> expected = {
//        "customer.tbl",
//        "lineitem.tbl",
//        "nation.tbl",
//        "orders.tbl",
//        "part.tbl",
//        "partsupp.tbl",
//        "region.tbl",
//        "supplier.tbl"};
//    // TODO percy simple case is with / at the end
//    const Uri uri(FileSystemType::GOOGLE_CLOUD_STORAGE, AUTHORITY, Path("/new_folder_1/folder_child1"));
//    const std::vector<std::string> result = googleCloudStorage->listResourceNames(uri);

//    EXPECT_EQ(result, expected);
//}

TEST_F(GoogleCloudStorageTest, CanListUris) {
	const std::string target = "/new_folder_1/folder_child1";
	const Uri dir(FileSystemType::GOOGLE_CLOUD_STORAGE, AUTHORITY, Path(target));

	const std::vector<Uri> expected = {Uri(dir.getFileSystemType(), dir.getAuthority(), Path(target + "/customer.tbl")),
		Uri(dir.getFileSystemType(), dir.getAuthority(), Path(target + "/lineitem.tbl")),
		Uri(dir.getFileSystemType(), dir.getAuthority(), Path(target + "/nation.tbl")),
		Uri(dir.getFileSystemType(), dir.getAuthority(), Path(target + "/orders.tbl")),
		Uri(dir.getFileSystemType(), dir.getAuthority(), Path(target + "/part.tbl")),
		Uri(dir.getFileSystemType(), dir.getAuthority(), Path(target + "/partsupp.tbl")),
		Uri(dir.getFileSystemType(), dir.getAuthority(), Path(target + "/region.tbl")),
		Uri(dir.getFileSystemType(), dir.getAuthority(), Path(target + "/supplier.tbl"))};
	// TODO percy simple case is with / at the end

	const std::vector<Uri> result = googleCloudStorage->list(dir);

	EXPECT_EQ(result, expected);
}

// TEST_F(GoogleCloudStorageTest, CreateDirecotriesTest) {
//    const std::string parent_folder = "/folder_container";

//    const std::vector<std::string> dirs = {
//        "/newdir1",
//        "/newdir2/",
//        "/newdir3/subdir1",
//        "/newdir3/subdir2/"};

//    // TODO percy simple case is with / at the end
//    const Uri parent(FileSystemType::GOOGLE_CLOUD_STORAGE, AUTHORITY, Path(parent_folder));

//    for (std::string dir : dirs) {
//        const Uri uri(parent.getFileSystemType(), parent.getAuthority(), parent_folder + dir);

//        const bool exists = googleCloudStorage->exists(uri);

//        EXPECT_FALSE(exists);

//        const bool created = googleCloudStorage->makeDirectory(uri);

//        EXPECT_TRUE(created);

//        const FileStatus fileStatus = googleCloudStorage->getFileStatus(uri);

//        EXPECT_TRUE(fileStatus.isDirectory());
//    }
//}


// TODO percy
///// ref old code delete later
////#include "FileSystem/LocalFileSystem.h"

// class LocalFileSystemTest : public testing::Test {
//	protected:
//        //LocalFileSystemTest() : localFileSystem(new LocalFileSystem()) {
//        LocalFileSystemTest() {
//		}

//		virtual ~LocalFileSystemTest() {

//		}

//		virtual void SetUp() {
//			//TODO just a dummy call for the case of local fs
//			//localFileSystem->connect();
//		}

//		virtual void TearDown() {
//			//TODO just a dummy call for the case of local fs
//			//localFileSystem->disconnect();
//		}

//	protected:
////		const std::unique_ptr<LocalFileSystem> localFileSystem;
//};

//#include <iostream>

//#include "google/cloud/storage/client.h"

// namespace gcs = google::cloud::storage;

// void GetObjectMetadata(google::cloud::storage::Client client, const std::string &bucket_name, const std::string
// &object_name) {
//  //! [get object metadata] [START storage_get_metadata]
//  namespace gcs = google::cloud::storage;
//  using ::google::cloud::StatusOr;
//  [](gcs::Client client, std::string bucket_name, std::string object_name) {
//    StatusOr<gcs::ObjectMetadata> object_metadata =
//        client.GetObjectMetadata(bucket_name, object_name);

//    if (!object_metadata) {
//      throw std::runtime_error(object_metadata.status().message());
//    }

//    std::cout << "The metadata for object " << object_metadata->name()
//              << " in bucket " << object_metadata->bucket() << " is "
//              << *object_metadata << "\n";
//  }
//  //! [get object metadata] [END storage_get_metadata]
//  (std::move(client), bucket_name, object_name);
//}

// void ReadObject(google::cloud::storage::Client client, const std::string &bucket_name, const std::string
// &object_name) {
//  //! [read object] [START storage_download_file]
//  namespace gcs = google::cloud::storage;
//  [](gcs::Client client, std::string bucket_name, std::string object_name) {
//    gcs::ObjectReadStream stream = client.ReadObject(bucket_name, object_name);

//    //percy code
//    std::cout << stream.status().ok() << std::endl;
//    std::cout << stream.status().code() << std::endl;
//    std::cout << stream.status().message() << std::endl;

//    int count = 0;
//    std::string line;
//    while (std::getline(stream, line, '\n')) {
//      ++count;
//    }

//    std::cout << "The object has " << count << " lines\n";
//  }
//  //! [read object] [END storage_download_file]
//  (std::move(client), bucket_name, object_name);
//}

// TEST_F(LocalFileSystemTest, GetFileStatusLinuxRegularFile) {

//    std::string const project_id = "blazingdb-jenkins";
//    std::string const bucket = "blazingsql-test";
//    std::string const object = "cudf.ultrabasic.py";

//    google::cloud::StatusOr<gcs::ClientOptions> opts = gcs::ClientOptions::CreateDefaultClientOptions();
//    if (!opts) {
//        std::cerr << "Couldn't create gcs::ClientOptions, status=" << opts.status();
//        return ; // 1 err
//    }

//    gcs::Client client(opts->set_project_id(project_id));

//    auto buckets = client.ListBuckets();

//    std::cout << "BUCKETS:" << std::endl;
//    for (auto bucket : buckets) {
//           std::cout << (*bucket).name() << std::endl;
//    }

//    std::cout << "METADATA:" << std::endl;
//    GetObjectMetadata(client, bucket, object);

//    std::cout << "OBJECT DATA:" << std::endl;
//    ReadObject(client, bucket, object);


////	const std::string currentExe = "/proc/self/exe";
////	const FileStatus fileStatus = localFileSystem->getFileStatus(currentExe);

////	EXPECT_TRUE(fileStatus.isFile());
////	EXPECT_FALSE(fileStatus.isDirectory());
////	EXPECT_EQ(fileStatus.getUri().getPath().toString(true), currentExe);
////	EXPECT_TRUE(fileStatus.getFileSize() > 0);
//}

////TEST_F(LocalFileSystemTest, GetFileStatusLinuxDirectory) {
////	const std::string currentExe = "/proc/self/net";
////	const FileStatus fileStatus = localFileSystem->getFileStatus(currentExe);

////	EXPECT_FALSE(fileStatus.isFile());
////	EXPECT_TRUE(fileStatus.isDirectory());
////	EXPECT_EQ(fileStatus.getUri().getPath().toString(true), currentExe);
////	EXPECT_EQ(fileStatus.getFileSize(), 0);
////}

////TEST_F(LocalFileSystemTest, CanListLinuxRootDirectories) {
////	const std::set<std::string> dirs = {"/root", "/home", "/etc"};

////	const std::vector<Uri> files = localFileSystem->list(Uri("/"));

////	int foundCount = 0;

////	for (const Uri &uri : files) {
////		const Path path = uri.getPath();

////		//TODO create a STD common code (for test only?) where we can put helpers like find(value, container)
////		const bool found = (dirs.find(path.toString()) != dirs.end());

////		if (found) {
////			//TODO improve this part of the test
////			//EXPECT_TRUE(path.isDirectory());
////			++foundCount;
////		}
////	}

////	EXPECT_EQ(foundCount, dirs.size());
////}

////TEST_F(LocalFileSystemTest, CheckIgnoreDotAnd2DotsWhenListLinuxRoot9999) {
////	const std::vector<Uri> files = localFileSystem->list(Uri("/"));

////	for (const Uri &uri : files) {
////		const Path path = uri.getPath();
////		const std::string test = path.toString(true);
////		const bool found1DotOr2Dots =  (test == "/.") || (test == "/..");
////		EXPECT_FALSE(found1DotOr2Dots);
////	}
////}
