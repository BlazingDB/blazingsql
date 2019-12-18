#include <string>
#include <vector>

#include "Common/HadoopFileSystemTest.h"

using std::string;
using std::vector;

bool search_in(const std::vector<std::string> & filenames, const std::string & s) {
	for(std::string filename : filenames) {
		if(filename == s) {
			return true;
		}
	}
	return false;
}

TEST_F(HadoopFileSystemTest, ResourcesDirectory) {
	vector<string> filenames;

	const vector<Uri> uris = hadoopFileSystem->list(uri("/resources"));

	for(const Uri & uri : uris) {
		filenames.push_back(uri.getPath().toString());
	}

	EXPECT_TRUE(search_in(filenames, "/resources/directory01"));
	EXPECT_TRUE(search_in(filenames, "/resources/empty_file"));
	EXPECT_TRUE(search_in(filenames, "/resources/file01"));
}

TEST_F(HadoopFileSystemTest, RootDirectory) {
	vector<string> filenames;

	const vector<Uri> uris = hadoopFileSystem->list(uri("/"));

	for(const Uri & uri : uris) {
		filenames.push_back(uri.getPath().toString(true));
	}

	EXPECT_TRUE(search_in(filenames, "/resources"));
}
