#include <fstream>
#include "tests/utilities/BlazingUnitTest.h"
#include "io/data_provider/UriDataProvider.h"

struct ProviderTest : public BlazingUnitTest {};

TEST_F(ProviderTest, non_existent_directory) {
    
    std::string filename = "/fake/";
	std::vector<Uri> uris;
	uris.push_back(Uri{filename});

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

    bool open_file = false;
    if(provider->has_next()){
        try{
            ral::io::data_handle new_handle = provider->get_next(open_file);
            FAIL();
        }
        catch(std::runtime_error e){
            SUCCEED();
        }
        catch(std::exception e){
            FAIL();
        }
        catch(...){
            FAIL();
        }
    }
}

void create_dummy_file(std::string content, std::string filename){
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();
}

TEST_F(ProviderTest, ignoring_dummy_files) {
    
    std::vector<std::string> test_files = {"/tmp/file.crc", "/tmp/file_SUCCESS", "/tmp/file_metadata", "/tmp/file.csv"};

    create_dummy_file("some crc", test_files[0]);
    create_dummy_file("some flag", test_files[1]);
    create_dummy_file("some meta", test_files[2]);
    create_dummy_file("a|b\n0|0", test_files[3]);

    std::vector<Uri> uris;
    uris.push_back(Uri{"/tmp/file*"});

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

    bool open_file = false;

    std::vector<std::string> result;

    while(provider->has_next()){
        ral::io::data_handle new_handle = provider->get_next(open_file);
        std::string file_name = new_handle.uri.toString(true);
        result.push_back(file_name);
    }

    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], "/tmp/file.csv");
}