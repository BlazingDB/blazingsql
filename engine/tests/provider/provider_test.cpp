#include <fstream>
#include "tests/utilities/BlazingUnitTest.h"
#include "io/data_provider/UriDataProvider.h"
#include "FileSystem/LocalFileSystem.h"
#include "Util/StringUtil.h"

const std::string BLAZING_TMP_PATH = "/tmp/blazing";

struct ProviderTest : public BlazingUnitTest {};

TEST_F(ProviderTest, non_existent_directory) {
    
    std::string filename = "/fake/";
	std::vector<Uri> uris = {Uri{filename}};

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

bool create_dummy_file(std::string content, std::string filename){
	std::ofstream outfile(filename, std::ofstream::out);
	if( outfile.is_open() )
	{
		outfile << content << std::endl;
		return true;
	}

	return false;
}

bool create_folder_test()
{
	LocalFileSystem localFileSystem(Path("/"));
	localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	localFileSystem.makeDirectory(Uri{BLAZING_TMP_PATH});
	return localFileSystem.exists(Uri(BLAZING_TMP_PATH));
}

void remove_dummy_file(std::vector<Uri> uris){
	LocalFileSystem localFileSystem( Path("/") );
	for( Uri & p : uris )
	{
		localFileSystem.remove(p);
	}
}
void remove_dummy_file(std::vector<std::string> uris){
	LocalFileSystem localFileSystem( Path("/") );
	for( std::string & p : uris )
	{
		localFileSystem.remove(Uri(p));
	}
}
TEST_F(ProviderTest, ignoring_dummy_files) {
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files = {
		BLAZING_TMP_PATH + "/file.crc", BLAZING_TMP_PATH + "/file_SUCCESS", BLAZING_TMP_PATH + "/file_metadata", BLAZING_TMP_PATH + "/file.csv"};

	ASSERT_TRUE(create_dummy_file("some crc", test_files[0]));
	ASSERT_TRUE(create_dummy_file("some flag", test_files[1]));
	ASSERT_TRUE(create_dummy_file("some meta", test_files[2]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[3]));

    std::vector<Uri> uris = {Uri{BLAZING_TMP_PATH + "/file*"}};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

    bool open_file = false;

    std::vector<std::string> result;

    while(provider->has_next()){
        ral::io::data_handle new_handle = provider->get_next(open_file);
        std::string file_name = new_handle.uri.toString(true);
        result.push_back(file_name);
    }

	remove_dummy_file(test_files);

    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], BLAZING_TMP_PATH + "/file.csv");
}

TEST_F(ProviderTest, empty_dir) {
	std::unique_ptr<LocalFileSystem> localFileSystem(new LocalFileSystem(Path("/")));
	ASSERT_TRUE(create_folder_test());

    const int length = 10;
    std::string dirname = "/tmp/" + randomString(length);

    bool dir_create_ok = localFileSystem->makeDirectory(Uri{dirname});
    ASSERT_TRUE(dir_create_ok);

    std::vector<Uri> uris = {Uri{dirname}};
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

    bool open_file = false;

    std::vector<std::string> result;

    if(provider->has_next()){
        ral::io::data_handle new_handle = provider->get_next(open_file);
        // an empty folder must return an empty handle
        EXPECT_EQ(new_handle.uri.isEmpty(), true);
    }

    bool dir_remove_ok = localFileSystem->remove(Uri{dirname});
    ASSERT_TRUE(dir_remove_ok);
}

TEST_F(ProviderTest, folder_with_one_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<Uri> uris = {Uri(BLAZING_TMP_PATH + "/file.csv")};

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[0].toString()));

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	if( provider->has_next() )
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		EXPECT_EQ(new_handle.uri.toString(), uris[0].toString());
	}

	remove_dummy_file(uris);
}

TEST_F(ProviderTest, folder_with_one_file_ignore_missing_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<Uri> uris{Uri(BLAZING_TMP_PATH + "/filezxc.csv")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, true);

	bool open_file = false;

	if( provider->has_next() )
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		std::string name = new_handle.uri.toString();
		EXPECT_EQ(name, "");
		EXPECT_EQ(new_handle.uri.isValid(), false);
	}
}

TEST_F(ProviderTest, folder_multiple_files)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<Uri> uris = {
		Uri(BLAZING_TMP_PATH + "/file.orc"),
		Uri(BLAZING_TMP_PATH + "/file_SUCCESS.csv"),
		Uri(BLAZING_TMP_PATH + "/file_metadata.csv"),
		Uri(BLAZING_TMP_PATH + "/file.csv")};

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[0].toString()));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[1].toString()));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[2].toString()));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[3].toString()));

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<Uri> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		result.emplace_back(new_handle.uri.toString());
	}

	EXPECT_EQ(uris, result);

	remove_dummy_file(uris);
}

TEST_F(ProviderTest, folder_multiple_files_ignore_missing_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<Uri> uris = {
		Uri(BLAZING_TMP_PATH + "/file.orc"),
		Uri(BLAZING_TMP_PATH + "/file_SUCCESS.csv"),
		Uri(BLAZING_TMP_PATH + "/file_metadata.csv"),
		Uri(BLAZING_TMP_PATH + "/file.csv")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, true);

	bool open_file = false;

	std::vector<Uri> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		EXPECT_EQ(new_handle.uri.isValid(), false);
	}
}

TEST_F(ProviderTest, folder_multiple_files_one_empty_folder)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{BLAZING_TMP_PATH + "/file.orc",
										BLAZING_TMP_PATH + "/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/file_metadata.csv",
										BLAZING_TMP_PATH + "/file.csv"};

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[0]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[1]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[2]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[3]));

	LocalFileSystem localFileSystem(Path("/"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/emptyFolder"));

	std::vector<Uri> uris = {
		Uri(test_files[0]),
		Uri(test_files[1]),
		Uri(test_files[2]),
		Uri(test_files[3]),
		Uri(BLAZING_TMP_PATH + "/emptyFolder/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		if(new_handle.is_valid()) result.emplace_back(new_handle.uri.toString());
	}

	EXPECT_EQ(test_files, result);

	remove_dummy_file(test_files);
	bool dir_remove_ok = localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	ASSERT_TRUE(dir_remove_ok);
}

TEST_F(ProviderTest, folder_multiple_files_one_empty_folder_ignore_missing_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{BLAZING_TMP_PATH + "/file.orc",
										BLAZING_TMP_PATH + "/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/file_metadata.csv",
										BLAZING_TMP_PATH + "/file.csv"};

	std::vector<Uri> uris = {
		Uri(test_files[0]),
		Uri(test_files[1]),
		Uri(test_files[2]),
		Uri(test_files[3]),
		Uri(BLAZING_TMP_PATH + "/emptyFolder/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, true);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		EXPECT_EQ(new_handle.uri.isValid(), false);
	}
}

TEST_F(ProviderTest, folder_multiple_files_one_non_empty_folder)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{BLAZING_TMP_PATH + "/file.orc",
										BLAZING_TMP_PATH + "/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/file_metadata.csv",
										BLAZING_TMP_PATH + "/file.csv",
										BLAZING_TMP_PATH + "/folder/file.orc",
										BLAZING_TMP_PATH + "/folder/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder/file_metadata.csv",
										BLAZING_TMP_PATH + "/folder/file.csv"};

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[0]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[1]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[2]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[3]));

	LocalFileSystem localFileSystem(Path("/"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder"));

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[4]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[5]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[6]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[7]));

	std::vector<Uri> uris = {
		Uri(test_files[0]),
		Uri(test_files[1]),
		Uri(test_files[2]),
		Uri(test_files[3]),
		Uri(BLAZING_TMP_PATH + "/folder/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		if(new_handle.is_valid()) result.emplace_back(new_handle.uri.toString());
	}

	std::sort(test_files.begin(), test_files.end());
	std::sort(result.begin(), result.end());
	EXPECT_EQ(test_files, result);

	remove_dummy_file(test_files);
	bool dir_remove_ok = localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	ASSERT_TRUE(dir_remove_ok);
}

TEST_F(ProviderTest, folder_multiple_files_one_non_empty_folder_ignore_missing_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{BLAZING_TMP_PATH + "/file.orc",
										BLAZING_TMP_PATH + "/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/file_metadata.csv",
										BLAZING_TMP_PATH + "/file.csv",
										BLAZING_TMP_PATH + "/folder/file.orc",
										BLAZING_TMP_PATH + "/folder/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder/file_metadata.csv",
										BLAZING_TMP_PATH + "/folder/file.csv"};

	std::vector<Uri> uris = {
		Uri(test_files[0]),
		Uri(test_files[1]),
		Uri(test_files[2]),
		Uri(test_files[3]),
		Uri(BLAZING_TMP_PATH + "/folder/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, true);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		EXPECT_EQ(new_handle.uri.isValid(), false);
	}
}

TEST_F(ProviderTest, folder_multiple_folder_on_multiple_folder)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{BLAZING_TMP_PATH + "/folder1/file.orc",
										BLAZING_TMP_PATH + "/folder1/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder2/file.orc",
										BLAZING_TMP_PATH + "/folder2/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder3/file.orc",
										BLAZING_TMP_PATH + "/folder3/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder4/file.orc",
										BLAZING_TMP_PATH + "/folder4/file_SUCCESS.csv"};

	LocalFileSystem localFileSystem(Path{BLAZING_TMP_PATH});
	localFileSystem.makeDirectory(Uri("/folder1"));
	localFileSystem.makeDirectory(Uri("/folder2"));
	localFileSystem.makeDirectory(Uri("/folder3"));
	localFileSystem.makeDirectory(Uri("/folder4"));

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[0]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[1]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[2]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[3]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[4]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[5]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[6]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[7]));

	std::vector<Uri> uris = {
		Uri(BLAZING_TMP_PATH + "/folder1/*"),
		Uri(BLAZING_TMP_PATH + "/folder2/*"),
		Uri(BLAZING_TMP_PATH + "/folder3/*"),
		Uri(BLAZING_TMP_PATH + "/folder4/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		if(new_handle.is_valid()) result.emplace_back(new_handle.uri.toString());
	}

	std::sort(test_files.begin(), test_files.end());
	std::sort(result.begin(), result.end());
	EXPECT_EQ(test_files, result);

	remove_dummy_file(test_files);
	bool dir_remove_ok = localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	ASSERT_TRUE(dir_remove_ok);
}

TEST_F(ProviderTest, folder_multiple_folder_on_multiple_folder_ignore_missing_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<Uri> uris = {
		Uri(BLAZING_TMP_PATH + "/folder1/*"),
		Uri(BLAZING_TMP_PATH + "/folder2/*"),
		Uri(BLAZING_TMP_PATH + "/folder3/*"),
		Uri(BLAZING_TMP_PATH + "/folder4/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, true);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		EXPECT_EQ(new_handle.uri.isValid(), false);
	}
}

TEST_F(ProviderTest, wildcard_return_nothing)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<Uri> uris = {Uri(BLAZING_TMP_PATH + "/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		result.emplace_back(new_handle.uri.toString());
	}

	EXPECT_EQ(result, std::vector<std::string>{""});
}

TEST_F(ProviderTest, wildcard_one_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{
		BLAZING_TMP_PATH + "/fileone.orc", BLAZING_TMP_PATH + "/filetwo.orc", BLAZING_TMP_PATH + "/filethree.orc"};

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[0]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[1]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[2]));

	std::vector<Uri> uris = {Uri(BLAZING_TMP_PATH + "/*ileo*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		result.emplace_back(new_handle.uri.toString());
	}

	EXPECT_EQ(result, std::vector<std::string>{BLAZING_TMP_PATH + "/fileone.orc"});

	remove_dummy_file(test_files);
}

TEST_F(ProviderTest, wildcard_multiple_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{
		BLAZING_TMP_PATH + "/fileone.orc", BLAZING_TMP_PATH + "/filetwo.orc", BLAZING_TMP_PATH + "/filethree.orc"};

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[0]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[1]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[2]));

	std::vector<Uri> uris = {Uri(BLAZING_TMP_PATH + "/*ilet*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		result.emplace_back(new_handle.uri.toString());
	}

	std::vector<std::string> cmp{BLAZING_TMP_PATH + "/filetwo.orc", BLAZING_TMP_PATH + "/filethree.orc"};
	std::sort(cmp.begin(), cmp.end());
	std::sort(result.begin(), result.end());
	EXPECT_EQ(result, cmp);

	remove_dummy_file(test_files);
}

///\TODO Recursive wildcard not supported (/*) - skip test
TEST_F(ProviderTest, wilcard_recursive)
{
	GTEST_SKIP();

	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{BLAZING_TMP_PATH + "/folder1/file.orc",
										BLAZING_TMP_PATH + "/folder1/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder2/file.orc",
										BLAZING_TMP_PATH + "/folder2/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder3/file.orc",
										BLAZING_TMP_PATH + "/folder3/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder4/file.orc",
										BLAZING_TMP_PATH + "/folder4/file_SUCCESS.csv"};

	LocalFileSystem localFileSystem(Path("/"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder1"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder2"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder3"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder4"));

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[0]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[1]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[2]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[3]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[4]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[5]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[6]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[7]));

	std::vector<Uri> uris = {Uri(BLAZING_TMP_PATH + "/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		if(new_handle.is_valid()) result.emplace_back(new_handle.uri.toString());
	}

	std::sort(test_files.begin(), test_files.end());
	std::sort(result.begin(), result.end());
	EXPECT_EQ(test_files, result);

	remove_dummy_file(test_files);
	bool dir_remove_ok = localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	ASSERT_TRUE(dir_remove_ok);
}

///\TODO Wildcard folder not supported (/folder*/*) - skip test
TEST_F(ProviderTest, wilcard_folder)
{
	GTEST_SKIP();

	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> test_files{BLAZING_TMP_PATH + "/folder1/file.orc",
										BLAZING_TMP_PATH + "/folder1/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder2/file.orc",
										BLAZING_TMP_PATH + "/folder2/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder3/file.orc",
										BLAZING_TMP_PATH + "/folder3/file_SUCCESS.csv",
										BLAZING_TMP_PATH + "/folder4/file.orc",
										BLAZING_TMP_PATH + "/folder4/file_SUCCESS.csv"};

	LocalFileSystem localFileSystem(Path("/"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder1"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder2"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder3"));
	localFileSystem.makeDirectory(Uri(BLAZING_TMP_PATH + "/folder4"));

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[0]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[1]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[2]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[3]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[4]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[5]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[6]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", test_files[7]));

	std::vector<Uri> uris = {Uri(BLAZING_TMP_PATH + "/folder*/*")};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		if(new_handle.is_valid()) result.emplace_back(new_handle.uri.toString());
	}

	std::sort(test_files.begin(), test_files.end());
	std::sort(result.begin(), result.end());
	EXPECT_EQ(test_files, result);

	remove_dummy_file(test_files);
	bool dir_remove_ok = localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	ASSERT_TRUE(dir_remove_ok);
}

TEST_F(ProviderTest, catch_exception_ignore_missing_paths)
{
	try
	{
		ASSERT_TRUE(create_folder_test());

		std::vector<Uri> uris{Uri(BLAZING_TMP_PATH + "/file.csv")};

		auto provider = std::make_shared<ral::io::uri_data_provider>(uris, false);

		bool open_file = false;

		if( provider->has_next() )
		{
			ral::io::data_handle new_handle = provider->get_next(open_file);
		}

		FAIL();
	}
	catch(const std::runtime_error& e)
	{
		SUCCEED();
	}
}

bool make_directories_hive()
{
	LocalFileSystem localFileSystem(Path{BLAZING_TMP_PATH});
	localFileSystem.makeDirectory(Uri("/t_year=2017"));
	localFileSystem.makeDirectory(Uri("/t_year=2018"));
	localFileSystem.makeDirectory(Uri("/t_year=2017/t_company_id=2"));
	localFileSystem.makeDirectory(Uri("/t_year=2017/t_company_id=4"));
	localFileSystem.makeDirectory(Uri("/t_year=2018/t_company_id=6"));
	localFileSystem.makeDirectory(Uri("/t_year=2017/t_company_id=2/region=asia"));
	localFileSystem.makeDirectory(Uri("/t_year=2017/t_company_id=4/region=asia"));
	localFileSystem.makeDirectory(Uri("/t_year=2017/t_company_id=4/region=europa"));
	localFileSystem.makeDirectory(Uri("/t_year=2018/t_company_id=6/region=europa"));
}

TEST_F(ProviderTest, uri_values_one_folder_multiple_files_wildcard)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<std::string> uri_files = {
		BLAZING_TMP_PATH + "/t_year=2017/t_company_id=2/region=asia/file1.parquet",
		BLAZING_TMP_PATH + "/t_year=2017/t_company_id=2/region=asia/file2.parquet",
		BLAZING_TMP_PATH + "/t_year=2017/t_company_id=4/region=asia/file3.parquet",
		BLAZING_TMP_PATH + "/t_year=2017/t_company_id=4/region=europa/file4.parquet",
		BLAZING_TMP_PATH + "/t_year=2018/t_company_id=6/region=europa/file5.parquet",
		BLAZING_TMP_PATH + "/t_year=2018/t_company_id=6/region=europa/file6.parquet",
	};
	std::vector<Uri> uris = {
		Uri(BLAZING_TMP_PATH + "/t_year=2017/t_company_id=2/region=asia/*"),
		Uri(BLAZING_TMP_PATH + "/t_year=2017/t_company_id=4/region=asia/*"),
		Uri(BLAZING_TMP_PATH + "/t_year=2017/t_company_id=4/region=europa/*"),
		Uri(BLAZING_TMP_PATH + "/t_year=2018/t_company_id=6/region=europa/*"),
	};

	make_directories_hive();

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uri_files[0]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uri_files[1]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uri_files[2]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uri_files[3]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uri_files[4]));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uri_files[5]));

	std::vector<std::map<std::string, std::string>> uri_values
	{
			{{"t_year", "2017"}, {"t_company_id", "2"}, {"region", "asia"}},
			{{"t_year", "2017"}, {"t_company_id", "4"}, {"region", "asia"}},
			{{"t_year", "2017"}, {"t_company_id", "4"}, {"region", "europa"}},
			{{"t_year", "2018"}, {"t_company_id", "6"}, {"region", "europa"}}
	};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, uri_values);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		result.emplace_back(new_handle.uri.toString());
	}

	std::sort(uri_files.begin(), uri_files.end());
	std::sort(result.begin(), result.end());
	EXPECT_EQ(uri_files, result);

	remove_dummy_file(uris);
	LocalFileSystem localFileSystem(Path(""));
	bool dir_remove_ok = localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	ASSERT_TRUE(dir_remove_ok);
}

TEST_F(ProviderTest, uri_values_one_folder_multiple_files)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<Uri> uris = {
		Uri(BLAZING_TMP_PATH + "/t_year=2017/t_company_id=2/region=asia/file1.parquet"),
		Uri(BLAZING_TMP_PATH + "/t_year=2017/t_company_id=2/region=asia/file2.parquet"),
		Uri(BLAZING_TMP_PATH + "/t_year=2017/t_company_id=4/region=asia/file3.parquet"),
		Uri(BLAZING_TMP_PATH + "/t_year=2017/t_company_id=4/region=europa/file4.parquet"),
		Uri(BLAZING_TMP_PATH + "/t_year=2018/t_company_id=6/region=europa/file5.parquet"),
		Uri(BLAZING_TMP_PATH + "/t_year=2018/t_company_id=6/region=europa/file6.parquet"),
	};

	make_directories_hive();

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[0].toString()));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[1].toString()));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[2].toString()));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[3].toString()));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[4].toString()));
	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[5].toString()));

	std::vector<std::map<std::string, std::string>> uri_values
		{
			{{"t_year", "2017"}, {"t_company_id", "2"}, {"region", "asia"}},
			{{"t_year", "2017"}, {"t_company_id", "2"}, {"region", "asia"}},
			{{"t_year", "2017"}, {"t_company_id", "4"}, {"region", "asia"}},
			{{"t_year", "2017"}, {"t_company_id", "4"}, {"region", "europa"}},
			{{"t_year", "2018"}, {"t_company_id", "6"}, {"region", "europa"}},
			{{"t_year", "2018"}, {"t_company_id", "6"}, {"region", "europa"}}
		};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, uri_values);

	bool open_file = false;

	std::vector<std::string> result;

	while(provider->has_next())
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		result.emplace_back(new_handle.uri.toString());
	}

	std::vector<std::string> cmp;
	std::transform(uris.begin(), uris.end(), std::back_inserter(cmp), [](const Uri& uri){return uri.toString();});

	std::sort(cmp.begin(), cmp.end());
	std::sort(result.begin(), result.end());
	EXPECT_EQ(cmp, result);

	remove_dummy_file(uris);
	LocalFileSystem localFileSystem(Path(""));
	bool dir_remove_ok = localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	ASSERT_TRUE(dir_remove_ok);
}

TEST_F(ProviderTest, uri_values_folder_with_one_file)
{
	ASSERT_TRUE(create_folder_test());

	std::vector<Uri> uris = {Uri(BLAZING_TMP_PATH + "/t_year=2017/t_company_id=2/region=asia/file.csv")};

	make_directories_hive();

	ASSERT_TRUE(create_dummy_file("a|b\n0|0", uris[0].toString()));

	std::vector<std::map<std::string, std::string>> uri_values
		{
			{{"t_year", "2017"}, {"t_company_id", "2"}, {"region", "asia"}}
		};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, uri_values);

	bool open_file = false;

	if( provider->has_next() )
	{
		ral::io::data_handle new_handle = provider->get_next(open_file);
		EXPECT_EQ(new_handle.uri.toString(), uris[0].toString());
	}

	remove_dummy_file(uris);
	LocalFileSystem localFileSystem(Path(""));
	bool dir_remove_ok = localFileSystem.remove(Uri{BLAZING_TMP_PATH});
	ASSERT_TRUE(dir_remove_ok);
}

TEST_F(ProviderTest, uri_values_empty_dir) {
	std::unique_ptr<LocalFileSystem> localFileSystem(new LocalFileSystem(Path("/")));

	const int length = 10;
	std::string dirname = "/tmp/" + randomString(length);

	bool dir_create_ok = localFileSystem->makeDirectory(Uri{dirname});
	ASSERT_TRUE(dir_create_ok);

	std::vector<Uri> uris = {Uri{dirname}};
	std::vector<std::map<std::string, std::string>> uri_values
		{
			{{"t_year", "2017"}, {"t_company_id", "2"}, {"region", "asia"}}
		};

	auto provider = std::make_shared<ral::io::uri_data_provider>(uris, uri_values);

	bool open_file = false;

	std::vector<std::string> result;

	if(provider->has_next()){
		ral::io::data_handle new_handle = provider->get_next(open_file);
		EXPECT_EQ(new_handle.uri.isEmpty(), true);
	}

	bool dir_remove_ok = localFileSystem->remove(Uri{dirname});
	ASSERT_TRUE(dir_remove_ok);
}