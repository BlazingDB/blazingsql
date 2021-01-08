#include <gtest/gtest.h>
#include "Util/StringUtil.h"

TEST(FindAndReplaceAll, NoMatches) {
    std::string data="No Matches";
    std::string toSearch="SQL";
    std::string replaceStr="Nevermind";

    StringUtil::findAndReplaceAll(data, toSearch, replaceStr);
    ASSERT_EQ(data, data);
}

TEST(FindAndReplaceAll, TwoMatches) {
    std::string data="LogicalJoin(condition=[AND(IS NOT DISTINCT FROM($0, $3), IS NOT DISTINCT FROM($1, $4))], joinType=[inner])";
    std::string toSearch="IS NOT DISTINCT FROM";
    std::string replaceStr="=";
    std::string expected="LogicalJoin(condition=[AND(=($0, $3), =($1, $4))], joinType=[inner])";

    StringUtil::findAndReplaceAll(data, toSearch, replaceStr);
    ASSERT_EQ(data, expected);
}

TEST(FindAndReplaceAll, SeveralMatches) {
    std::string data="LogicalProject(EXPR$0=[RAND()], EXPR$1=[RAND()], EXPR$2=[RAND()], EXPR$3=[RAND()], EXPR$4=[RAND()])";
    std::string toSearch="RAND()";
    std::string replaceStr="BLZ_RND()";
    std::string expected="LogicalProject(EXPR$0=[BLZ_RND()], EXPR$1=[BLZ_RND()], EXPR$2=[BLZ_RND()], EXPR$3=[BLZ_RND()], EXPR$4=[BLZ_RND()])";

    StringUtil::findAndReplaceAll(data, toSearch, replaceStr);
    ASSERT_EQ(data, expected);
}

TEST(FindAndReplaceAll, PatternLimits) {
    std::string data="AbcbAbbAbbAbbAbbAbc";
    std::string toSearch="Abc";
    std::string replaceStr="C";
    std::string expected="CbAbbAbbAbbAbbC";

    StringUtil::findAndReplaceAll(data, toSearch, replaceStr);
    ASSERT_EQ(data, expected);
}

TEST(FindAndReplaceAll, RemoveSpaces) {
    std::string data="The quick brown fox jumps over the lazy dog.";
    std::string toSearch=" ";
    std::string replaceStr="";
    std::string expected="Thequickbrownfoxjumpsoverthelazydog.";

    StringUtil::findAndReplaceAll(data, toSearch, replaceStr);
    ASSERT_EQ(data, expected);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}