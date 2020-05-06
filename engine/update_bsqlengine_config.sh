#=============================================================================
# Copyright 2020 BlazingDB, Inc.
#     Copyright 2020 Percy Camilo Triveño Aucahuasi <percy@blazingsql.com>
#=============================================================================

#!/bin/bash
build_dir=$1

bsqlengine_git_config_path=$build_dir/bsqlengine_internal_config.h

rm -f $bsqlengine_git_config_path

# Get the current branch
blazingsql_git_branch=$(git rev-parse --abbrev-ref HEAD)

# Get the latest abbreviated commit hash
blazingsql_git_commit_hash=$(git log -1 --format=%H)

# Get latest tag and number of commits since tag
blazingsql_git_describe_tag=$(git describe --abbrev=0 --tags)
blazingsql_git_describe_number=$(git rev-list ${blazingsql_git_describe_tag}..HEAD --count)

cat <<EOT >> $bsqlengine_git_config_path
#ifndef GIT_CONFIG_blazingsql_H
#define GIT_CONFIG_blazingsql_H

#define BLAZINGSQL_GIT_BRANCH std::make_pair("BLAZINGSQL_GIT_BRANCH", "$blazingsql_git_branch")
#define BLAZINGSQL_GIT_COMMIT_HASH std::make_pair("BLAZINGSQL_GIT_COMMIT_HASH", "$blazingsql_git_commit_hash")

#define BLAZINGSQL_GIT_DESCRIBE_TAG std::make_pair("BLAZINGSQL_GIT_DESCRIBE_TAG", "$blazingsql_git_describe_tag")
#define BLAZINGSQL_GIT_DESCRIBE_NUMBER std::make_pair("BLAZINGSQL_GIT_DESCRIBE_NUMBER", "$blazingsql_git_describe_number")

#endif // GIT_CONFIG_blazingsql_H
EOT
