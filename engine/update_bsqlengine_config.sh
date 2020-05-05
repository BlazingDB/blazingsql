#=============================================================================
# Copyright 2020 BlazingDB, Inc.
#     Copyright 2020 Percy Camilo Triveño Aucahuasi <percy@blazingsql.com>
#=============================================================================

#!/bin/bash
build_dir=$1

bsqlengine_git_config_path=$build_dir/bsqlengine_internal_config.h

rm -f $bsqlengine_git_config_path

# Get the current branch
simplicity_git_branch=$(git rev-parse --abbrev-ref HEAD)

# Get the latest abbreviated commit hash
simplicity_git_commit_hash=$(git log -1 --format=%H)

cat <<EOT >> $bsqlengine_git_config_path
#ifndef GIT_CONFIG_SIMPLICITY_H
#define GIT_CONFIG_SIMPLICITY_H

#define SIMPLICITY_GIT_BRANCH std::make_pair("GIT_BRANCH", "$simplicity_git_branch")
#define SIMPLICITY_GIT_COMMIT_HASH std::make_pair("GIT_COMMIT_HASH", "$simplicity_git_commit_hash")

#endif // GIT_CONFIG_SIMPLICITY_H
EOT
