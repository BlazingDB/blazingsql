#!/bin/bash
# usage: ./build.sh build_type test
# example: ./build.sh Release|Debug ON|OFF

set -e

echo "  - blazingsql-nightly" >> /conda/.condarc
${WORKSPACE}/build.sh
