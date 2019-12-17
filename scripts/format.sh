#!/bin/bash

# INSTALL
# sudo apt install clang-format clang-format-3.8
# pip install autopep8==1.4.4

# NOTES: for Ubuntu Xenial 14.06 we are working with clang-format-3.8
# which is the default clang-format from official repositories.

# USAGE: run `./scripts/format.sh` from root project folder (folder with .git)

find . \( -name \*.cpp -o -name \*.h -o -name \*.cc -o -name \*.hpp -o -name \*.cxx -o -name \*.hxx -o -name \*.java \) -exec clang-format -i -style=file {} \;
find . -name \*.py -exec autopep8 -i -a -a {} \;
