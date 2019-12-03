#!/bin/bash

find . \( -name \*.cpp -o -name \*.h -o -name \*.cc -o -name \*.hpp -o -name \*.cxx -o -name \*.hxx -o -name \*.java \) -exec clang-format -i -style=file {} \;
find . -name \*.py -exec autopep8 -i -a -a {} \;
