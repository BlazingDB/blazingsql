/*
 * Copyright 2020 BlazingDB, Inc.
 *     Copyright 2020 Percy Camilo Triveño Aucahuasi <percy@blazingsql.com>
 */

// NOTE declare here all static functions that output fixed data

#ifndef _BSQL_ENGINE_STATIC_H_
#define _BSQL_ENGINE_STATIC_H_

#include <map>
#include <string>

// Returns all the information about the blazingsql binary (git hash, version, os, compiler used, etc.)
std::map<std::string, std::string> getProductDetails();

#endif // _BSQL_ENGINE_STATIC_H_
