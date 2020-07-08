#include "port.h"
#include "kernel.h"

namespace ral {
namespace cache {
void port::register_port(std::string port_name) { cache_machines_[port_name] = nullptr; }

#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
#include <cxxabi.h>

/** Print a demangled stack backtrace of the caller function to FILE* out. */
static inline void print_stacktrace(FILE *out = stderr, unsigned int max_frames = 63)
{
    fprintf(out, "stack trace:\n");

    // storage array for stack trace address data
    void* addrlist[max_frames+1];

    // retrieve current stack addresses
    int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

    if (addrlen == 0) {
	fprintf(out, "  <empty, possibly corrupt>\n");
	return;
    }

    // resolve addresses into strings containing "filename(function+address)",
    // this array must be free()-ed
    char** symbollist = backtrace_symbols(addrlist, addrlen);

    // allocate string which will be filled with the demangled function name
    size_t funcnamesize = 256;
    char* funcname = (char*)malloc(funcnamesize);

    // iterate over the returned symbol lines. skip the first, it is the
    // address of this function.
    for (int i = 1; i < addrlen; i++)
    {
	char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

	// find parentheses and +address offset surrounding the mangled name:
	// ./module(function+0x15c) [0x8048a6d]
	for (char *p = symbollist[i]; *p; ++p)
	{
	    if (*p == '(')
		begin_name = p;
	    else if (*p == '+')
		begin_offset = p;
	    else if (*p == ')' && begin_offset) {
		end_offset = p;
		break;
	    }
	}

	if (begin_name && begin_offset && end_offset
	    && begin_name < begin_offset)
	{
	    *begin_name++ = '\0';
	    *begin_offset++ = '\0';
	    *end_offset = '\0';

	    // mangled name is now in [begin_name, begin_offset) and caller
	    // offset in [begin_offset, end_offset). now apply
	    // __cxa_demangle():

	    int status;
	    char* ret = abi::__cxa_demangle(begin_name,
					    funcname, &funcnamesize, &status);
	    if (status == 0) {
		funcname = ret; // use possibly realloc()-ed string
		fprintf(out, "  %s : %s+%s\n",
			symbollist[i], funcname, begin_offset);
	    }
	    else {
		// demangling failed. Output function name as a C function with
		// no arguments.
		fprintf(out, "  %s : %s()+%s\n",
			symbollist[i], begin_name, begin_offset);
	    }
	}
	else
	{
	    // couldn't parse the line? print the whole line.
	    fprintf(out, "  %s\n", symbollist[i]);
	}
    }

    free(funcname);
    free(symbollist);
}
std::shared_ptr<CacheMachine> & port::get_cache(const std::string & port_name) {
	if(port_name.length() == 0) {
		// NOTE: id is the `default` cache_machine name
		auto id = std::to_string(kernel_->get_id());
		auto it = cache_machines_.find(id);
		return it->second;
	}
	auto it = cache_machines_.find(port_name);
	if (it == cache_machines_.end())
		std::cout<<"ERROR get_cache did not find cache "<<port_name<<std::endl;
		print_stacktrace();
	return it->second;
}

void port::register_cache(const std::string & port_name, std::shared_ptr<CacheMachine> cache_machine) {
	this->cache_machines_[port_name] = cache_machine;
}
void port::finish() {
	for(auto it : cache_machines_) {
		it.second->finish();
	}
}

bool port::all_finished(){
	for (auto cache : cache_machines_){
		if (!cache.second->is_finished())
			return false;
	}
	return true;
}

bool port::is_finished(const std::string & port_name){
	if(port_name.length() == 0) {
		// NOTE: id is the `default` cache_machine name
		auto id = std::to_string(kernel_->get_id());
		auto it = cache_machines_.find(id);
		return it->second->is_finished();
	}
	auto it = cache_machines_.find(port_name);
	return it->second->is_finished();
}

uint64_t port::total_bytes_added(){
	uint64_t total = 0;
	for (auto cache : cache_machines_){
		total += cache.second->get_num_bytes_added();
	}
	return total;
}

uint64_t port::total_rows_added(){
	uint64_t total = 0;
	for (auto cache : cache_machines_){
		total += cache.second->get_num_rows_added();
	}
	return total;
}

uint64_t port::get_num_rows_added(const std::string & port_name){
	if(port_name.length() == 0) {
		// NOTE: id is the `default` cache_machine name
		auto id = std::to_string(kernel_->get_id());
		auto it = cache_machines_.find(id);
		return it->second->get_num_rows_added();
	}
	auto it = cache_machines_.find(port_name);
	return it->second->get_num_rows_added();
}


}  // end namespace cache
}  // end namespace ral