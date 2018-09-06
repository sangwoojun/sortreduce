#include "types.h"

template <class K, class V>
SortReduceTypes::Config<K,V>::Config(std::string temporary_directory, std::string output_filename, int max_threads) {
	this->temporary_directory = temporary_directory;
	this->output_filename = output_filename;

	if ( max_threads < 0 ) {
		this->maximum_threads = std::thread::hardware_concurrency();
	} else {
		this->maximum_threads = max_threads;
	}

	this->update = NULL;
	
	this->buffer_size = 0;
	this->buffer_count = 0;
	this->max_bytes_inflight = 0;
}

template <class K, class V>
void 
SortReduceTypes::Config<K,V>::SetManagedBufferSize(size_t buffer_size, int buffer_count) {
	this->buffer_size = buffer_size;
	this->buffer_count = buffer_count;
}

template <class K, class V>
void 
SortReduceTypes::Config<K,V>::SetMaxBytesInFlight(size_t buffer_size) {
	this->max_bytes_inflight = buffer_size;
}

template <class K, class V>
void
SortReduceTypes::Config<K,V>::SetUpdateFunction(V (*update)(V,V)) {
	this->update = update;
}

/*
template <class K, class V>
void
SortReduceTypes::Config<K,V>::SetUpdateFunction(uint64_t (*update64)(uint64_t,uint64_t)) {
	this->update64 = update64;
}
*/

SortReduceTypes::Status::Status() {
	this->done_input = false;
	this->done_inmem = false;
	this->done_external = false;
	this->done_file = NULL;
}

TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceTypes::Config)
