#include "types.h"

template <class K, class V>
SortReduceTypes::Config<K,V>::Config(int file_input, int file_output, std::string temporary_directory) {
	this->file_input = file_input;
	this->file_output = file_output;
	this->temporary_directory = temporary_directory;

	this->maximum_threads = 4;
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
	this->done = false;
}

template class SortReduceTypes::Config<uint32_t,uint32_t>;
