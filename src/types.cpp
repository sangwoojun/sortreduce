#include "types.h"

SortReduceTypes::Config::Config(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, int file_input, int file_output, std::string temporary_directory) {
	this->key_type = key_type;
	this->val_type = val_type;
	this->file_input = file_input;
	this->file_output = file_output;
	this->temporary_directory = temporary_directory;

	this->maximum_threads = 4;
	this->update32 = NULL;
	this->update64 = NULL;
	
	this->buffer_size = 0;
	this->buffer_count = 0;
	this->max_bytes_inflight = 0;
}
void 
SortReduceTypes::Config::SetManagedBufferSize(size_t buffer_size, int buffer_count) {
	this->buffer_size = buffer_size;
	this->buffer_count = buffer_count;
}
void 
SortReduceTypes::Config::SetMaxBytesInFlight(size_t buffer_size) {
	this->max_bytes_inflight = buffer_size;
}

void
SortReduceTypes::Config::SetUpdateFunction(uint32_t (*update32)(uint32_t,uint32_t)) {
	this->update32 = update32;
}
void
SortReduceTypes::Config::SetUpdateFunction(uint64_t (*update64)(uint64_t,uint64_t)) {
	this->update64 = update64;
}

