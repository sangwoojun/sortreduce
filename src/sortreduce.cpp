#include "sortreduce.h"


SortReduce::SortReduce(SortReduce::Config *config) {
	this->config = config;
	switch (config->val_type) {
		case SortReduce::VAL_BINARY32: {
			if ( config->update32 == NULL ) {
				fprintf(stderr, "Update function for VAL_BINARY32 not supplied\n" );
				return;
			}
			break;
		}
		case SortReduce::VAL_BINARY64: {
			if ( config->update64 == NULL ) {
				fprintf(stderr, "Update function for VAL_BINARY64 not supplied\n" );
				return;
			}
			break;
		}
	}
	manager_thread = std::thread(&SortReduce::ManagerThread, this);
}

void 
SortReduce::PutBlock(void* buffer, size_t bytes) {
}

size_t
SortReduce::GetBlock(void* buffer) {
	return 0;
}


SortReduce::Status 
SortReduce::CheckStatus() {
	SortReduce::Status status;
	return status;
}

void
SortReduce::ManagerThread() {
	//printf( "maximum threads: %d\n", config->maximum_threads );
}


SortReduce::Config::Config(SortReduce::KeyType key_type, SortReduce::ValType val_type, int file_input, int file_output, std::string temporary_directory) {
	this->key_type = key_type;
	this->val_type = val_type;
	this->file_input = file_input;
	this->file_output = file_output;
	this->temporary_directory = temporary_directory;

	this->maximum_threads = 4;
	this->update32 = NULL;
	this->update64 = NULL;
}

void
SortReduce::Config::SetUpdateFunction(uint32_t (*update32)(uint32_t,uint32_t)) {
	this->update32 = update32;
}
void
SortReduce::Config::SetUpdateFunction(uint64_t (*update64)(uint64_t,uint64_t)) {
	this->update64 = update64;
}

SortReduce::Status::Status() {
	this->done = false;
}
