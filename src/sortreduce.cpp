#include "sortreduce.h"


/**
TODO
Files in mq_temp_files may have writes inflight...
**/


SortReduce::SortReduce(SortReduce::Config *config) {
	this->config = config;
	switch (config->val_type) {
		case SortReduceTypes::VAL_BINARY32: {
			if ( config->update32 == NULL ) {
				fprintf(stderr, "ERROR: Update function for VAL_BINARY32 not supplied\n" );
				return;
			}
			break;
		}
		case SortReduceTypes::VAL_BINARY64: {
			if ( config->update64 == NULL ) {
				fprintf(stderr, "ERROR: Update function for VAL_BINARY64 not supplied\n" );
				return;
			}
			break;
		}
	}

	mq_temp_files = new SortReduceUtils::MutexedQueue<SortReduceTypes::File>();

	mp_block_sorter = new BlockSorter(config->key_type, config->val_type, mq_temp_files, config->temporary_directory, config->maximum_threads/2); //FIXME thread count

	manager_thread = std::thread(&SortReduce::ManagerThread, this);
}

bool 
SortReduce::PutBlock(void* buffer, size_t bytes) {
	mp_block_sorter->PutBlock(buffer,bytes);
	return false;
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

	while (true) {
		mp_block_sorter->CheckSpawnThreads();
	}
}


SortReduce::Config::Config(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, int file_input, int file_output, std::string temporary_directory) {
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
