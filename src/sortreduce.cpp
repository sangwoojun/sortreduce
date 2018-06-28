#include "sortreduce.h"


/**
TODO
Files in mq_temp_files may have writes inflight...
mpp_managed_buffers can be used as 'managed' 'Block's
consolidate arguments to blocksorter/blocksorterthread into one config*
**/


SortReduce::SortReduce(SortReduceTypes::Config *config) {
	this->m_config = config;
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

	mp_block_sorter = new BlockSorter(config, config->key_type, config->val_type, mq_temp_files, config->temporary_directory, config->buffer_size, config->buffer_count, config->maximum_threads/2); //FIXME thread count

	manager_thread = std::thread(&SortReduce::ManagerThread, this);
}

SortReduce::~SortReduce() {
	delete mq_temp_files;
	delete mp_block_sorter;
}

bool 
SortReduce::PutBlock(void* buffer, size_t bytes) {
	size_t bytes_inflight = mp_block_sorter->BytesInFlight();
	if ( bytes_inflight + bytes < m_config->max_bytes_inflight ) {
		mp_block_sorter->PutBlock(buffer,bytes);
		return true;
	} else return false;
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
		sleep(1);
		mp_block_sorter->CheckSpawnThreads();
	}
}


SortReduce::Status::Status() {
	this->done = false;
}
