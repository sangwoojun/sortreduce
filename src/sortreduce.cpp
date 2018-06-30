#include "sortreduce.h"


/**
TODO
Files in mq_temp_files may have writes inflight...
mpp_managed_buffers can be used as 'managed' 'Block's
consolidate arguments to blocksorter/blocksorterthread into one config*
**/


template <class K, class V>
SortReduce<K,V>::SortReduce(SortReduceTypes::Config<K,V> *config) {
	this->m_config = config;
	if ( config->update == NULL ) {
		fprintf(stderr, "ERROR: Update function not supplied\n" );
		return;
	}
	mq_temp_files = new SortReduceUtils::MutexedQueue<SortReduceTypes::File>();

	mp_block_sorter = new BlockSorter<K,V>(config, mq_temp_files, config->temporary_directory, config->buffer_size, config->buffer_count, config->maximum_threads/2); //FIXME thread count

	manager_thread = std::thread(&SortReduce<K,V>::ManagerThread, this);
}

template <class K, class V>
SortReduce<K,V>::~SortReduce() {
	delete mq_temp_files;
	delete mp_block_sorter;
}

template <class K, class V>
bool 
SortReduce<K,V>::PutBlock(void* buffer, size_t bytes) {
	size_t bytes_inflight = mp_block_sorter->BytesInFlight();
	if ( bytes_inflight + bytes < m_config->max_bytes_inflight ) {
		mp_block_sorter->PutBlock(buffer,bytes);
		return true;
	} else return false;
}

template <class K, class V>
size_t
SortReduce<K,V>::GetBlock(void* buffer) {
	return 0;
}


template <class K, class V>
SortReduceTypes::Status 
SortReduce<K,V>::CheckStatus() {
	SortReduceTypes::Status status;
	return status;
}


template <class K, class V>
bool
SortReduce<K,V>::Update(K key, V val) {

	//catches cold updates with no cur_update_block and when cur_update_block is full
	if ( m_cur_update_offset + sizeof(K) + sizeof(V) > m_cur_update_block.bytes ) {
		if ( m_cur_update_block.buffer != NULL ) { // or managed_idx < 0 or bytes = 0
			m_cur_update_block.valid_bytes  = m_cur_update_offset;
			mp_block_sorter->PutManagedBlock(m_cur_update_block);
			m_cur_update_block.buffer = NULL;
		}
		m_cur_update_block = mp_block_sorter->GetFreeManagedBlock();
		if ( m_cur_update_block.buffer == NULL ) return false;
	}

	K* cur_key_ptr = (K*)((uint8_t*)m_cur_update_block.buffer + m_cur_update_offset);
	*cur_key_ptr = key;
	V* cur_val_ptr = (V*)((uint8_t*)m_cur_update_block.buffer + m_cur_update_offset + sizeof(K));
	*cur_val_ptr = val;

	m_cur_update_offset += sizeof(K)+sizeof(V);

	return true;
}

template <class K, class V>
void
SortReduce<K,V>::ManagerThread() {
	//printf( "maximum threads: %d\n", config->maximum_threads );

	while (true) {
		sleep(1);
		mp_block_sorter->CheckSpawnThreads();

		// if GetBlock() returns more than ...say 16, spawn a merge-reducer
	}
}


template class SortReduce<uint32_t,uint32_t>;
template class SortReduce<uint32_t,uint64_t>;
template class SortReduce<uint64_t,uint32_t>;
template class SortReduce<uint64_t,uint64_t>;
