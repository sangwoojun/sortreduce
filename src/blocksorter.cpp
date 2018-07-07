#include "blocksorter.h"

/**
TODO
Remove mp_temp_file_manager
Merge PutBlock, PutManagedBlock?
Use valid_bytes to determine how to sort <- come non-local fixing required 


**/

template <class K, class V>
BlockSorter<K,V>::BlockSorter(SortReduceTypes::Config<K,V>* config, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, std::string temp_path, size_t buffer_size, int buffer_count, int max_threads) {

	AlignedBufferManager* buffer_manager = AlignedBufferManager::GetInstance();
	buffer_manager->Init(1024*1024*4, 128); //FIXME fixed to 4 MB

	this->m_buffer_queue_in = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();
	this->m_buffer_queue_out = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();

	this->mp_config = config;

	this->m_maximum_threads = max_threads;

	clock_gettime(CLOCK_REALTIME, &this->m_last_thread_check_time);

	// How many items in the queue before spawning another thread?
	this->m_in_queue_spawn_limit_blocks = 4;
	this->m_out_queue_spawn_limit_blocks = 4;
	//this->mp_temp_file_manager = new TempFileManager(temp_path);
	this->mq_temp_files = temp_files;

	this->m_buffer_size = buffer_size;
	this->m_buffer_count = buffer_count;
	mpp_managed_buffers = (void**)malloc(sizeof(void**)*buffer_count);
	for ( int i = 0; i < buffer_count; i++ ) {
		//mpp_managed_buffers[i] = aligned_alloc(512, buffer_size);

		// unallocated, or free'd buffers MUST be marked NULL
		mpp_managed_buffers[i] = NULL;
		// so that first pop gets 0, not buffer_count-1
		ms_free_managed_buffers.push(buffer_count-1-i);
	}
}

template <class K, class V>
BlockSorter<K,V>::~BlockSorter() {
	for ( int i = 0; i < m_buffer_count; i++ ) {
		if ( mpp_managed_buffers[i] != NULL ) free(mpp_managed_buffers[i]);
	}
	free (mpp_managed_buffers);

}

template <class K, class V>
void 
BlockSorter<K,V>::PutBlock(void* buffer, size_t bytes, bool last) {
	SortReduceTypes::Block b;
	b.buffer = buffer; 
	b.bytes = bytes;
	b.managed = false;
	b.valid = true;
	b.last = last;
	m_buffer_queue_in->push(b);

	m_status.bytes_inflight += bytes;
}

template <class K, class V>
SortReduceTypes::Block
BlockSorter<K,V>::GetBlock() {
	return m_buffer_queue_out->get();
}

template <class K, class V>
SortReduceTypes::Block
BlockSorter<K,V>::GetFreeManagedBlock() {
	SortReduceTypes::Block b;

	if ( ms_free_managed_buffers.empty() ) {
		b.buffer = NULL;
		b.valid = false;
	} else {
		int idx = ms_free_managed_buffers.top();
		ms_free_managed_buffers.pop();
		if ( mpp_managed_buffers[idx] == NULL ) {
			mpp_managed_buffers[idx] = aligned_alloc(512, m_buffer_size);
		}
		b.buffer = mpp_managed_buffers[idx];
		b.bytes = m_buffer_size;
		b.managed = true;
		b.valid = true;
	}

	return b;
}

template <class K, class V>
void 
BlockSorter<K,V>::PutManagedBlock(SortReduceTypes::Block block) {
	if ( block.managed == false || block.managed_idx < 0 ) {
		return;
	}
	
	m_buffer_queue_in->push(block);
}

/**
	Should be called by manager thread
**/
template <class K, class V>
void
BlockSorter<K,V>::CheckSpawnThreads() {
	//TODO move this to a separate function?
	//mp_temp_file_manager->CheckDone();

	timespec cur_time;
	clock_gettime(CLOCK_REALTIME, &cur_time);
	double diff_secs = SortReduceUtils::TimespecDiffSec(m_last_thread_check_time, cur_time);

	if ( diff_secs >= 0.2 ) {
		m_last_thread_check_time = cur_time;

		// If no threads exist, and input data exists, spawn thread
		if ( m_buffer_queue_in->size() > 0 && mv_sorter_threads.size() == 0 ) {
			// Increase thread count
			if ( mv_sorter_threads.size() < m_maximum_threads ) {
				BlockSorterThread<K,V>* new_thread = new BlockSorterThread<K,V>(this->mp_config, this->m_buffer_queue_in, this->m_buffer_queue_out, this->mq_temp_files, &this->m_status);
				mv_sorter_threads.push_back(new_thread);
			}
		}
		else if ( m_buffer_queue_in->size() > m_in_queue_spawn_limit_blocks
			/* && mp_temp_file_manager->CountFreeBuffers() < (int)m_out_queue_spawn_limit_blocks */) {

			// Increase thread count
			if ( mv_sorter_threads.size() < m_maximum_threads ) {
				BlockSorterThread<K,V>* new_thread = new BlockSorterThread<K,V>(this->mp_config, this->m_buffer_queue_in, this->m_buffer_queue_out, this->mq_temp_files, &this->m_status);
				mv_sorter_threads.push_back(new_thread);
			}
		}
		else if ( m_buffer_queue_in->size() == 0 && mv_sorter_threads.size() > 0 ) {
			// Decrease thread count
			mv_sorter_threads[mv_sorter_threads.size()-1]->Exit();
			mv_sorter_threads.pop_back();
		}
		else if ( m_buffer_queue_in->size() < m_in_queue_spawn_limit_blocks ) {

			// Decrease thread count
			if ( mv_sorter_threads.size() > 1 ) {
				mv_sorter_threads[mv_sorter_threads.size()-1]->Exit();
				mv_sorter_threads.pop_back();
			}
		}
	}
}

template <class K, class V>
size_t
BlockSorter<K,V>::BytesInFlight() {
	return /*mp_temp_file_manager->BytesInFlight() +*/ m_status.bytes_inflight;
}

template <class K, class V>
BlockSorterThread<K,V>::BlockSorterThread(SortReduceTypes::Config<K,V>* config, SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* buffer_queue_in, SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* buffer_queue_out, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, SortReduceTypes::ComponentStatus* status) {
	m_exit = false;
	m_buffer_queue_in = buffer_queue_in;
	m_buffer_queue_out = buffer_queue_out;
	mp_config = config;

	m_thread = std::thread(&BlockSorterThread<K,V>::SorterThread, this);
	mq_temp_files = temp_files;

	mp_status = status;
}
	
template <class K, class V>
void 
BlockSorterThread<K,V>::SortKV(void* buffer, size_t bytes) {
	KvPair* tbuffer = (KvPair*)buffer;
	size_t count = bytes/sizeof(KvPair);
	std::sort(tbuffer, tbuffer+count, BlockSorterThread<K,V>::CompareKV);

}

template <class K, class V>
bool 
BlockSorterThread<K,V>::CompareKV(KvPair a, KvPair b) {
	return (a.key < b.key);
}

template <class K, class V>
void
BlockSorterThread<K,V>::SorterThread() {
	printf( "BlockSorterThread created!\n" );
	while ( !m_exit ) {
		SortReduceTypes::Block block;
		block = m_buffer_queue_in->get();
		size_t bytes = block.bytes;
		void* buffer = block.buffer;

		if ( bytes <= 0 ) continue;

		BlockSorterThread<K,V>::SortKV(buffer, bytes);
		//size_t reduced_bytes = SortReduceReducer::ReduceInBuffer<K,V>(mp_config->update, buffer, bytes);
		size_t reduced_bytes = bytes;

		/*
		uint32_t last_key = 0;
		for ( int i = 0; i < 1024*1024; i++ ) {
			uint32_t cur = ((uint32_t*)buffer)[i*2];
			if ( cur < last_key ) {
				printf( "Wrong order after sort %x %x\n", last_key, cur );
			}
			last_key = cur;
		}
		*/

		// non-managed buffers are freed after writing to file
		if ( !block.managed ) {
			mp_status->bytes_inflight -= bytes;
		}
		block.valid_bytes = reduced_bytes;
		m_buffer_queue_out->push(block);

		//SortReduceTypes::File new_file = mp_file_manager->CreateFile(buffer, bytes,reduced_bytes, !managed);
		//mq_temp_files->push(new_file);

	}
}

template <class K, class V>
void
BlockSorterThread<K,V>::Exit() {
	//Mark thread to exit after current work
	m_exit = true;
	//Detach thread so resoures will be released after
	m_thread.detach();
}

template class BlockSorter<uint32_t,uint32_t>;
template class BlockSorter<uint32_t,uint64_t>;
template class BlockSorter<uint64_t,uint32_t>;
template class BlockSorter<uint64_t,uint64_t>;

template class BlockSorterThread<uint32_t,uint32_t>;
template class BlockSorterThread<uint32_t,uint64_t>;
template class BlockSorterThread<uint64_t,uint32_t>;
template class BlockSorterThread<uint64_t,uint64_t>;
