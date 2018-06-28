#include "blocksorter.h"

BlockSorter::BlockSorter(SortReduceTypes::Config* config, SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, std::string temp_path, size_t buffer_size, int buffer_count, int max_threads) {

	this->m_buffer_queue_in = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();

	this->mp_config = config;

	this->m_key_type = key_type;
	this->m_val_type = val_type;
	this->m_maximum_threads = max_threads;

	clock_gettime(CLOCK_REALTIME, &this->m_last_thread_check_time);

	// How many items in the queue before spawning another thread?
	this->m_in_queue_spawn_limit_blocks = 4;
	this->m_out_queue_spawn_limit_blocks = 4;
	this->mp_temp_file_manager = new TempFileManager(temp_path);
	this->mq_temp_files = temp_files;

	this->m_buffer_size = buffer_size;
	this->m_buffer_count = buffer_count;
	mpp_managed_buffers = (void**)malloc(sizeof(void**)*buffer_count);
	for ( int i = 0; i < buffer_count; i++ ) {
		//mpp_managed_buffers[i] = aligned_alloc(512, buffer_size);

		// unallocated, or free'd buffers MUST be marked NULL
		mpp_managed_buffers[i] = NULL;
		mq_free_managed_buffers.push(i);
	}
}

BlockSorter::~BlockSorter() {
	for ( int i = 0; i < m_buffer_count; i++ ) {
		if ( mpp_managed_buffers[i] != NULL ) free(mpp_managed_buffers[i]);
	}
	free (mpp_managed_buffers);

}

void 
BlockSorter::PutBlock(void* buffer, size_t bytes) {
	SortReduceTypes::Block b;
	b.buffer = buffer; 
	b.bytes = bytes;
	b.managed = false;
	m_buffer_queue_in->push(b);

	m_status.bytes_inflight += bytes;
}

/**
	Should be called by manager thread
**/
void
BlockSorter::CheckSpawnThreads() {
	//TODO move this to a separate function?
	mp_temp_file_manager->CheckDone();

	timespec cur_time;
	clock_gettime(CLOCK_REALTIME, &cur_time);
	double diff_secs = SortReduceUtils::TimespecDiffSec(m_last_thread_check_time, cur_time);

	if ( diff_secs >= 0.2 ) {
		m_last_thread_check_time = cur_time;

		// If no threads exist, and input data exists, spawn thread
		if ( m_buffer_queue_in->size() > 0 && mv_sorter_threads.size() == 0 ) {
			// Increase thread count
			if ( mv_sorter_threads.size() < m_maximum_threads ) {
				BlockSorterThread* new_thread = new BlockSorterThread(this->mp_config, this->m_key_type, this->m_val_type, this->m_buffer_queue_in, this->mp_temp_file_manager, this->mq_temp_files, &this->m_status);
				mv_sorter_threads.push_back(new_thread);
			}
		}
		else if ( m_buffer_queue_in->size() > m_in_queue_spawn_limit_blocks &&
			mp_temp_file_manager->CountFreeBuffers() < (int)m_out_queue_spawn_limit_blocks ) {

			// Increase thread count
			if ( mv_sorter_threads.size() < m_maximum_threads ) {
				BlockSorterThread* new_thread = new BlockSorterThread(this->mp_config, this->m_key_type, this->m_val_type, this->m_buffer_queue_in, this->mp_temp_file_manager, this->mq_temp_files, &this->m_status);
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

size_t
BlockSorter::BytesInFlight() {
	return mp_temp_file_manager->BytesInFlight() + m_status.bytes_inflight;
}

/*
size_t 
BlockSorter::GetBlock(void* buffer) {
	size_t ret = m_buffer_queue->deq_out(&buffer);
	return ret;
}
*/

BlockSorterThread::BlockSorterThread(SortReduceTypes::Config* config, SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* buffer_queue, TempFileManager* file_manager, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, SortReduceTypes::ComponentStatus* status) {
	m_exit = false;
	m_buffer_queue_in = buffer_queue;
	m_key_type = key_type;
	m_val_type = val_type;
	mp_config = config;

	m_thread = std::thread(&BlockSorterThread::SorterThread, this);
	mp_file_manager = file_manager;
	mq_temp_files = temp_files;

	mp_status = status;
}
	
template <class tKV>
void 
BlockSorterThread::SortKV(void* buffer, size_t bytes) {
	tKV* tbuffer = (tKV*)buffer;
	size_t count = bytes/sizeof(tKV);
	std::sort(tbuffer, tbuffer+count, BlockSorterThread::CompareKV<tKV>);

}
// Explicit instantitation to go across library boundaries
template void BlockSorterThread::SortKV<BlockSorterThread::tK32_V32>(void* buffer, size_t bytes);
template void BlockSorterThread::SortKV<BlockSorterThread::tK32_V64>(void* buffer, size_t bytes);
template void BlockSorterThread::SortKV<BlockSorterThread::tK64_V32>(void* buffer, size_t bytes);
template void BlockSorterThread::SortKV<BlockSorterThread::tK64_V64>(void* buffer, size_t bytes);

template <class tKV>
bool 
BlockSorterThread::CompareKV(tKV a, tKV b) {
	return (a.key < b.key);
}

void
BlockSorterThread::SorterThread() {
	printf( "BlockSorterThread created!\n" );
	while ( !m_exit ) {
		SortReduceTypes::Block block;
		block = m_buffer_queue_in->get();
		size_t bytes = block.bytes;
		void* buffer = block.buffer;
		bool managed = block.managed;

		if ( bytes <= 0 ) continue;

		size_t reduced_bytes = bytes;

		if ( m_key_type == SortReduceTypes::KEY_BINARY32 && m_val_type == SortReduceTypes::VAL_BINARY32 ) {
			BlockSorterThread::SortKV<tK32_V32>(buffer, bytes);
			reduced_bytes = SortReduceReducer::ReduceInBuffer<uint32_t,uint32_t>(mp_config->update32, buffer, bytes);
		}
		else if ( m_key_type == SortReduceTypes::KEY_BINARY32 && m_val_type == SortReduceTypes::VAL_BINARY64 ) {
			BlockSorterThread::SortKV<tK32_V64>(buffer, bytes);
			//reduced_bytes = SortReduceReducer::ReduceInBuffer<uint32_t,uint64_t>(mp_config->update32, buffer, bytes);
		}
		else if ( m_key_type == SortReduceTypes::KEY_BINARY64 && m_val_type == SortReduceTypes::VAL_BINARY32 ) {
			BlockSorterThread::SortKV<tK64_V32>(buffer, bytes);
		}
		else if ( m_key_type == SortReduceTypes::KEY_BINARY64 && m_val_type == SortReduceTypes::VAL_BINARY64 ) {
			BlockSorterThread::SortKV<tK64_V64>(buffer, bytes);
		}
		else {
			fprintf(stderr, "BlockSorterThread undefined key value types %s:%d\n", __FILE__, __LINE__ );
		}

/*
		if ( m_val_type == SortReduceTypes::VAL_BINARY32 ) {
			reduced_bytes = SortReduceReducer::ReduceInBuffer32(mp_config->update32, m_key_type, buffer, bytes);
		} else if ( m_val_type == SortReduceTypes::VAL_BINARY64 ) {
			reduced_bytes = SortReduceReducer::ReduceInBuffer64(mp_config->update64, m_key_type, buffer, bytes);
		} else {
			fprintf(stderr, "BlockSorterThread undefined key value types %s:%d\n", __FILE__, __LINE__ );
		}
*/
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
		SortReduceTypes::File new_file = mp_file_manager->CreateFile(buffer, bytes,reduced_bytes, !managed);
		mp_status->bytes_inflight -= bytes;

		mq_temp_files->push(new_file);
	}
}

void
BlockSorterThread::Exit() {
	//Mark thread to exit after current work
	m_exit = true;
	//Detach thread so resoures will be released after
	m_thread.detach();
}
