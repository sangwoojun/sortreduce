#include "blocksorter.h"

BlockSorter::BlockSorter(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, std::string temp_path, int max_threads) {

	this->m_buffer_queue_in = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();

	this->m_key_type = key_type;
	this->m_val_type = val_type;
	this->m_maximum_threads = max_threads;

	clock_gettime(CLOCK_REALTIME, &this->m_last_thread_check_time);

	// How many items in the queue before spawning another thread?
	this->m_in_queue_spawn_limit_blocks = 4;
	this->m_out_queue_spawn_limit_blocks = 4;
	this->mp_temp_file_manager = new TempFileManager(temp_path);
	this->mq_temp_files = temp_files;
}

void 
BlockSorter::PutBlock(void* buffer, size_t bytes) {
	SortReduceTypes::Block b;
	b.buffer = buffer; b.bytes = bytes;
	m_buffer_queue_in->push(b);
}

/**
	Should be called by manager thread
**/
void
BlockSorter::CheckSpawnThreads() {
	timespec cur_time;
	clock_gettime(CLOCK_REALTIME, &cur_time);
	double diff_secs = SortReduceUtils::TimespecDiffSec(m_last_thread_check_time, cur_time);

	if ( diff_secs >= 0.2 ) {
		m_last_thread_check_time = cur_time;

		// If no threads exist, and input data exists, spawn thread
		if ( m_buffer_queue_in->size() > 0 && mv_sorter_threads.size() == 0 ) {
			// Increase thread count
			if ( mv_sorter_threads.size() < m_maximum_threads ) {
				BlockSorterThread* new_thread = new BlockSorterThread(this->m_key_type, this->m_val_type, this->m_buffer_queue_in, this->mp_temp_file_manager, this->mq_temp_files);
				mv_sorter_threads.push_back(new_thread);
			}
		}
		else if ( m_buffer_queue_in->size() > m_in_queue_spawn_limit_blocks &&
			mp_temp_file_manager->CountFreeBuffers() < (int)m_out_queue_spawn_limit_blocks ) {

			// Increase thread count
			if ( mv_sorter_threads.size() < m_maximum_threads ) {
				BlockSorterThread* new_thread = new BlockSorterThread(this->m_key_type, this->m_val_type, this->m_buffer_queue_in, this->mp_temp_file_manager, this->mq_temp_files);
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

/*
size_t 
BlockSorter::GetBlock(void* buffer) {
	size_t ret = m_buffer_queue->deq_out(&buffer);
	return ret;
}
*/

BlockSorterThread::BlockSorterThread(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* buffer_queue, TempFileManager* file_manager, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files) {
	m_exit = false;
	m_buffer_queue_in = buffer_queue;
	m_key_type = key_type;
	m_val_type = val_type;

	m_thread = std::thread(&BlockSorterThread::SorterThread, this);
	mp_file_manager = file_manager;
	mq_temp_files = temp_files;
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
		void* buffer = NULL;
		SortReduceTypes::Block block;
		block = m_buffer_queue_in->get();
		//size_t bytes = m_buffer_queue->deq_in(&buffer);
		size_t bytes = block.bytes;
		buffer = block.buffer;

		if ( bytes <= 0 ) continue;

		if ( m_key_type == SortReduceTypes::KEY_BINARY32 && m_val_type == SortReduceTypes::VAL_BINARY32 ) {
			BlockSorterThread::SortKV<tK32_V32>(buffer, bytes);
		}
		else if ( m_key_type == SortReduceTypes::KEY_BINARY32 && m_val_type == SortReduceTypes::VAL_BINARY64 ) {
			BlockSorterThread::SortKV<tK32_V64>(buffer, bytes);
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

		//TODO push fd to global work queue
		SortReduceTypes::File new_file = mp_file_manager->CreateFile(buffer, bytes);
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
