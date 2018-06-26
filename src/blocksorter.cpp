#include "blocksorter.h"

BlockSorter::BlockSorter(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, int max_threads) {

	this->m_buffer_queue = new BufferQueueInOut();

	this->m_key_type = key_type;
	this->m_val_type = val_type;
	this->m_maximum_threads = max_threads;

	clock_gettime(CLOCK_REALTIME, &this->m_last_thread_check_time);

	// How many items in the queue before spawning another thread?
	this->m_in_queue_spawn_limit_blocks = 4;
	this->m_out_queue_spawn_limit_blocks = 4;
}

void 
BlockSorter::PutBlock(void* buffer, size_t bytes) {
	m_buffer_queue->enq_in(buffer, bytes);
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
		if ( m_buffer_queue->in_count() > 0 && mv_sorter_threads.size() == 0 ) {
			// Increase thread count
			if ( mv_sorter_threads.size() < m_maximum_threads ) {
				BlockSorterThread* new_thread = new BlockSorterThread(this->m_key_type, this->m_val_type, this->m_buffer_queue);
				mv_sorter_threads.push_back(new_thread);
			}
		}
		else if ( m_buffer_queue->in_count() > m_in_queue_spawn_limit_blocks &&
			m_buffer_queue->out_count() < m_out_queue_spawn_limit_blocks ) {

			// Increase thread count
			if ( mv_sorter_threads.size() < m_maximum_threads ) {
				BlockSorterThread* new_thread = new BlockSorterThread(this->m_key_type, this->m_val_type, this->m_buffer_queue);
				mv_sorter_threads.push_back(new_thread);
			}
		}
		else if ( m_buffer_queue->in_count() < m_in_queue_spawn_limit_blocks ) {

			// Decrease thread count
			if ( mv_sorter_threads.size() > 0 ) {
				mv_sorter_threads[mv_sorter_threads.size()-1]->Exit();
				mv_sorter_threads.pop_back();
			}
		}
	}
}

size_t 
BlockSorter::GetBlock(void* buffer) {
	size_t ret = m_buffer_queue->deq_out(&buffer);
	return ret;
}

BlockSorterThread::BlockSorterThread(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, BufferQueueInOut* buffer_queue) {
	m_exit = false;
	m_buffer_queue = buffer_queue;
	m_key_type = key_type;
	m_val_type = val_type;

	m_thread = std::thread(&BlockSorterThread::SorterThread, this);
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
		size_t bytes = m_buffer_queue->deq_in(&buffer);

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

		m_buffer_queue->enq_out(buffer, bytes);
	}
}

void
BlockSorterThread::Exit() {
	//Mark thread to exit after current work
	m_exit = true;
	//Detach thread so resoures will be released after
	m_thread.detach();
}
