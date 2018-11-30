#include "blocksorter.h"

/**
TODO
Remove mp_temp_file_manager
Merge PutBlock, PutManagedBlock?
Use valid_bytes to determine how to sort <- come non-local fixing required 


**/

template <class K, class V>
BlockSorter<K,V>::BlockSorter(SortReduceTypes::Config<K,V>* config, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, std::string temp_path, int max_threads) {


	this->m_blocks_inflight = 0;
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
	
	BlockSorterThread<K,V>* new_thread = new BlockSorterThread<K,V>(this->mp_config, this->m_buffer_queue_in, this->m_buffer_queue_out, this->mq_temp_files, &this->m_status, mv_sorter_threads.size());
	mv_sorter_threads.push_back(new_thread);

/*
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
*/
}

template <class K, class V>
BlockSorter<K,V>::~BlockSorter() {
	while ( this->GetThreadCount() > 0 ) {
		this->KillThread();
	}
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

	m_blocks_inflight ++;

	m_status.bytes_inflight += bytes;
}

template <class K, class V>
SortReduceTypes::Block
BlockSorter<K,V>::GetOutBlock() {
	m_blocks_inflight --;

	return m_buffer_queue_out->get();
}

template <class K, class V>
SortReduceTypes::Block
BlockSorter<K,V>::GetFreeManagedBlock() {
	AlignedBufferManager* managed_buffers = AlignedBufferManager::GetInstance(0);

	return managed_buffers->GetBuffer();

/*
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
		b.managed_idx = idx;
		b.valid = true;
	}

	return b;
*/
}

template <class K, class V>
void 
BlockSorter<K,V>::PutManagedBlock(SortReduceTypes::Block block) {
	if ( block.managed == false || block.managed_idx < 0 ) {
		fprintf(stderr, "PutManagedBlock with non-managed block!\n" );
		return;
	}
	
	m_buffer_queue_in->push(block);

	m_blocks_inflight ++;
}
template <class K, class V>
void
BlockSorter<K,V>::SpawnThread() {
	BlockSorterThread<K,V>* new_thread = new BlockSorterThread<K,V>(this->mp_config, this->m_buffer_queue_in, this->m_buffer_queue_out, this->mq_temp_files, &this->m_status, mv_sorter_threads.size());
	mv_sorter_threads.push_back(new_thread);

	//printf( "BlockSorter thread spawned! %lu\n", mv_sorter_threads.size() ); fflush(stdout);
}

template <class K, class V>
void
BlockSorter<K,V>::KillThread() {
	if ( mv_sorter_threads.size() > 0 ) {
		mv_sorter_threads[mv_sorter_threads.size()-1]->Exit();
		mv_sorter_threads.pop_back();
	}
	
	//printf( "BlockSorter thread killed! %lu\n", mv_sorter_threads.size() ); fflush(stdout);
}

/**
	Should be called by manager thread
**/
template <class K, class V>
void
BlockSorter<K,V>::CheckSpawnThreads() {
/*
	timespec cur_time;
	clock_gettime(CLOCK_REALTIME, &cur_time);
	double diff_secs = SortReduceUtils::TimespecDiffSec(m_last_thread_check_time, cur_time);
*/


/*
	if ( diff_secs >= 1 ) {
		m_last_thread_check_time = cur_time;
	
		//printf( "Block sorter threads: %lu\n", mv_sorter_threads.size() );
	
		while ( mv_sorter_threads.size() < 8 ) {
			BlockSorterThread<K,V>* new_thread = new BlockSorterThread<K,V>(this->mp_config, this->m_buffer_queue_in, this->m_buffer_queue_out, this->mq_temp_files, &this->m_status, mv_sorter_threads.size());
			mv_sorter_threads.push_back(new_thread);
		}
	}
*/

/*
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
			 //&& mp_temp_file_manager->CountFreeBuffers() < (int)m_out_queue_spawn_limit_blocks 
			) {

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
*/

}

template <class K, class V>
size_t
BlockSorter<K,V>::BytesInFlight() {
	return /*mp_temp_file_manager->BytesInFlight() +*/ m_status.bytes_inflight;
}

template <class K, class V>
BlockSorterThread<K,V>::BlockSorterThread(SortReduceTypes::Config<K,V>* config, SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* buffer_queue_in, SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* buffer_queue_out, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, SortReduceTypes::ComponentStatus* status, int thread_idx) {
	m_exit = false;
	m_thread_idx = thread_idx;

	m_buffer_queue_in = buffer_queue_in;
	m_buffer_queue_out = buffer_queue_out;
	mp_config = config;

	m_thread = std::thread(&BlockSorterThread<K,V>::SorterThread, this);
	mq_temp_files = temp_files;

	mp_status = status;
}
	
template <class K, class V>
size_t 
BlockSorterThread<K,V>::SortKV(void* buffer, size_t bytes, V (*update)(V,V)) {
	KvPair* tbuffer = (KvPair*)buffer;
	size_t count = bytes/sizeof(KvPair);
	std::sort(tbuffer, tbuffer+count, BlockSorterThread<K,V>::CompareKV);

	size_t out_size = 0;

	if ( update != NULL ) {
		KvPair last_kvp = tbuffer[0];
		for (size_t i = 1; i < count; i++ ) {
			KvPair c = tbuffer[i];
			if ( last_kvp.key == c.key ) {
				last_kvp.val = update(last_kvp.val, c.val);
			} else {
				tbuffer[out_size++] = last_kvp;
				last_kvp = c;
			}
		}
		tbuffer[out_size++] = last_kvp;
	} else {
		out_size = count;
	}

	return out_size*sizeof(KvPair);
}

template <class K, class V>
bool 
BlockSorterThread<K,V>::CompareKV(KvPair a, KvPair b) {
	return (a.key < b.key);
}

template <class K, class V>
void
BlockSorterThread<K,V>::SorterThread() {
	//printf( "BlockSorterThread created!\n" ); fflush(stdout);
	while ( !m_exit ) {
		SortReduceTypes::Block block;
		block = m_buffer_queue_in->get();
		size_t valid_bytes = block.valid_bytes;
		void* buffer = block.buffer;

		if ( valid_bytes <= 0 ) {
			usleep(100);
			continue;
		}

		std::chrono::high_resolution_clock::time_point start, end;
		start = std::chrono::high_resolution_clock::now();

		//printf( "Begin sort block of size %ld\n", valid_bytes );
		size_t new_bytes = BlockSorterThread<K,V>::SortKV(buffer, valid_bytes, mp_config->update);
		block.valid_bytes = new_bytes;
		//printf( "Done sort block of size %ld -- \n", valid_bytes  );
		//size_t reduced_bytes = SortReduceReducer::ReduceInBuffer<K,V>(mp_config->update, buffer, bytes);
		end = std::chrono::high_resolution_clock::now();
		std::chrono::milliseconds duration_milli;
		duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (end-start);
		//printf( "%d: Block sorted: %lu ms\n", m_thread_idx, duration_milli.count() );
		//fflush(stdout);

/*
		K last_key = 0;
		for ( int i = 0; i < mp_config->buffer_size/sizeof(KvPair); i++ ) {
			KvPair cur = ((KvPair*)buffer)[i];
			if ( cur.key < last_key ) {
				printf( "Wrong order after sort %x %x\n", last_key, cur.key );
			}
			last_key = cur.key;
		}
*/

		// non-managed buffers are freed after writing to file
		if ( !block.managed ) {
			mp_status->bytes_inflight -= valid_bytes;
		}
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

TEMPLATE_EXPLICIT_INSTANTIATION(BlockSorter)
TEMPLATE_EXPLICIT_INSTANTIATION(BlockSorterThread)
