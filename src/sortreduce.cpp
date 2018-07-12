#include "sortreduce.h"


/**
TODO
consolidate arguments to blocksorter/blocksorterthread into one config*
TempFileManager writes need to have information about <512 bit filesize alignment
TempFileManager returns alignedbuffermanager
Check if StreamReducer's destructor is actually working
mp_status->bytes_inflight management is wrong for non-managed blocks

Storage->Storage instance should be limited due to input aligned buffer count
**/


template <class K, class V>
SortReduce<K,V>::SortReduce(SortReduceTypes::Config<K,V> *config) {
	this->m_done_input = false;
	this->m_done_inmem = false;
	this->m_done_external = false;

	this->m_cur_update_block.valid = false;
	this->m_cur_update_block.bytes = 0;
	
	//Buffers for in-memory sorting
	AlignedBufferManager* managed_buffers = AlignedBufferManager::GetInstance(0);
	managed_buffers->Init(config->buffer_size, config->buffer_count);

	//Buffers for file I/O
	AlignedBufferManager* buffer_manager = AlignedBufferManager::GetInstance(1);
	buffer_manager->Init(1024*1024, 1024*4); //FIXME fixed to 1 MB -> 4GB
	

	this->m_config = config;
	if ( config->update == NULL ) {
		fprintf(stderr, "ERROR: Update function not supplied\n" );
		return;
	}
	mq_temp_files = new SortReduceUtils::MutexedQueue<SortReduceTypes::File>();

	mp_block_sorter = new BlockSorter<K,V>(config, mq_temp_files, config->temporary_directory, 8); //FIXME thread count

	manager_thread = std::thread(&SortReduce<K,V>::ManagerThread, this);
}

template <class K, class V>
SortReduce<K,V>::~SortReduce() {
	delete mq_temp_files;
	delete mp_block_sorter;
}

template <class K, class V>
bool 
SortReduce<K,V>::PutBlock(void* buffer, size_t bytes, bool last) {
	size_t bytes_inflight = mp_block_sorter->BytesInFlight();
	if ( bytes_inflight + bytes < m_config->max_bytes_inflight ) {
		mp_block_sorter->PutBlock(buffer,bytes, last);
		return true;
	} else return false;
}

template <class K, class V>
SortReduceTypes::Block
SortReduce<K,V>::GetFreeManagedBlock() {
	return mp_block_sorter->GetFreeManagedBlock();
}

template <class K, class V>
void 
SortReduce<K,V>::PutManagedBlock(SortReduceTypes::Block block) {
	mp_block_sorter->PutManagedBlock(block);
}

template <class K, class V>
SortReduceTypes::Status 
SortReduce<K,V>::CheckStatus() {
	SortReduceTypes::Status status;
	status.done_input = m_done_input;
	status.done_inmem = m_done_inmem;
	status.done_external = m_done_external;
	if ( m_done_external ) {
		if ( m_file_priority_queue.size() != 1 ) {
			fprintf(stderr, "Sort-Reduce is done, but m_file_priority_queue has %lu elements\n", m_file_priority_queue.size() );
		}
		status.done_file = m_file_priority_queue.top();
	}
	status.external_count = mv_stream_mergers_from_storage.size();
	status.internal_count = mv_stream_mergers_from_mem.size();
	status.sorted_count = mp_block_sorter->GetBlockCount();
	status.file_count = m_file_priority_queue.size();
	return status;
}


template <class K, class V>
bool
SortReduce<K,V>::Update(K key, V val, bool last) {
	if ( m_done_input ) return false;

	if ( last ) m_done_input = true;
	
	if ( m_cur_update_block.valid == false ) {
		m_cur_update_block = mp_block_sorter->GetFreeManagedBlock();
		if ( m_cur_update_block.valid == false ) return false;
		m_cur_update_offset = 0;
		//printf( "Got new managed block %s\n", m_cur_update_block.managed?"yes":"no" ); fflush(stdout);
	}
	
	K* cur_key_ptr = (K*)((uint8_t*)m_cur_update_block.buffer + m_cur_update_offset);
	*cur_key_ptr = key;
	V* cur_val_ptr = (V*)((uint8_t*)m_cur_update_block.buffer + m_cur_update_offset + sizeof(K));
	*cur_val_ptr = val;
	m_cur_update_offset += sizeof(K)+sizeof(V);

	//also catches cold updates with no cur_update_block and when cur_update_block is full
	// sizeof(K)+sizeof(V) because KVsize may not be block size aligned
	if ( last || m_cur_update_offset + sizeof(K)+sizeof(V) > m_cur_update_block.bytes ) { 
		if ( m_cur_update_block.valid ) { // or managed_idx < 0 or bytes = 0
			m_cur_update_block.valid_bytes  = m_cur_update_offset;
			if ( last ) m_cur_update_block.last = true;
			
			//printf( "Putting managed block %s\n", m_cur_update_block.managed?"yes":"no" ); fflush(stdout);
			mp_block_sorter->PutManagedBlock(m_cur_update_block);
			m_cur_update_block.valid = false;
			m_cur_update_block.bytes = 0;
		}
	}


	return true;
}

template <class K, class V>
std::tuple<K,V,bool>
SortReduce<K,V>::Next() {
	if ( mp_file_kv_reader == NULL ) {
		return std::make_tuple(0,0,false);
	}

	return mp_file_kv_reader->Next();
}


template<class K, class V>
typename SortReduce<K,V>::IoEndpoint*
SortReduce<K,V>::GetEndpoint() {
	IoEndpoint* ep = new IoEndpoint(this);
	mv_endpoints.push_back(ep);
	return ep;
}

template <class K, class V>
void
SortReduce<K,V>::ManagerThread() {
	//printf( "maximum threads: %d\n", config->maximum_threads );

	while (true) {
		//sleep(1);
		if ( !m_done_inmem ) mp_block_sorter->CheckSpawnThreads();

		// TODO if m_done_input and managed blocks are all back, mark m_done_inmem

		// if GetBlock() returns more than ...say 16, spawn a merge-reducer
		size_t temp_file_count = m_file_priority_queue.size();
		if ( ((m_done_inmem&&temp_file_count>1) || temp_file_count > 16) && mv_stream_mergers_from_storage.size() < 4 ) { //FIXME
			SortReduceReducer::StreamMergeReducer<K,V>* merger;
			if ( m_done_inmem && mv_stream_mergers_from_storage.empty() ) {
				merger = new SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>(m_config->update, m_config->temporary_directory, m_config->output_filename);
			} else {
				// Invisible temporary file
				merger = new SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>(m_config->update, m_config->temporary_directory);
			}
			int to_sort = temp_file_count>128?128:temp_file_count;
			for ( size_t i = 0; i < to_sort; i++ ) {
				SortReduceTypes::File* file = m_file_priority_queue.top();

				m_file_priority_queue.pop();

				merger->PutFile(file);
				//printf( "%d -- %x %x\n", i, *((uint32_t*)block.buffer),((uint32_t*)block.buffer)[1] );
			}
			merger->Start();
			//printf( "Storage->Storage Reducer\n" );fflush(stdout);
			mv_stream_mergers_from_storage.push_back(merger);
		}
		for ( int i = 0; i < mv_stream_mergers_from_storage.size(); i++ ) {
			SortReduceReducer::StreamMergeReducer<K,V>* reducer = mv_stream_mergers_from_storage[i];
			if ( reducer->IsDone() ) {
				SortReduceTypes::File* reduced_file = reducer->GetOutFile();
				m_file_priority_queue.push(reduced_file);
				//printf( "Storage->Storage Pushed sort-reduced file ( size %lu ) -> %lu\n", reduced_file->bytes, m_file_priority_queue.size() ); fflush(stdout);

				mv_stream_mergers_from_storage.erase(mv_stream_mergers_from_storage.begin() + i);
				delete reducer;
			}
		}

		size_t sorted_blocks_cnt = mp_block_sorter->GetBlockCount();
		if ( ((m_done_input && sorted_blocks_cnt>0) || sorted_blocks_cnt >= 16) && mv_stream_mergers_from_mem.size() < 1 ) { //FIXME
			SortReduceReducer::StreamMergeReducer<K,V>* merger = new SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>(m_config->update, m_config->temporary_directory);
			int to_sort = sorted_blocks_cnt;// (sorted_blocks_cnt > 64)?64:sorted_blocks_cnt; //TODO
			
			for ( size_t i = 0; i < to_sort; i++ ) {
				SortReduceTypes::Block block = mp_block_sorter->GetBlock();
				block.last = true;
				merger->PutBlock(block);
				//printf( "%d -- %x %x\n", i, *((uint32_t*)block.buffer),((uint32_t*)block.buffer)[1] );
			}
			merger->Start();


			mv_stream_mergers_from_mem.push_back(merger);
		}

		for ( int i = 0; i < mv_stream_mergers_from_mem.size(); i++ ) {
			SortReduceReducer::StreamMergeReducer<K,V>* reducer = mv_stream_mergers_from_mem[i];
			if ( reducer->IsDone() ) {
				SortReduceTypes::File* reduced_file = reducer->GetOutFile();
				m_file_priority_queue.push(reduced_file);
				//printf( "Pushed sort-reduced file ( size %lu ) -> %lu\n", reduced_file->bytes, m_file_priority_queue.size() ); fflush(stdout);

				//size_t fsize = lseek(reduced_file->fd, 0, SEEK_END);
				//printf( "File size %lx %lx\n", fsize, reduced_file->bytes );
				fflush(stdout);

				mv_stream_mergers_from_mem.erase(mv_stream_mergers_from_mem.begin() + i);
				delete reducer;
			}
		}

		if ( !m_done_inmem && m_done_input && mp_block_sorter->BlocksInFlight() == 0 
			&& mv_stream_mergers_from_mem.empty() ) {
			m_done_inmem = true;
			printf( "Im-memory sort done!\n" ); fflush(stdout);
		}

		if ( !m_done_external && m_done_input && m_done_inmem && 
			m_file_priority_queue.size() == 1 && mv_stream_mergers_from_storage.empty() ) {

			mp_file_kv_reader = new SortReduceUtils::FileKvReader<K,V>(m_file_priority_queue.top(), m_config);

			m_done_external = true;

			printf( "Sort-reduce all done!\n" ); fflush(stdout);
			break;
		}


	}
}

template<class K, class V>
SortReduce<K,V>::IoEndpoint::IoEndpoint(SortReduce<K,V>* sr) {
	mp_sortreduce = sr;
	m_cur_update_block.valid = false;
	m_cur_update_offset = 0;
	m_done = false;
}

template<class K, class V>
bool
SortReduce<K,V>::IoEndpoint::Update(K key, V val) {
	if ( m_done ) return false;

	if ( m_cur_update_block.valid == false ) {
		m_cur_update_block = mp_sortreduce->GetFreeManagedBlock();
		if ( m_cur_update_block.valid == false ) return false;
		m_cur_update_offset = 0;
		//printf( "Got new managed block %s\n", m_cur_update_block.managed?"yes":"no" ); fflush(stdout);
	}
	
	K* cur_key_ptr = (K*)((uint8_t*)m_cur_update_block.buffer + m_cur_update_offset);
	*cur_key_ptr = key;
	V* cur_val_ptr = (V*)((uint8_t*)m_cur_update_block.buffer + m_cur_update_offset + sizeof(K));
	*cur_val_ptr = val;
	m_cur_update_offset += sizeof(K)+sizeof(V);

	//also catches cold updates with no cur_update_block and when cur_update_block is full
	// sizeof(K)+sizeof(V) because KVsize may not be block size aligned
	if ( m_cur_update_offset + sizeof(K)+sizeof(V) > m_cur_update_block.bytes ) { 
		if ( m_cur_update_block.valid ) { // or managed_idx < 0 or bytes = 0
			m_cur_update_block.valid_bytes  = m_cur_update_offset;
			
			//printf( "Putting managed block %s\n", m_cur_update_block.managed?"yes":"no" ); fflush(stdout);
			mp_sortreduce->PutManagedBlock(m_cur_update_block);
			m_cur_update_block.valid = false;
			m_cur_update_block.bytes = 0;
		}
	}


	return true;
}

template<class K, class V>
void
SortReduce<K,V>::IoEndpoint::Finish() {
	if ( m_cur_update_block.valid &&  m_cur_update_offset > 0 ) {
		m_cur_update_block.valid_bytes  = m_cur_update_offset;
		
		//printf( "Putting managed block %s\n", m_cur_update_block.managed?"yes":"no" ); fflush(stdout);
		mp_sortreduce->PutManagedBlock(m_cur_update_block);
		m_cur_update_block.valid = false;
		m_cur_update_block.bytes = 0;
	}
	m_done = true;
}

template<class K, class V>
std::tuple<K,V,bool> 
SortReduce<K,V>::IoEndpoint::Next() {
	std::tuple<K,V,bool> ret;
	//return mp_sortreduce->Next(); //TODO blocks
	return ret; //TODO
}


template class SortReduce<uint32_t,uint32_t>;
template class SortReduce<uint32_t,uint64_t>;
template class SortReduce<uint64_t,uint32_t>;
template class SortReduce<uint64_t,uint64_t>;
