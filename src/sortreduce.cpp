#include "sortreduce.h"


/**
TODO
consolidate arguments to blocksorter/blocksorterthread into one config*
TempFileManager writes need to have information about <512 bit filesize alignment
TempFileManager returns alignedbuffermanager
mp_status->bytes_inflight management is wrong for non-managed blocks

Storage->Storage instance should be limited due to input aligned buffer count
**/


template <class K, class V>
SortReduce<K,V>::SortReduce(SortReduceTypes::Config<K,V> *config) {
	this->m_done_input = false;
	this->m_done_input_main = false;
	this->m_done_inmem = false;
	this->m_done_external = false;
	this->m_reduce_phase = false;

	this->m_cur_update_block.valid = false;
	this->m_cur_update_block.bytes = 0;

	printf( "SortReduce Starting...\n" );
	printf( "Remember to set per-thread open file limit to be larger at /etc/security/limits.conf\n" );
	
	//Buffers for in-memory sorting
	AlignedBufferManager* managed_buffers = AlignedBufferManager::GetInstance(0);
	managed_buffers->Init(config->buffer_size, config->buffer_count);

	//Buffers for file I/O
	AlignedBufferManager* buffer_manager_io = AlignedBufferManager::GetInstance(1);
	buffer_manager_io->Init(1024*256, 1024*2*16); //FIXME set to maximum of 8 GBs. Is this enough?

	this->m_config = config;
	if ( config->update == NULL ) {
		fprintf(stderr, "ERROR: Update function not supplied\n" );
		return;
	}
	mq_temp_files = new SortReduceUtils::MutexedQueue<SortReduceTypes::File>();

	m_maximum_threads = config->maximum_threads;
	// 1 for the manager threads, one for the reducer
	int block_sorter_maximum_threads = config->maximum_threads - 2;

	mp_block_sorter = new BlockSorter<K,V>(config, mq_temp_files, config->temporary_directory, block_sorter_maximum_threads);

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
	status.sorted_count = mp_block_sorter->GetOutBlockCount();
	status.file_count = m_file_priority_queue.size();
	return status;
}


template <class K, class V>
inline bool
SortReduce<K,V>::Update(K key, V val) {
	if ( m_done_input ) return false;

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
	if ( m_cur_update_offset + sizeof(K)+sizeof(V) > m_cur_update_block.bytes ) { 
		if ( m_cur_update_block.valid ) { // or managed_idx < 0 or bytes = 0
			m_cur_update_block.valid_bytes  = m_cur_update_offset;
			
			//printf( "Putting managed block %s\n", m_cur_update_block.managed?"yes":"no" ); fflush(stdout);
			mp_block_sorter->PutManagedBlock(m_cur_update_block);
			m_cur_update_block.valid = false;
			m_cur_update_block.bytes = 0;
		}
	}


	return true;
}

template <class K, class V>
void
SortReduce<K,V>::Finish() {
	if ( m_cur_update_block.valid &&  m_cur_update_offset > 0 ) {
		m_cur_update_block.valid_bytes  = m_cur_update_offset;
		
		//printf( "Putting managed block %s\n", m_cur_update_block.managed?"yes":"no" ); fflush(stdout);
		mp_block_sorter->PutManagedBlock(m_cur_update_block);
		m_cur_update_block.valid = false;
		m_cur_update_block.bytes = 0;
	}
	m_done_input_main = true;

	CheckInputDone();
}

template <class K, class V>
inline std::tuple<K,V,bool>
SortReduce<K,V>::Next() {
	if ( mp_file_kv_reader != NULL ) {
		return mp_file_kv_reader->Next();
	}
	
	if ( mp_result_stream_reader != NULL ) {
		bool empty = mp_result_stream_reader->Empty();
		if ( !empty ) {
			SortReduceTypes::KvPair<K,V> kvp = mp_result_stream_reader->GetNext();

			return std::make_tuple(kvp.key,kvp.val,true);
		}
	}

	return std::make_tuple(0,0,false);
}

template <class K, class V>
void
SortReduce<K,V>::CheckInputDone() {
	bool all_done = true;
	m_mutex.lock();

	int endpoint_count = mv_endpoints.size();
	for ( int i = 0; i < endpoint_count; i++ ) {
		if ( mv_endpoints[i]->IsDone() != true ) {
			all_done = false;
			break;
		}
	}

	m_mutex.unlock();

	if ( !m_done_input_main ) all_done = false;

	m_done_input = all_done;
}


template<class K, class V>
typename SortReduce<K,V>::IoEndpoint*
SortReduce<K,V>::GetEndpoint(bool input_only) {
	m_mutex.lock();

	IoEndpoint* ep = new IoEndpoint(this, input_only);
	mv_endpoints.push_back(ep);

	m_mutex.unlock();

	return ep;
}

template <class K, class V>
void
SortReduce<K,V>::ManagerThread() {
	//printf( "maximum threads: %d\n", config->maximum_threads );

	const size_t reducer_from_mem_fan_in = 32;
	const size_t reducer_from_mem_fan_in_max = 32;
	const size_t reducer_from_storage_fan_in_max = 32;
	int reducer_from_mem_max_count = 1;
	
	std::chrono::high_resolution_clock::time_point last_time;
	std::chrono::milliseconds duration_milli;

	last_time = std::chrono::high_resolution_clock::now();

	uint64_t total_blocks_sorted = 0;
	uint64_t total_bytes_file_from_mem = 0;
	uint64_t total_bytes_file_from_storage = 0;

	struct statvfs fs_stat;

	int cur_thread_count = 0;

	size_t cur_storage_total_bytes = 0;
	size_t max_storage_bytes = m_config->max_storage_allocatd_bytes;

	int statvfs_ret =  statvfs(m_config->temporary_directory.c_str(), &fs_stat);

	//FIXME padding
	size_t fs_avail_bytes = fs_stat.f_bavail * fs_stat.f_bsize * 0.9;
	if (m_config->max_storage_allocatd_bytes == 0 && ( statvfs_ret != 0 || fs_avail_bytes == 0 ) ) {
		fprintf(stderr, "statvfs returns invalid storage capacity! Set storage usage manually via Config\n" );
		exit(1);
	}
	if ( max_storage_bytes == 0 && fs_avail_bytes > 0 ) max_storage_bytes = fs_avail_bytes;

	while (true) {
		//sleep(1);
		//if ( !m_done_inmem ) mp_block_sorter->CheckSpawnThreads();
	
		std::chrono::high_resolution_clock::time_point now;
		now = std::chrono::high_resolution_clock::now();

		duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-last_time);

		if ( !m_done_inmem && !m_reduce_phase && duration_milli.count() > 500 ) {
			last_time = now;
			AlignedBufferManager* buffer_manager = AlignedBufferManager::GetInstance(0);
			int free_cnt = buffer_manager->GetFreeCount();

			if ( free_cnt == 0 ) {
				size_t in_block_count = mp_block_sorter->GetInBlockCount();
				//size_t out_block_count = mp_block_sorter->GetOutBlockCount();
				size_t block_sorter_thread_count = mp_block_sorter->GetThreadCount();
				//size_t reducer_from_mem_threads = mv_stream_mergers_from_mem.size();
				//size_t reducer_from_storage_threads = mv_stream_mergers_from_storage.size();

				int threads_available = m_maximum_threads - 1 - block_sorter_thread_count - reducer_from_mem_max_count;// reducer_from_mem_threads - reducer_from_storage_threads;

				//printf( "---%d - %lu %lu\n", free_cnt, in_block_count, out_block_count );

				if ( block_sorter_thread_count < in_block_count ) {
					// If backed up more than there are threads*2
					// Bottleneck is the block sorter

					if ( threads_available > 0 ) {
						mp_block_sorter->SpawnThread();
					} else {
						if ( reducer_from_mem_max_count > 1 ) {
							reducer_from_mem_max_count --;
							printf( "reducer_from_mem_max_count %d\n", reducer_from_mem_max_count ); fflush(stdout);
						} else {
							//printf( "No more reducers to kill\n" );
						}
					}
				} 
				if ( /*reducer_from_mem_fan_in < out_block_count ||*/ in_block_count == 0 ) {
					// If output backed up more than fan in*2
					// Bottleneck is the from-mem reducer
					
					if ( threads_available > 0 ) {
						reducer_from_mem_max_count ++;
						printf( "reducer_from_mem_max_count %d << %d\n", reducer_from_mem_max_count, threads_available ); fflush(stdout);
					} else if ( reducer_from_mem_fan_in_max * reducer_from_mem_max_count + block_sorter_thread_count < (size_t)m_config->buffer_count ) {
						// delete sorter thread
						// reducer_from_mem_max_count can be increased next
						if ( block_sorter_thread_count > 1 ) {
							mp_block_sorter->KillThread();
							//printf( "Killing thread\n" );
						} else {
							//printf( "No more block sorters to kill\n" );
						}
					}
				}
				fflush(stdout);
			} else {
				// TODO check if input is bottleneck
			}


		}

		if ( m_reduce_phase == false && m_file_priority_queue.size() > 0 ) {
			//FIXME
			size_t required_space_safe = m_file_priority_queue.top()->bytes*reducer_from_storage_fan_in_max;
			if ( m_file_priority_queue.size() <= reducer_from_storage_fan_in_max ) {
				required_space_safe = cur_storage_total_bytes;
			}
			if ( !m_done_inmem && max_storage_bytes - cur_storage_total_bytes < required_space_safe ) {
				printf( "SortReduce entering reduce phase due to lack of storage %lu\n", max_storage_bytes - cur_storage_total_bytes );
				m_reduce_phase = true;
				while ( mp_block_sorter->GetThreadCount() > 0 ) {
					mp_block_sorter->KillThread();
				}
			}
		}

		size_t min_files_per_single_merger = 32;
		size_t max_files_per_single_merger = 128;
		// if GetOutBlock() returns more than ...say 16, spawn a merge-reducer
		size_t temp_file_count = m_file_priority_queue.size();
		if ( (m_done_inmem||m_reduce_phase) && 
			(
				(temp_file_count>1&&mv_stream_mergers_from_storage.empty()) || 
				temp_file_count >= min_files_per_single_merger // FIXME too much?
			) 

			//((m_done_inmem&&temp_file_count>1) || temp_file_count >= 16) 
			//&& mv_stream_mergers_from_storage.size() < (size_t)m_maximum_threads 
			&& cur_thread_count + 2 <= m_maximum_threads 
			&& mv_stream_mergers_from_storage.size() < 32 // FIXME(because of read buffer count)
			) {
			
			int to_sort = temp_file_count>max_files_per_single_merger?max_files_per_single_merger:temp_file_count;

			bool last_merge = false;
			if ( m_done_inmem && mv_stream_mergers_from_storage.empty() 
				&& temp_file_count < m_maximum_threads*min_files_per_single_merger
				&& temp_file_count < 256) { // FIXME (because of read buffer count)

				last_merge = true;
				to_sort = temp_file_count;
			}

			//SortReduceReducer::StreamMergeReducer<K,V>* merger;
			SortReduceReducer::MergeReducer<K,V>* merger;
			printf( "Want to start storage-storage merge with %d inputs out of %ld %s\n", to_sort, temp_file_count, last_merge?"last":"not last" );
			if ( last_merge ) {
				//merger = new SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>(m_config->update, m_config->temporary_directory, m_config->output_filename);
				
				if ( m_config->output_filename == "" ) {
					//FIXME...
					SortReduceReducer::MergeReducer_MultiTree<K,V>* mmerger = new SortReduceReducer::MergeReducer_MultiTree<K,V>(m_config->update, m_config->temporary_directory, m_maximum_threads);
					mp_result_stream_reader = mmerger->GetResultReader();
					merger = mmerger;
				} else {
					merger = new SortReduceReducer::MergeReducer_MultiTree<K,V>(m_config->update, m_config->temporary_directory, m_maximum_threads, m_config->output_filename);
				}
			} else if ( cur_thread_count + 2 <= m_maximum_threads ) {
				merger = new SortReduceReducer::MergeReducer_MultiTree<K,V>(m_config->update, m_config->temporary_directory, (m_maximum_threads-cur_thread_count)>4?4:(m_maximum_threads-cur_thread_count), "");
			} else {
				// Invisible temporary file
				merger = new SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>(m_config->update, m_config->temporary_directory);
			}

			//size_t last_bytes = 0;
			for ( int i = 0; i < to_sort; i++ ) {
				SortReduceTypes::File* file = m_file_priority_queue.top();

				m_file_priority_queue.pop();

/*
				if ( last_bytes > file->bytes ) {
					printf( "File size order wrong!\n" );fflush(stdout);
				} else {
					last_bytes = file->bytes;
				}
*/

				merger->PutFile(file);
				//printf( "%d -- %x %x\n", i, *((uint32_t*)block.buffer),((uint32_t*)block.buffer)[1] );
			}
			merger->Start();
			//printf( "Storage->Storage Reducer\n" );fflush(stdout);
			mv_stream_mergers_from_storage.push_back(merger);
			cur_thread_count += merger->GetThreadCount();
		}

		for ( int i = 0; (size_t)i < mv_stream_mergers_from_storage.size(); ) {
			SortReduceReducer::MergeReducer<K,V>* reducer = mv_stream_mergers_from_storage[i];
			if ( reducer->IsDone() ) {
				SortReduceTypes::File* reduced_file = reducer->GetOutFile();
				m_file_priority_queue.push(reduced_file);
				//printf( "Storage->Storage Pushed sort-reduced file ( size %lu ) -> %lu\n", reduced_file->bytes, m_file_priority_queue.size() ); fflush(stdout);
				total_bytes_file_from_storage += reduced_file->bytes;

				mv_stream_mergers_from_storage.erase(mv_stream_mergers_from_storage.begin() + i);

				cur_storage_total_bytes += reduced_file->bytes;
				cur_storage_total_bytes -= reducer->GetInputFileBytes();
				cur_thread_count -= reducer->GetThreadCount();
				delete reducer;

				if ( m_reduce_phase && mv_stream_mergers_from_storage.empty() ) {
					m_reduce_phase = false;
					printf( "SortReduce exiting reduce phase %lu\n", max_storage_bytes - cur_storage_total_bytes );
				}

			} else {
				i++;
			}
		}

		size_t sorted_blocks_cnt = mp_block_sorter->GetOutBlockCount();
		if ( !m_reduce_phase && ((m_done_input && sorted_blocks_cnt>0&&mv_stream_mergers_from_mem.empty()) || sorted_blocks_cnt >= reducer_from_mem_fan_in) 
			&& mv_stream_mergers_from_mem.size() < (size_t)reducer_from_mem_max_count ) {
			
			int to_sort = (sorted_blocks_cnt > reducer_from_mem_fan_in_max)?reducer_from_mem_fan_in_max:sorted_blocks_cnt; //TODO

			SortReduceReducer::MergeReducer<K,V>* merger = NULL;
			if ( to_sort == 1 ) {
				//FIXME just write it to file!
				merger = new SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>(m_config->update, m_config->temporary_directory);
			} else {
				merger = new SortReduceReducer::MergeReducer_MultiTree<K,V>(m_config->update, m_config->temporary_directory, 2, ""); // Just so we can use acceleration
			}

			
			for ( int i = 0; i < to_sort; i++ ) {
				SortReduceTypes::Block block = mp_block_sorter->GetOutBlock();
				block.last = true;
				merger->PutBlock(block);
				//printf( "%d -- %x %x\n", i, *((uint32_t*)block.buffer),((uint32_t*)block.buffer)[1] );
			}
			merger->Start();
			total_blocks_sorted += to_sort;

			mv_stream_mergers_from_mem.push_back(merger);
		}

		for ( size_t i = 0; i < mv_stream_mergers_from_mem.size(); ) {
			SortReduceReducer::MergeReducer<K,V>* reducer = mv_stream_mergers_from_mem[i];
			if ( reducer->IsDone() ) {
				SortReduceTypes::File* reduced_file = reducer->GetOutFile();
				m_file_priority_queue.push(reduced_file);
				total_bytes_file_from_mem += reduced_file->bytes;

				//size_t fsize = lseek(reduced_file->fd, 0, SEEK_END);
				//printf( "File size %lx %lx\n", fsize, reduced_file->bytes );
				printf( "Pushed sort-reduced file ( size %lu ) -> %lu\n", reduced_file->bytes, m_file_priority_queue.size() ); fflush(stdout);
				//printf( "from_mem erased %d\n", mv_stream_mergers_from_mem.size() );
				fflush(stdout);

				mv_stream_mergers_from_mem.erase(mv_stream_mergers_from_mem.begin() + i);
				delete reducer;

				cur_storage_total_bytes += reduced_file->bytes;
			} else {
				i++;
			}
		}

		if ( !m_done_inmem && m_done_input && mp_block_sorter->BlocksInFlight() == 0 
			&& mv_stream_mergers_from_mem.empty() ) {
			m_done_inmem = true;
			printf( "Im-memory sort done!\n" ); fflush(stdout);
				
			while ( mp_block_sorter->GetThreadCount() > 0 ) {
				mp_block_sorter->KillThread();
			}
			//TODO delete mp_block_sorter
		}

		if ( !m_done_external && m_done_input && m_done_inmem && !m_reduce_phase &&
			
			(
				(m_file_priority_queue.size() == 1 && mv_stream_mergers_from_storage.empty()) 
				|| (mp_result_stream_reader != NULL )
			)
			
			) {

			if ( mp_result_stream_reader == NULL ) {
				mp_file_kv_reader = new SortReduceUtils::FileKvReader<K,V>(m_file_priority_queue.top(), m_config);
			}

			mp_output_file = m_file_priority_queue.top();
			m_done_external = true;

			printf( "Sort-reduce all done! Processed %lu blocks\n", total_blocks_sorted); 
			printf( "Wrote %lu bytes to storage during inmem phase\n", total_bytes_file_from_mem );
			printf( "Wrote %lu bytes to storage during storage phase\n", total_bytes_file_from_storage );
			fflush(stdout);
			break;
		}

	}
}

template<class K, class V>
SortReduce<K,V>::IoEndpoint::IoEndpoint(SortReduce<K,V>* sr, bool input_only) {
	mp_sortreduce = sr;
	m_cur_update_block.valid = false;
	m_cur_update_offset = 0;
	m_done = false;
	m_input_only = input_only;
}

template<class K, class V>
inline bool
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

	mp_sortreduce->CheckInputDone();
}

template<class K, class V>
inline std::tuple<K,V,bool> 
SortReduce<K,V>::IoEndpoint::Next() {
	std::tuple<K,V,bool> ret;

	//return mp_sortreduce->Next(); //TODO blocks
	if ( m_input_only ) {
		ret = std::make_tuple(0,0,false);
		return ret; //TODO
	}
	return ret; //TODO
}

TEMPLATE_EXPLICIT_INSTANTIATION(SortReduce)
