#include "VertexValues.h"

template <class K, class V>
VertexValues<K,V>::VertexValues(std::string temp_directory, K key_count, V default_value, bool(*isactive)(V,V,bool), V(*finalize)(V,V), int thread_count) {

	std::chrono::high_resolution_clock::time_point start;
	start = std::chrono::high_resolution_clock::now();
	std::chrono::high_resolution_clock::time_point now;
	std::chrono::milliseconds duration_milli;

	m_cur_iteration = 0;
	m_active_cnt = 0;
	m_iteration_element_cnt = 0;
	m_max_thread_count = thread_count;
	m_cur_thread_count = 0;
	m_kill_threads = false;

	m_next_buffer_write_order = 0;
	m_cur_buffer_write_order = 0;

	mp_is_active = isactive;
	mp_finalize = finalize;
	m_default_value = default_value;

	char tmp_filename[128];
	m_temp_directory = temp_directory;
	sprintf(tmp_filename, "%s/vertex_data.dat", temp_directory.c_str() );
	m_vertex_data_fd = open(tmp_filename, O_RDWR|O_CREAT|O_DIRECT|O_TRUNC, S_IRUSR|S_IWUSR);

/*
	ma_value_cache = (ValueCacheItem*)malloc(sizeof(ValueCacheItem)*m_value_cache_size);
	for ( K i = 0; i < m_value_cache_size; i++ ) {
		ma_value_cache[i].iteration = 0;
		ma_value_cache[i].key = i;
		ma_value_cache[i].val = default_value;
		ma_value_cache[i].valid = false;
	}
*/

	ValueItem* defaultv = (ValueItem*)aligned_alloc(512,sizeof(ValueItem)*1024*1024);
	for ( int i = 0; i < 1024*1024; i++ ) {
		defaultv[i].iteration = 0;
		defaultv[i].val = default_value;
	}
	K cnt = 0;
	while (cnt < key_count) {
		write(m_vertex_data_fd, defaultv, sizeof(ValueItem)*1024*1024);
		cnt += 1024*1024;
	}
	free(defaultv);

	printf( "Created vertex values file at %s -- %ld\n", tmp_filename, (uint64_t)key_count ); 
	fflush(stdout);

	mp_io_buffer = aligned_alloc(512, m_io_buffer_alloc_size);
	mp_active_buffer = malloc(m_write_buffer_alloc_size);
	//mp_active_buffer = malloc(m_io_buffer_alloc_items*sizeof(KvPair)+1);
	m_active_buffer_idx = 0;


	m_active_vertices_fd = 0;
	for ( int i = 0; i < m_max_thread_count; i++ ) {
		maq_req_blocks[i] = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();
		ma_active_count[i] = 0;
	}

	this->mq_free_block = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();
	for ( int i = 0; i < MAX_VERTEXVAL_REQS; i++ ) {
		SortReduceTypes::Block block;
		block.valid = true;
		block.last = true; // each block is independent
		block.bytes = m_io_buffer_alloc_size;
		block.buffer = aligned_alloc(512, m_io_buffer_alloc_size);
		mq_free_block->push(block);

	}
	for ( int i = 0; i < MAX_VERTEXVAL_THREADS; i++ ) {
		ma_cur_out_idx[i] = 0;
		ma_cur_out_block[i].valid = false;
	}
		
	now = std::chrono::high_resolution_clock::now();
	duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-start);
	printf( "VertexValues default init done : %lu ms\n", duration_milli.count() );
}

template <class K, class V>
VertexValues<K,V>::~VertexValues() {
	if ( m_vertex_data_fd > 0 ) close (m_vertex_data_fd);
	free(mp_io_buffer);
	free(mp_active_buffer);
}

template <class K, class V>
void
VertexValues<K,V>::Start() {
	if ( m_active_vertices_fd == 0 ) {
		char filename[128];
		sprintf(filename, "%s/vertex_%d.dat", m_temp_directory.c_str(), m_cur_iteration );
		m_active_vertices_fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
		lseek(m_active_vertices_fd, 0, SEEK_SET);
	}
	for ( int i = m_cur_thread_count; i < m_max_thread_count; i++ ) {
		ma_worker_thread[i] = std::thread(&VertexValues<K,V>::WorkerThread, this, i);
	}
	m_cur_thread_count = m_max_thread_count;

	m_active_buffer_idx = 0;

	m_last_key = 0;
	m_last_out_key = 0;
	m_kv_cnt = 0;
	print_cnt = 0;
}

template <class K, class V>
void
VertexValues<K,V>::Finish() {
	for ( int thread_idx = 0; thread_idx < m_cur_thread_count; thread_idx++ ) {
		if ( ma_cur_out_block[thread_idx].valid == false ) continue;

		ma_cur_out_block[thread_idx].valid_bytes = (ma_cur_out_idx[thread_idx]*sizeof(KvPair));
		maq_req_blocks[thread_idx]->push(ma_cur_out_block[thread_idx]);
		ma_cur_out_block[thread_idx].valid = false;
	}


	m_kill_threads = true;
	m_total_active_cnt = m_active_cnt;
	for ( int i = 0; i < m_cur_thread_count; i++ ) {
		ma_worker_thread[i].join();
		m_total_active_cnt += ma_active_count[i];
	}
	m_cur_thread_count = 0;
	
	if ( m_active_buffer_idx > 0 ) {
		write(m_active_vertices_fd, mp_active_buffer, m_active_buffer_idx*sizeof(KvPair));
		m_active_buffer_idx = 0;
	}


	m_kill_threads = false;

	if ( m_active_vertices_fd != 0 ) {
		close(m_active_vertices_fd);
		m_active_vertices_fd = 0;
	
		//char filename[128];
		//sprintf(filename, "%s/vertex_%d.dat", m_temp_directory.c_str(), m_cur_iteration );
		//unlink(filename);
	}
	
	printf( "\t\t++ Iteration %d done: %ld\n", m_cur_iteration, m_iteration_element_cnt );
}

template <class K, class V>
void
VertexValues<K,V>::NextIteration() {
	m_cur_iteration++;

	for ( int i = 0; i < m_max_thread_count; i++ ) {
		ma_active_count[i] = 0;
	}
	m_active_cnt = 0;
	m_iteration_element_cnt = 0;
}

template <class K, class V>
size_t
VertexValues<K,V>::GetActiveCount(){
	return m_total_active_cnt;
}


template <class K, class V>
inline bool
VertexValues<K,V>::Update(K key, V val){
/*
	if ( key < m_last_key ) {
		printf( "VertexValues::Update key order wrong %lx %x %x %lx\n", m_kv_cnt, m_last_key, key, val );
		print_cnt = 10;
	} else if ( key == 0 || print_cnt > 0 ) {
		printf( "VertexValues::Update key %x %x %x\n", m_kv_cnt, key, val );
		print_cnt--;
	}
	m_last_key = key;
	m_kv_cnt++;
	*/
	
	

/*
	ValueCacheItem vci = ma_value_cache[key%m_value_cache_size];
	
	if ( vci.valid == false || vci.key == key ) {
		bool is_marked = (vci.iteration == m_cur_iteration);
		bool is_active = mp_is_active(vci.val, val, is_marked);
		if ( is_active ) {
			ValueCacheItem* p_vci = &ma_value_cache[key%m_value_cache_size];
			p_vci->iteration = m_cur_iteration+1;
			p_vci->key = key;
			p_vci->val = val;
			p_vci->valid = true;
			m_active_cnt++;


			KvPair wkvp;
			wkvp.key = key;
			wkvp.val = val;
			KvPair* active_buffer = (KvPair*)mp_active_buffer;
			active_buffer[m_active_buffer_idx] = wkvp;
			m_active_buffer_idx++;

			if ( m_active_buffer_idx*sizeof(KvPair) > m_write_buffer_alloc_size ) {
				write(m_active_vertices_fd, mp_active_buffer, m_active_buffer_idx*sizeof(KvPair));
				m_active_buffer_idx = 0;
			}

		}
		m_iteration_element_cnt++;
		return true;
	}
*/

	
	size_t buffer_offset = key/m_io_buffer_alloc_items;
	size_t thread_idx = buffer_offset % (m_cur_thread_count);

	if ( ma_cur_out_block[thread_idx].valid == false ) {
		ma_cur_out_block[thread_idx] = mq_free_block->get();
		ma_cur_out_idx[thread_idx] = 0;
	}
	if ( ma_cur_out_block[thread_idx].valid == false ) return false;
	m_iteration_element_cnt++;

	KvPair* buf = (KvPair*)ma_cur_out_block[thread_idx].buffer;
	size_t size = ma_cur_out_block[thread_idx].bytes;
	KvPair kvp;
	kvp.key = key;
	kvp.val = val;
	buf[ma_cur_out_idx[thread_idx]++] = kvp;
	if ( ma_cur_out_idx[thread_idx]* sizeof(KvPair) > size ) {
		ma_cur_out_block[thread_idx].valid_bytes = (ma_cur_out_idx[thread_idx]*sizeof(KvPair));
		//printf( "Pushing to %d\n", thread_idx );
		maq_req_blocks[thread_idx]->push(ma_cur_out_block[thread_idx]);
		ma_cur_out_block[thread_idx].valid = false;
	}

	return true;


/*
	size_t byte_offset = sizeof(ValueItem)*((size_t)key);
	if ( byte_offset < m_io_buffer_offset || byte_offset+sizeof(ValueItem) > m_io_buffer_offset+m_io_buffer_bytes ) {

		if ( m_io_buffer_dirty ) {
			// write it back to disk
			lseek(m_vertex_data_fd, m_io_buffer_offset, SEEK_SET);
			write(m_vertex_data_fd, mp_io_buffer, m_io_buffer_bytes);
			m_io_buffer_bytes = 0;

			//printf( "Wrote dirty buffer at %x\n", m_io_buffer_offset );
		} 
		
		//size_t offset_aligned = byte_offset-(byte_offset%m_io_buffer_alloc_size);
		size_t offset_aligned = byte_offset&(~0x3ff); // 1 KB alignment
		pread(m_vertex_data_fd, mp_io_buffer, m_io_buffer_alloc_size, offset_aligned);
		m_io_buffer_offset = offset_aligned;
		m_io_buffer_bytes = m_io_buffer_alloc_size;
		m_io_buffer_dirty = false;

		//printf( "Read new buffer at %x\n", byte_offset_aligned );
	}

	size_t internal_offset = byte_offset - m_io_buffer_offset;
	ValueItem* pvi = ((ValueItem*)(((uint8_t*)mp_io_buffer)+internal_offset));
	ValueItem vi = *pvi;
	bool is_marked = (vi.iteration == m_cur_iteration);
	bool is_active = mp_is_active(vi.val, val, is_marked);


	if ( is_active ) {
		vi.val = val;
		vi.iteration = m_cur_iteration + 1;
		*pvi = vi;

		m_io_buffer_dirty = true;


		//TODO buffer for performance
		//FIXME maintain order!
		//write(m_active_vertices_fd, &key, sizeof(K));
		//write(m_active_vertices_fd, &val, sizeof(V));
		//printf( "Write %x %x\n", key, val );
		
		
		
		KvPair wkvp;
		wkvp.key = key;
		wkvp.val = val;
		KvPair* active_buffer = (KvPair*)mp_active_buffer;
		active_buffer[m_active_buffer_idx] = wkvp;
		m_active_buffer_idx++;
		
		if ( m_active_buffer_idx*sizeof(KvPair) > m_write_buffer_alloc_size ) {
			write(m_active_vertices_fd, mp_active_buffer, m_active_buffer_idx*sizeof(KvPair));
			m_active_buffer_idx = 0;
		}
		
		m_active_cnt++;


		//printf( "Active vertex %x, %x\n", key, m_active_cnt );
	}

	return true;
	*/
}

template <class K, class V>
int 
VertexValues<K,V>::OpenActiveFile(uint32_t iteration) {
	char filename[128];
	sprintf(filename, "%s/vertex_%d.dat", m_temp_directory.c_str(), iteration );
	int fd = open(filename, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
	return fd;
}

template <class K, class V>
void
VertexValues<K,V>::WorkerThread(int idx) {

	//printf( "VertexValues::WorkerThread spawned %d\n", idx ); fflush(stdout);
	io_context_t io_ctx;
	struct io_event a_events[AIO_DEPTH];
	struct iocb a_iocb[AIO_DEPTH];

	memset(&io_ctx, 0, sizeof(io_ctx));
	if( io_setup( AIO_DEPTH, &io_ctx ) != 0 ) {
		fprintf(stderr, "%s %d io_setup error\n", __FILE__, __LINE__);
		return;
	}
	typedef struct {
		int buf_idx;
		bool write;
		size_t offset;
		size_t bytes;
		void* buffer;
		bool done;
	} IocbArgs;
	IocbArgs a_request_args[AIO_DEPTH];
	std::queue<int> q_free_req;
	std::queue<int> q_aio_order_read;
	for ( int i = 0; i < AIO_DEPTH; i++ ) {
		q_free_req.push(i);
		a_request_args[i].buffer = aligned_alloc(512, m_io_buffer_alloc_size);
	}

	SortReduceTypes::Block cur_block;
	cur_block.valid = false;
	size_t cur_block_offset_idx = 0;

	SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* q_req = maq_req_blocks[idx];
	std::queue<KvPair> q_req_kv;
	std::queue<int> q_write;
	
	size_t req_buffer_offset = 0;
	size_t req_buffer_bytes = 0;

	size_t resp_buffer_offset = 0;
	size_t resp_buffer_bytes = 0;
	void* resp_buffer = NULL;
	int resp_arg_idx = -1;

	int aio_inflight = 0;
	bool io_buffer_dirty = false;
	
	void* p_active_buffer = malloc(m_write_buffer_alloc_size);
	size_t active_buffer_idx = 0;


	while ( true ) {
		if ( cur_block.valid == false ) {
			cur_block =  q_req->get();
			cur_block_offset_idx = 0;
			//if ( cur_block.valid ) printf( "Block got %ld\n", cur_block.bytes );
		}
		while ( cur_block.valid && q_req_kv.size() < 1024*1024 ) {
			KvPair* buf = (KvPair*)cur_block.buffer;
			size_t size = cur_block.valid_bytes;
			size_t cnt = size/sizeof(KvPair);
			KvPair kvp = buf[cur_block_offset_idx];

			size_t byte_offset = sizeof(ValueItem)*((size_t)kvp.key);
			if ( byte_offset < req_buffer_offset || byte_offset+sizeof(ValueItem) > req_buffer_offset+req_buffer_bytes ) {
				size_t offset_aligned = byte_offset-(byte_offset%m_io_buffer_alloc_size);
				if ( !q_free_req.empty() ) {
					int idx = q_free_req.front();
					q_free_req.pop();

					IocbArgs* args = &a_request_args[idx];
					io_prep_pread(&a_iocb[idx], m_vertex_data_fd, args->buffer, m_io_buffer_alloc_size, offset_aligned);
					args->write = false;
					args->buf_idx = idx;
					args->offset = offset_aligned;
					args->bytes = m_io_buffer_alloc_size;
					args->done = false;
					a_iocb[idx].data =args;
					struct iocb* iocbs = &a_iocb[idx];
					int ret_count = io_submit(io_ctx, 1, &iocbs);
					if ( ret_count <= 0 ) {
						q_free_req.push(idx);
						break;
					} else {
						aio_inflight += ret_count;
						q_req_kv.push(kvp);
						cur_block_offset_idx++;

						req_buffer_offset = offset_aligned;
						req_buffer_bytes = m_io_buffer_alloc_size;
						q_aio_order_read.push(idx);
						
						//printf( "VertexValues::WorkerThread io_submit %d %lx %lx\n", idx, offset_aligned, m_io_buffer_alloc_size ); fflush(stdout);
					}
				} else {
					// do nothing
					break;
				}
			} else {
				q_req_kv.push(kvp);
				cur_block_offset_idx++;
				//printf( "VertexValues::WorkerThread matched cache\n" ); fflush(stdout);
			}

			if ( cur_block_offset_idx >= cnt ) {
				mq_free_block->push(cur_block);
				cur_block.valid = false;
				cur_block_offset_idx = 0;
			}
		}
		int num_events = io_getevents(io_ctx, 0, AIO_DEPTH, a_events, NULL);
		for ( int i = 0; i < num_events; i++ ) {
			struct io_event event = a_events[i];
			IocbArgs* arg = (IocbArgs*)event.data;
			arg->done = true;
			if ( arg->write ) {
				q_free_req.push(arg->buf_idx);
			}
		}
		aio_inflight -= num_events;


		while ( !q_req_kv.empty() ) {
			KvPair kvp = q_req_kv.front();

			size_t byte_offset = sizeof(ValueItem)*((size_t)kvp.key);
			if ( byte_offset < resp_buffer_offset || byte_offset+sizeof(ValueItem) > resp_buffer_offset+resp_buffer_bytes ) {

				if ( q_aio_order_read.empty() ) break;
				int aio_idx = q_aio_order_read.front();

				if ( a_request_args[aio_idx].done ) {
					q_aio_order_read.pop();
					a_request_args[aio_idx].done = false;

					IocbArgs* arg = &a_request_args[aio_idx];
					if ( resp_arg_idx >= 0 ) {
						if ( io_buffer_dirty ) {
							q_write.push(resp_arg_idx);
						} else {
							q_free_req.push(resp_arg_idx);
						}
					}
					resp_buffer_offset = arg->offset;
					resp_buffer_bytes = arg->bytes;
					resp_buffer = arg->buffer;
					//printf( "VertexValues::WorkerThread reading %d %lx %x\n", aio_idx, resp_buffer_offset ,resp_buffer_bytes ); fflush(stdout);
					resp_arg_idx = arg->buf_idx;
				} else break;
			}
			//printf( "VertexValues::WorkerThread kv %lx %lx %lx \n", byte_offset, kvp.key, kvp.val ); fflush(stdout);
	
			size_t internal_offset = byte_offset - resp_buffer_offset;
			if ( internal_offset+sizeof(ValueItem) > resp_buffer_bytes ) {
				printf( "VertexValues::WorkerThread why %lx \n", byte_offset ); fflush(stdout);
				break;
			}
			q_req_kv.pop();

			ValueItem* pvi = ((ValueItem*)(((uint8_t*)resp_buffer)+internal_offset));
			ValueItem vi = *pvi;
			bool is_marked = (vi.iteration == m_cur_iteration);
			V final_val = mp_finalize(vi.val, kvp.val);

			bool is_active = mp_is_active(vi.val, final_val, is_marked);

			//printf( "isactive %lx %lx %s %s\n", vi.val, kvp.val, is_marked?"Y":"N", is_active?"Y":"N" );

			if ( is_active ) {
				vi.val = kvp.val;
				vi.iteration = m_cur_iteration + 1;
				*pvi = vi;

				io_buffer_dirty = true;

				KvPair wkvp;
				wkvp.key = kvp.key;
				wkvp.val = kvp.val;
				KvPair* active_buffer = (KvPair*)p_active_buffer;
				active_buffer[active_buffer_idx] = wkvp;
				active_buffer_idx++;

				if ( active_buffer_idx*sizeof(KvPair) > m_write_buffer_alloc_size ) {
					//FIXME not in order across vertices... maybe that's okay
					m_mutex.lock();
					write(m_active_vertices_fd, p_active_buffer, active_buffer_idx*sizeof(KvPair));
					active_buffer_idx = 0;
					m_mutex.unlock();
				}

				ma_active_count[idx]++;


			}
		}

		if ( !q_write.empty() ) {
			int idx = q_write.front();
			q_write.pop();
			//printf( "VertexValues::WorkerThread writing  \n" ); fflush(stdout);

			IocbArgs* args = &a_request_args[idx];
			memset(&a_iocb[idx], 0, sizeof(a_iocb[idx]));
			io_prep_pwrite(&a_iocb[idx], m_vertex_data_fd, args->buffer, args->bytes, args->offset);
			args->write = true;
			a_iocb[idx].data =args;
			struct iocb* iocbs = &a_iocb[idx];
			int ret_count = io_submit(io_ctx, 1, &iocbs);
			if ( ret_count <= 0 ) {
				q_write.push(idx);
			} else {
				aio_inflight += ret_count;
			}
		}


		// break when
		if ( m_kill_threads && q_req->size() == 0 && cur_block_offset_idx == 0 && q_req_kv.empty() && q_write.empty() && aio_inflight == 0 ) break;
	}

	if ( resp_arg_idx >= 0 ) {
		int idx = resp_arg_idx;
		IocbArgs* args = &a_request_args[idx];
		memset(&a_iocb[idx], 0, sizeof(a_iocb[idx]));
		io_prep_pwrite(&a_iocb[idx], m_vertex_data_fd, args->buffer, args->bytes, args->offset);
		args->write = true;
		a_iocb[idx].data =args;
		struct iocb* iocbs = &a_iocb[idx];
		while ( io_submit(io_ctx, 1, &iocbs) <= 0 ) usleep(1000);
	}
	
	if ( active_buffer_idx > 0 ) {
		//FIXME not in order across vertices... maybe that's okay
		m_mutex.lock();
		write(m_active_vertices_fd, p_active_buffer, active_buffer_idx*sizeof(KvPair));
		active_buffer_idx = 0;
		m_mutex.unlock();
	}

	if ( resp_arg_idx >= 0 ) {
		while ( io_getevents(io_ctx, 0, AIO_DEPTH, a_events, NULL) == 0 ) usleep(1000);
	}



	for ( int i = 0; i < AIO_DEPTH; i++ ) {
		free(a_request_args[i].buffer);
	}
	free(p_active_buffer);

	//printf( "VertexValues WorkerThread done %d\n", idx );


	io_destroy(io_ctx);
}

TEMPLATE_EXPLICIT_INSTANTIATION(VertexValues)
