#include "VertexValues.h"

template <class K, class V>
VertexValues<K,V>::VertexValues(std::string temp_directory, K key_count, V default_value, bool(*isactive)(V,V,bool), int thread_count) {

	std::chrono::high_resolution_clock::time_point start;
	start = std::chrono::high_resolution_clock::now();
	std::chrono::high_resolution_clock::time_point now;
	std::chrono::milliseconds duration_milli;

	m_cur_iteration = 0;
	m_active_cnt = 0;
	m_max_thread_count = thread_count;
	m_cur_thread_count = 0;
	m_kill_threads = false;

	m_next_buffer_write_order = 0;
	m_cur_buffer_write_order = 0;

	mp_is_active = isactive;
	m_default_value = default_value;

	char tmp_filename[128];
	m_temp_directory = temp_directory;
	sprintf(tmp_filename, "%s/vertex_data.dat", temp_directory.c_str() );
	m_vertex_data_fd = open(tmp_filename, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
	ValueItem defaultv[1024];
	for ( int i = 0; i < 1024; i++ ) {
		defaultv[i].iteration = 0;
		defaultv[i].val = default_value;
	}
	K cnt = 0;
	while (cnt < key_count) {
		write(m_vertex_data_fd, defaultv, sizeof(defaultv));
		cnt += 1024;
	}
	printf( "Created vertex values file at %s -- %s\n", tmp_filename, temp_directory.c_str() ); 
	fflush(stdout);

	mp_io_buffer = malloc(m_io_buffer_alloc_size);
	mp_active_buffer = malloc(m_io_buffer_alloc_items*sizeof(KvPair)+1);
	m_active_buffer_idx = 0;


	m_active_vertices_fd = 0;
	for ( int i = 0; i < MAX_VERTEXVAL_THREADS; i++ ) {
		maq_req_blocks[i] = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();
		ma_active_count[i] = 0;
	}

	mq_free_block = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();
	for ( int i = 0; i < MAX_VERTEXVAL_REQS; i++ ) {
		SortReduceTypes::Block block;
		block.valid = true;
		block.last = true; // each block is independent
		block.bytes = m_io_buffer_alloc_size;
		block.buffer = malloc(m_io_buffer_alloc_size);
		mq_free_block->push(block);

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
}

template <class K, class V>
void
VertexValues<K,V>::NextIteration() {
	for ( int thread_idx = 0; thread_idx < m_cur_thread_count; thread_idx++ ) {
		if ( ma_cur_out_block[thread_idx].valid == false ) continue;

		ma_cur_out_block[thread_idx].valid_bytes = (ma_cur_out_idx[thread_idx]*sizeof(KvPair));
		maq_req_blocks[thread_idx]->push(ma_cur_out_block[thread_idx]);
		ma_cur_out_block[thread_idx].valid = false;
	}


	if ( m_active_buffer_idx > 0 ) {
		write(m_active_vertices_fd, mp_active_buffer, m_active_buffer_idx*sizeof(KvPair));
		m_active_buffer_idx = 0;
	}

	m_kill_threads = true;
	for ( int i = 0; i < m_cur_thread_count; i++ ) {
		ma_worker_thread[i].join();
	}
	m_cur_thread_count = 0;

	m_kill_threads = false;

	if ( m_active_vertices_fd != 0 ) {
		close(m_active_vertices_fd);
		m_active_vertices_fd = 0;
	
		//char filename[128];
		//sprintf(filename, "%s/vertex_%d.dat", m_temp_directory.c_str(), m_cur_iteration );
		//unlink(filename);
	}
	
	m_cur_iteration++;
	m_active_cnt = 0;
}

template <class K, class V>
size_t
VertexValues<K,V>::GetActiveCount(){
	size_t ret = m_active_cnt;
	for ( int i = 0; i < m_cur_thread_count; i++ ) {
		ret += ma_active_count[i];
	}
	return ret;
}


template <class K, class V>
inline bool
VertexValues<K,V>::Update(K key, V val){
	size_t buffer_offset = key/m_io_buffer_alloc_items;
	size_t thread_idx = buffer_offset % (m_cur_thread_count+1);
	if ( thread_idx > 0 ) {
		thread_idx--; // 0 is current thread

		//send it to ...
		if ( ma_cur_out_block[thread_idx].valid == false ) {
			ma_cur_out_block[thread_idx] = mq_free_block->get();
			ma_cur_out_idx[thread_idx] = 0;
		}
		if ( ma_cur_out_block[thread_idx].valid == false ) return false;

		KvPair* buf = (KvPair*)ma_cur_out_block[thread_idx].buffer;
		size_t size = ma_cur_out_block[thread_idx].bytes;
		KvPair kvp;
		kvp.key = key;
		kvp.val = val;
		buf[ma_cur_out_idx[thread_idx]++] = kvp;
		if ( ma_cur_out_idx[thread_idx]* sizeof(KvPair) > size ) {
			ma_cur_out_block[thread_idx].valid_bytes = (ma_cur_out_idx[thread_idx]*sizeof(KvPair));
			maq_req_blocks[thread_idx]->push(ma_cur_out_block[thread_idx]);
			ma_cur_out_block[thread_idx].valid = false;
		}

		return true;
	} 


	size_t byte_offset = sizeof(ValueItem)*((size_t)key);
	if ( byte_offset < m_io_buffer_offset || byte_offset+sizeof(ValueItem) > m_io_buffer_offset+m_io_buffer_bytes ) {

		if ( m_io_buffer_dirty ) {
			// write it back to disk
			lseek(m_vertex_data_fd, m_io_buffer_offset, SEEK_SET);
			write(m_vertex_data_fd, mp_io_buffer, m_io_buffer_bytes);
			m_io_buffer_bytes = 0;

			//printf( "Wrote dirty buffer at %x\n", m_io_buffer_offset );
		} 
		if ( m_active_buffer_idx > 0 ) {
			write(m_active_vertices_fd, mp_active_buffer, m_active_buffer_idx*sizeof(KvPair));
			m_active_buffer_idx = 0;
		}
		size_t offset_aligned = (byte_offset/m_io_buffer_alloc_size)*m_io_buffer_alloc_size;
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

/*
		if ( m_active_vertices_fd == 0 ) {
			char filename[128];
			sprintf(filename, "%s/vertex_%d.dat", m_temp_directory.c_str(), m_cur_iteration );
			m_active_vertices_fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
			lseek(m_active_vertices_fd, 0, SEEK_SET);
		}
*/

		//TODO buffer for performance
		//FIXME maintain order!
		//write(m_active_vertices_fd, &key, sizeof(K));
		//write(m_active_vertices_fd, &val, sizeof(V));
		//printf( "Write %x %x\n", key, val );
		KvPair wkvp;
		wkvp.key = key;
		wkvp.val = val;
		KvPair* active_buffer = (KvPair*)mp_active_buffer;
		active_buffer[m_active_buffer_idx++] = wkvp;
		m_active_cnt++;

		//printf( "Active vertex %x, %x\n", key, m_active_cnt );
	}

	return true;
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
	printf( "VertexValues::WorkerThread spawned\n" ); fflush(stdout);
	size_t buffer_offset;
	size_t buffer_bytes;
	bool buffer_dirty = false;
	ma_active_count[idx] = 0;
	void* buffer = malloc(m_io_buffer_alloc_size);

	while (!m_kill_threads || maq_req_blocks[idx]->size() > 0 ) {
		SortReduceTypes::Block block = maq_req_blocks[idx]->get();
		if ( !block.valid ) {
			usleep(1000);
			continue;
		}
		KvPair* buf = (KvPair*)block.buffer;
		size_t size = block.valid_bytes;
		size_t cnt = size/sizeof(KvPair);
		for ( size_t i = 0; i < cnt; i++ ) {
			KvPair kvp = buf[i];
			K key = kvp.key;
			V val = kvp.val;

			size_t byte_offset = sizeof(ValueItem)*((size_t)key);
			if ( byte_offset < buffer_offset || byte_offset+sizeof(ValueItem) > buffer_offset+buffer_bytes ) {

				if ( buffer_dirty ) {
					// write it back to disk
					lseek(m_vertex_data_fd, buffer_offset, SEEK_SET);
					write(m_vertex_data_fd, buffer, buffer_bytes);
					buffer_bytes = 0;

					//printf( "Wrote dirty buffer at %x\n", m_io_buffer_offset );
				} 
				size_t offset_aligned = (byte_offset/m_io_buffer_alloc_size)*m_io_buffer_alloc_size;
				pread(m_vertex_data_fd, buffer, m_io_buffer_alloc_size, offset_aligned);
				buffer_offset = offset_aligned;
				buffer_bytes = m_io_buffer_alloc_size;
				buffer_dirty = false;

				//printf( "Read new buffer at %x\n", byte_offset_aligned );
			}

			size_t internal_offset = byte_offset - buffer_offset;
			ValueItem* pvi = ((ValueItem*)(((uint8_t*)buffer)+internal_offset));
			ValueItem vi = *pvi;
			bool is_marked = (vi.iteration == m_cur_iteration);
			bool is_active = mp_is_active(vi.val, val, is_marked);

			if ( is_active ) {
				vi.val = val;
				vi.iteration = m_cur_iteration + 1;
				*pvi = vi;

				buffer_dirty = true;

				/*
				if ( m_active_vertices_fd == 0 ) {
					char filename[128];
					sprintf(filename, "%s/vertex_%d.dat", m_temp_directory.c_str(), m_cur_iteration );
					m_active_vertices_fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
					lseek(m_active_vertices_fd, 0, SEEK_SET);
				}
				*/

				//TODO buffer for performance
				//FIXME maintain order!!
				//write(m_active_vertices_fd, &key, sizeof(K));
				//write(m_active_vertices_fd, &val, sizeof(V));
				//printf( "Write %x %x\n", key, val );
				ma_active_count[idx]++;

				//printf( "Active vertex %x, %x\n", key, m_active_cnt );
			}
		}

	
		mq_free_block->push(block);
	}

	free(buffer);
}

TEMPLATE_EXPLICIT_INSTANTIATION(VertexValues)
