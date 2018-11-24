#include "EdgeProcess.h"

template <class K, class V>
EdgeProcess<K,V>::EdgeProcess(std::string ridx_path, std::string matrix_path, V(*edge_program)(K,V,K)) {
	m_idx_buffer_bytes = 0;
	m_edge_buffer_bytes = 0;
	m_thread_count = 0;
	m_ep_count = 0;
	mp_sr_ep = NULL;
	
	mp_edge_program = edge_program;
	m_ridx_fd = open(ridx_path.c_str(), O_RDONLY|O_DIRECT, S_IRUSR|S_IWUSR);
	m_matrix_fd = open(matrix_path.c_str(), O_RDONLY|O_DIRECT, S_IRUSR|S_IWUSR);
	if ( m_ridx_fd < 0 || m_matrix_fd < 0 ) {
		fprintf(stderr, "ERROR: Graph ridx or matrix file not found\n" );
	}
	//mp_idx_buffer = malloc(m_buffer_alloc_bytes);
	mp_idx_buffer = aligned_alloc(512, m_buffer_alloc_bytes);
	//mp_edge_buffer = malloc(m_buffer_alloc_bytes);
	mp_edge_buffer = aligned_alloc(512, m_buffer_alloc_bytes);

	mq_req_blocks = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();
	mq_free_block = new SortReduceUtils::MutexedQueue<SortReduceTypes::Block>();
	for ( int i = 0; i < MAX_EDGEPROC_REQS; i++ ) {
		SortReduceTypes::Block block;
		block.valid = true;
		block.last = true; // each block is independent
		block.bytes = m_block_bytes;
		block.buffer = malloc(m_block_bytes);
		//ma_block_bytes[i] = block;
		mq_free_block->push(block);
	}

	for ( int i = 0; i < MAX_EDGEPROC_THREADS; i++ ) {
		ma_sr_ep[i] = NULL;
	}
	m_kill_threads = false;

	m_cur_out_block.valid = false;
}

template <class K, class V>
EdgeProcess<K,V>::~EdgeProcess() {
}

template <class K, class V>
size_t
EdgeProcess<K,V>::GetVertexCount() {
	size_t bytes = lseek(m_ridx_fd, 0, SEEK_END);
	return (bytes/sizeof(uint64_t));
}

template <class K, class V>
void 
EdgeProcess<K,V>::SetSortReduceEndpoint(typename SortReduce<K,V>::IoEndpoint* ep) {
	mp_sr_ep = ep;
	m_ep_count = 0;
}
template <class K, class V>
void 
EdgeProcess<K,V>::AddSortReduceEndpoint(typename SortReduce<K,V>::IoEndpoint* ep) {
	if ( m_ep_count >= MAX_EDGEPROC_THREADS ) {
		fprintf(stderr, "WARNING: EdgeProcess::AddProcessThread exceeds MAX_EDGEPROC_THREADS %s:%d\n",__FILE__,__LINE__ );
		ep->Finish();
		return;
	}
	ma_sr_ep[m_ep_count] = ep;
	//m_thread_count++;
	m_ep_count++;
}

template <class K, class V>
void 
EdgeProcess<K,V>::Start() {
	for ( int i = m_thread_count; i < m_ep_count; i++ ) {
		ma_sr_thread[i] = std::thread(&EdgeProcess<K,V>::WorkerThread,this, i);
	}
	m_thread_count = m_ep_count;

	m_index_blocks_read = 0;
	m_edge_blocks_read = 0;
}

template <class K, class V>
void 
EdgeProcess<K,V>::Finish() {
	//Send over last buffer
	m_cur_out_block.valid_bytes = (m_cur_out_idx*sizeof(KvPair));
	mq_req_blocks->push(m_cur_out_block);
	m_cur_out_block.valid = false;

	mp_sr_ep->Finish();

	// kill all threads
	m_kill_threads = true;
	for ( int i = 0; i < m_thread_count; i++ ) {
		ma_sr_thread[i].join();
	}
	m_thread_count = 0;

	m_kill_threads = false;

	printf( "Index bytes read : %lx\n", m_index_blocks_read*m_buffer_alloc_bytes );
	printf( "Edge bytes read : %lx\n", m_edge_blocks_read*m_buffer_alloc_bytes);
}

template <class K, class V>
inline void
EdgeProcess<K,V>::SourceVertex(K key, V val, bool write ) {
	if ( m_cur_out_block.valid == false ) {
		m_cur_out_block = mq_free_block->get();
		m_cur_out_idx = 0;
	}

	// If we have a free buffer, push it there
	if ( m_cur_out_block.valid ) {
		KvPair* buf = (KvPair*)m_cur_out_block.buffer;
		size_t size = m_cur_out_block.bytes;
		KvPair kvp;
		kvp.key = key;
		kvp.val = val;
		buf[m_cur_out_idx++] = kvp;
		if ( m_cur_out_idx* sizeof(KvPair) > size ) {
			m_cur_out_block.valid_bytes = (m_cur_out_idx*sizeof(KvPair));
			mq_req_blocks->push(m_cur_out_block);
			m_cur_out_block.valid = false;
		}
		return;
	}

	// If all other threads are busy, do it here
	size_t edge_element_bytes = sizeof(K);
	size_t byte_offset = ((size_t)key)*sizeof(uint64_t);
	if ( byte_offset < m_idx_buffer_offset || byte_offset + 2*sizeof(uint64_t) > m_idx_buffer_offset+m_idx_buffer_bytes ) {
		size_t byte_offset_aligned = byte_offset&(~0x3ff); // 1 KB alignment
		pread(m_ridx_fd, mp_idx_buffer, m_buffer_alloc_bytes, byte_offset_aligned);
		m_idx_buffer_offset = byte_offset_aligned;
		m_idx_buffer_bytes = m_buffer_alloc_bytes;
		//printf( "Read new %lx %lx -- %s\n", ret, ((uint64_t*)mp_idx_buffer)[4], strerror(errno ));

		m_index_blocks_read++;
	}
	size_t internal_offset = byte_offset - m_idx_buffer_offset;

	uint64_t byte_offset_1 = *((uint64_t*)(((uint8_t*)mp_idx_buffer)+internal_offset));
	uint64_t byte_offset_2 = *((uint64_t*)(((uint8_t*)mp_idx_buffer)+internal_offset+sizeof(uint64_t)));
	uint64_t edge_bytes = byte_offset_2 - byte_offset_1;
	uint32_t fanout = (uint32_t)(edge_bytes/edge_element_bytes);

	//printf( "Source vertex %x -- %lx %lx fanout %x\n", key, byte_offset_1, byte_offset_2, fanout );
	//FIXME move this into edge_bytes if edge program relies on edge weight
	V edgeval = mp_edge_program(key, val, fanout);
	//K last_neighbor = 0;
	for ( uint64_t i = 0; i < fanout; i++ ) {
		uint64_t edge_offset = byte_offset_1+(i*edge_element_bytes);
		if ( edge_offset < m_edge_buffer_offset || edge_offset + edge_element_bytes > m_edge_buffer_offset+m_edge_buffer_bytes ) {
			size_t byte_offset_aligned = edge_offset&(~0x3ff); // 1 KB alignment
			pread(m_matrix_fd, mp_edge_buffer, m_buffer_alloc_bytes, byte_offset_aligned);
			m_edge_buffer_offset = byte_offset_aligned;
			m_edge_buffer_bytes = m_buffer_alloc_bytes;
		}
		size_t internal_offset = edge_offset - m_edge_buffer_offset;
		//FIXME if the matrix format changes
		K neighbor = *((K*)(((uint8_t*)mp_edge_buffer)+internal_offset));
		//printf( "Dst vertex %x -- %x\n", neighbor, edgeval );
		/*
		if ( last_neighbor > neighbor ) {
			printf( "Dst vertex %x\n", neighbor );
		}
		last_neighbor = neighbor;
		*/
		if ( write ) while (!mp_sr_ep->Update(neighbor, edgeval)) usleep(100);

		m_edge_blocks_read++;
	}

}
template <class K, class V>
void
EdgeProcess<K,V>::WorkerThread(int idx) {
	//printf( "EdgeProcess WorkerThread %d started\n", idx );

	size_t idx_buffer_offset = 0;
	size_t idx_buffer_bytes = 0;
	void* idx_buffer = aligned_alloc(512, m_buffer_alloc_bytes);
	//mp_edge_buffer = malloc(m_buffer_alloc_bytes);

	size_t edge_buffer_offset = 0;
	size_t edge_buffer_bytes = 0;
	void* edge_buffer = aligned_alloc(512, m_buffer_alloc_bytes);
	typename SortReduce<K,V>::IoEndpoint* ep = ma_sr_ep[idx];

	while ( !m_kill_threads || mq_req_blocks->size() > 0 ) {
		SortReduceTypes::Block block = mq_req_blocks->get();
		if ( !block.valid ) {
			usleep(1000);
			continue;
		}
		KvPair* buf = (KvPair*)block.buffer;
		size_t size = block.valid_bytes;
		size_t cnt = size/sizeof(KvPair);


		size_t edge_element_bytes = sizeof(K);
		for ( size_t i = 0; i < cnt; i++ ) { 
			KvPair kvp = buf[i];
			K key = kvp.key;
			V val = kvp.val;

			size_t byte_offset = ((size_t)key)*sizeof(uint64_t);
			if ( byte_offset < idx_buffer_offset || byte_offset + 2*sizeof(uint64_t) > idx_buffer_offset+idx_buffer_bytes ) {
				size_t byte_offset_aligned = byte_offset&(~0x3ff); // 1 KB alignment
				pread(m_ridx_fd, idx_buffer, m_buffer_alloc_bytes, byte_offset_aligned);
				idx_buffer_offset = byte_offset_aligned;
				idx_buffer_bytes = m_buffer_alloc_bytes;
				//printf( "Read new %lx %lx -- %s\n", ret, ((uint64_t*)mp_idx_buffer)[4], strerror(errno ));
				m_index_blocks_read ++;
			}
			size_t internal_offset = byte_offset - idx_buffer_offset;

			uint64_t byte_offset_1 = *((uint64_t*)(((uint8_t*)idx_buffer)+internal_offset));
			uint64_t byte_offset_2 = *((uint64_t*)(((uint8_t*)idx_buffer)+internal_offset+sizeof(uint64_t)));
			uint64_t edge_bytes = byte_offset_2 - byte_offset_1;
			uint32_t fanout = (uint32_t)(edge_bytes/edge_element_bytes);

			//printf( "Source vertex %x -- %lx %lx fanout %x\n", key, byte_offset_1, byte_offset_2, fanout );
			//FIXME move this into edge_bytes if edge program relies on edge weight
			V edgeval = mp_edge_program(key, val, fanout);
			//K last_neighbor = 0;
			for ( uint64_t i = 0; i < fanout; i++ ) {
				uint64_t edge_offset = byte_offset_1+(i*edge_element_bytes);
				if ( edge_offset < edge_buffer_offset || edge_offset + edge_element_bytes > edge_buffer_offset+edge_buffer_bytes ) {
					size_t byte_offset_aligned = edge_offset&(~0x3ff); // 1 KB alignment
					pread(m_matrix_fd, edge_buffer, m_buffer_alloc_bytes, byte_offset_aligned);
					edge_buffer_offset = byte_offset_aligned;
					edge_buffer_bytes = m_buffer_alloc_bytes;

					m_edge_blocks_read++;
				}
				size_t internal_offset = edge_offset - edge_buffer_offset;
				//FIXME if the matrix format changes
				K neighbor = *((K*)(((uint8_t*)edge_buffer)+internal_offset));
				//printf( "Dst vertex %x -- %x\n", neighbor, edgeval );
				/*
				if ( last_neighbor > neighbor ) {
					printf( "Dst vertex %x\n", neighbor );
				}
				last_neighbor = neighbor;
				*/
				while (!ep->Update(neighbor, edgeval)) usleep(100);
			}
		}

		mq_free_block->push(block);
	}

	ep->Finish();

	free(idx_buffer);
	free(edge_buffer);

	//printf( "EdgeProcess WorkerThread %d ending\n", idx );
}


TEMPLATE_EXPLICIT_INSTANTIATION(EdgeProcess)
