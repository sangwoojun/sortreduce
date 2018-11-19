#include "EdgeProcess.h"

template <class K, class V>
EdgeProcess<K,V>::EdgeProcess(std::string ridx_path, std::string matrix_path, V(*edge_program)(K,V,K)) {
	m_idx_buffer_bytes = 0;
	m_edge_buffer_bytes = 0;

	mp_edge_program = edge_program;
	m_ridx_fd = open(ridx_path.c_str(), O_RDONLY, S_IRUSR|S_IWUSR);
	m_matrix_fd = open(matrix_path.c_str(), O_RDONLY, S_IRUSR|S_IWUSR);
	if ( m_ridx_fd < 0 || m_matrix_fd < 0 ) {
		fprintf(stderr, "ERROR: Graph ridx or matrix file not found\n" );
	}
	mp_idx_buffer = malloc(m_buffer_alloc_bytes);
	mp_edge_buffer = malloc(m_buffer_alloc_bytes);
}

template <class K, class V>
void 
EdgeProcess<K,V>::SetSortReduceEndpoint(SortReduce<uint32_t,uint32_t>::IoEndpoint* ep) {
	m_sr_ep = ep;
}

template <class K, class V>
inline void
EdgeProcess<K,V>::SourceVertex(K key, V val) {
	size_t edge_element_bytes = sizeof(K);
	size_t byte_offset = ((size_t)key)*sizeof(uint64_t);
	if ( byte_offset < m_idx_buffer_offset || byte_offset + 2*sizeof(uint64_t) > m_idx_buffer_offset+m_idx_buffer_bytes ) {
		size_t byte_offset_aligned = byte_offset&(~0x3ff); // 1 KB alignment
		pread(m_ridx_fd, mp_idx_buffer, m_buffer_alloc_bytes, byte_offset_aligned);
		m_idx_buffer_offset = byte_offset_aligned;
		m_idx_buffer_bytes = m_buffer_alloc_bytes;
		//printf( "Read new %lx %lx -- %s\n", ret, ((uint64_t*)mp_idx_buffer)[4], strerror(errno ));
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
		while (!m_sr_ep->Update(neighbor, edgeval));
	}





}

TEMPLATE_EXPLICIT_INSTANTIATION(EdgeProcess)
