#include "VertexValues.h"

template <class K, class V>
VertexValues<K,V>::VertexValues(std::string temp_directory, K key_count, V default_value, bool(*isactive)(V,V,bool)) {

	m_cur_iteration = 0;

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
	printf( "Created vertex values file at %s\n", tmp_filename );


	mp_io_buffer = malloc(m_io_buffer_alloc_size);

	m_active_vertices_fd = 0;
}

template <class K, class V>
void
VertexValues<K,V>::NextIteration() {

	if ( m_active_vertices_fd != 0 ) {
		close(m_active_vertices_fd);
		m_active_vertices_fd = 0;
	
		//char filename[128];
		//sprintf(filename, "%s/vertex_%d.dat", m_temp_directory.c_str(), m_cur_iteration );
		//unlink(filename);
	}
	
	m_cur_iteration++;
}

template <class K, class V>
inline bool
VertexValues<K,V>::Update(K key, V val){
	size_t byte_offset = sizeof(ValueItem)*((size_t)key);
	if ( byte_offset < m_io_buffer_offset || byte_offset+sizeof(ValueItem) > m_io_buffer_offset+m_io_buffer_bytes ) {
		if ( m_io_buffer_dirty ) {
			// write it back to disk
			lseek(m_vertex_data_fd, m_io_buffer_offset, SEEK_SET);
			write(m_vertex_data_fd, mp_io_buffer, m_io_buffer_bytes);
			m_io_buffer_bytes = 0;

			printf( "Wrote dirty buffer at %x\n", m_io_buffer_offset );
		} 
		size_t byte_offset_aligned = byte_offset&(~0x3ff); // 1 KB alignment
		pread(m_vertex_data_fd, mp_io_buffer, m_io_buffer_alloc_size, byte_offset_aligned);
		m_io_buffer_offset = byte_offset_aligned;
		m_io_buffer_bytes = m_io_buffer_alloc_size;
		m_io_buffer_dirty = false;

		printf( "Read new buffer at %x\n", byte_offset_aligned );
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

		if ( m_active_vertices_fd == 0 ) {
			char filename[128];
			sprintf(filename, "%s/vertex_%d.dat", m_temp_directory.c_str(), m_cur_iteration );
			m_active_vertices_fd = open(filename, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
			lseek(m_active_vertices_fd, 0, SEEK_SET);
		}

		write(m_active_vertices_fd, &key, sizeof(K));

		printf( "Active vertex %x\n", key );
	}

	return is_active;
}

TEMPLATE_EXPLICIT_INSTANTIATION(VertexValues)
