#include "filekvreader.h"

template <class K, class V>
SortReduceUtils::FileKvReader<K,V>::FileKvReader(SortReduceTypes::File* file, SortReduceTypes::Config<K,V>* config) {
	this->m_offset = 0;
	this->m_file_size = file->bytes;

	// temp
	this->m_fd = open((config->temporary_directory+"/"+config->output_filename).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);
	printf( "Loading file %s size %lu -- %d\n", config->output_filename.c_str(), m_file_size, m_fd ); fflush(stdout);

	m_buffer_offset = 0;
	m_buffer_bytes = 0;
	mp_read_buffer = malloc(m_buffer_alloc_bytes);
	m_last_key = 0;
}
template <class K, class V>
SortReduceUtils::FileKvReader<K,V>::FileKvReader(std::string filename, SortReduceTypes::Config<K,V>* config, size_t bytes) {
	this->m_offset = 0;
	this->m_fd = open((config->temporary_directory+"/"+filename).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);

	if ( bytes > 0 ) {
		this->m_file_size = bytes;
	} else {
		this->m_file_size = lseek(m_fd, 0, SEEK_END);
	}
	printf( "Loading file %s size %lu\n", filename.c_str(), m_file_size ); fflush(stdout);
	
	m_buffer_offset = 0;
	m_buffer_bytes = 0;
	mp_read_buffer = malloc(m_buffer_alloc_bytes);
	m_last_key = 0;
}

template <class K, class V>
SortReduceUtils::FileKvReader<K,V>::FileKvReader(int fd, size_t bytes) {
	this->m_offset = 0;
	this->m_fd = fd;
	if ( bytes > 0 ) {
		this->m_file_size = bytes;
	} else {
		this->m_file_size = lseek(m_fd, 0, SEEK_END);
	}
	printf( "Loading file fd %d size %lu\n", fd,  m_file_size ); fflush(stdout);
	
	m_buffer_offset = 0;
	m_buffer_bytes = 0;
	mp_read_buffer = malloc(m_buffer_alloc_bytes);
	m_last_key = 0;
}


template <class K, class V>
SortReduceUtils::FileKvReader<K,V>::~FileKvReader() {
	free(mp_read_buffer);
}

template <class K, class V>
void 
SortReduceUtils::FileKvReader<K,V>::Rewind() {
	//FIXME
}

template <class K, class V>
void 
SortReduceUtils::FileKvReader<K,V>::Seek(size_t idx) {
	//FIXME
}



template <class K, class V>
inline std::tuple<K,V, bool> 
SortReduceUtils::FileKvReader<K,V>::Next(bool advance) {
	if ( m_offset + sizeof(K)+sizeof(V) > m_file_size ) return std::make_tuple(0,0,false);

	if ( m_offset < m_buffer_offset || m_offset+sizeof(K)+sizeof(V) > m_buffer_offset+m_buffer_bytes ) {
		size_t byte_offset_aligned = m_offset&(~0x3ff); // 1 KB alignment
		pread(m_fd, mp_read_buffer, m_buffer_alloc_bytes, byte_offset_aligned);
		m_buffer_offset = byte_offset_aligned;
		m_buffer_bytes = m_buffer_alloc_bytes;
	}
	size_t internal_offset = m_offset - m_buffer_offset;
	K key = *((K*)(((uint8_t*)mp_read_buffer)+internal_offset));
	V val = *((V*)(((uint8_t*)mp_read_buffer)+internal_offset+sizeof(K)));
	if ( advance ) m_offset += sizeof(K)+sizeof(V);

/*
	if ( key < m_last_key ) {
		printf( "FileKvReader order wrong %lx %lx\n", m_last_key, key );
	}
	m_last_key = key;
*/
	
	return std::make_tuple(key,val,true);

	//printf( "!!! %lx %x %lu %lu\n", key, val, sizeof(K), sizeof(V) );
}



TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceUtils::FileKvReader)
