#include "filekvreader.h"

template <class K, class V>
SortReduceUtils::FileKvReader<K,V>::FileKvReader(SortReduceTypes::File* file, SortReduceTypes::Config<K,V>* config) {
	this->mp_file = file;

	// TempFileManager technically does not require path right now
	// (we're not going to create new files)
	// but here just for completeness sake
	this->mp_temp_file_manager = new TempFileManager(config->temporary_directory);

	mp_buffer_manager = AlignedBufferManager::GetInstance(1);

	this->m_offset = 0;
	this->m_file_size = file->bytes;
	printf( "Loading file %s size %lu\n", config->output_filename.c_str(), m_file_size );

	// temp
	this->mp_fp = fopen((config->temporary_directory+config->output_filename).c_str(), "rb");

}

template <class K, class V>
SortReduceUtils::FileKvReader<K,V>::~FileKvReader() {
	delete this->mp_temp_file_manager;
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
std::tuple<K,V, bool> 
SortReduceUtils::FileKvReader<K,V>::Next() {
	if ( m_offset >= m_file_size ) return std::make_tuple(0,0,false);
	
	K key = 0;
	V val = 0;
	fread(&key, sizeof(K), 1, mp_fp);
	fread(&val, sizeof(V), 1, mp_fp);
	m_offset += sizeof(K)+sizeof(V);

	return std::make_tuple(key,val,true);

	//printf( "!!! %lx %x %lu %lu\n", key, val, sizeof(K), sizeof(V) );
}



TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceUtils::FileKvReader)
