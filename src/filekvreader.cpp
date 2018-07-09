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
	printf( "Loading file size %lu\n", m_file_size );

	// temp
	this->mp_fp = fopen(config->output_filename.c_str(), "rb");

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
	std::tuple<K,V,bool> ret = std::make_tuple(0,0,false);

	if ( m_offset >= m_file_size ) return ret;
	
	KvPair kvp;
	size_t sz = fread(&kvp, sizeof(KvPair), 1, mp_fp);
	if ( sz > 0 ) {
		K key = kvp.key;
		V val = kvp.val;
		ret = std::make_tuple(key,val,true);

		m_offset += sizeof(KvPair);
	}

	return ret;
}




template class SortReduceUtils::FileKvReader<uint32_t, uint32_t>;
template class SortReduceUtils::FileKvReader<uint32_t, uint64_t>;
template class SortReduceUtils::FileKvReader<uint64_t, uint32_t>;
template class SortReduceUtils::FileKvReader<uint64_t, uint64_t>;

