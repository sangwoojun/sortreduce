#include "filekvreader.h"

template <class K, class V>
SortReduceUtils::FileKvReader<K,V>::FileKvReader(SortReduceTypes::File* file, std::string path) {
	this->mp_file = file;

	// TempFileManager technically does not require path right now
	// (we're not going to create new files)
	// but here just for completeness sake
	this->mp_temp_file_manager = new TempFileManager(path);

	mp_buffer_manager = AlignedBufferManager::GetInstance(1);

	this->m_offset = 0;

	//mp_buffer_manager
}

template <class K, class V>
SortReduceUtils::FileKvReader<K,V>::~FileKvReader() {
	delete this->mp_temp_file_manager;
}

template <class K, class V>
void 
SortReduceUtils::FileKvReader<K,V>::Rewind() {
}

template <class K, class V>
void 
SortReduceUtils::FileKvReader<K,V>::Seek(size_t idx) {
}


template <class K, class V>
std::tuple<K,V, bool> 
SortReduceUtils::FileKvReader<K,V>::Next() {
	std::tuple<K,V,bool> ret = std::make_tuple(0,0,false);

	return ret;
}




template class SortReduceUtils::FileKvReader<uint32_t, uint32_t>;
template class SortReduceUtils::FileKvReader<uint32_t, uint64_t>;
template class SortReduceUtils::FileKvReader<uint64_t, uint32_t>;
template class SortReduceUtils::FileKvReader<uint64_t, uint64_t>;

