#ifndef __FILE_KV_READER__
#define __FILE_KV_READER__

#include <string>
#include <tuple>

#include "alignedbuffermanager.h"
#include "tempfilemanager.h"
#include "types.h"

namespace SortReduceUtils {

template <class K, class V>
class FileKvReader {
public:
	FileKvReader(SortReduceTypes::File* file, std::string path);
	~FileKvReader();

	void Rewind();
	void Seek(size_t idx);
	std::tuple<K,V, bool> Next();


private:
	FileKvReader();

	TempFileManager* mp_temp_file_manager;
	SortReduceTypes::File* mp_file;

	AlignedBufferManager* mp_buffer_manager = NULL;

	size_t m_offset;
};
}


#endif
