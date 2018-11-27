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
		FileKvReader(SortReduceTypes::File* file, SortReduceTypes::Config<K,V>* config);
		FileKvReader(std::string filename, SortReduceTypes::Config<K,V>* config, size_t bytes = 0);
		FileKvReader(int fd, size_t bytes = 0);
		~FileKvReader();

		void Rewind();
		void Seek(size_t idx);
		std::tuple<K,V, bool> Next(bool advance = true);

		typedef struct __attribute__ ((__packed__)) {K key; V val;} KvPair;

	private:
		FileKvReader();

		size_t m_offset;
		size_t m_file_size;

		// temp
		FILE* mp_fp;
		int m_fd;

		void* mp_read_buffer;
		size_t m_buffer_offset;
		size_t m_buffer_bytes;
		static const size_t m_buffer_alloc_bytes = (1024*1024*4);

		K m_last_key;

	};
}


#endif
