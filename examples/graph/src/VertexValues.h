#ifndef __VERTEXSOURCE_H__
#define __VERTEXSOURCE_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>

#include "types.h"

template <class K, class V>
class VertexValues {
public:
	VertexValues(std::string temp_directory, K key_count, V default_value,bool(*isactive)(V,V,bool));
	~VertexValues();
	bool Update(K key, V val);
	void NextIteration();
	size_t GetActiveCount() { return m_active_cnt; };
	//TODO:Mark all vertices
	int OpenActiveFile(uint32_t iteration);
private:

	bool (*mp_is_active)(V,V,bool) = NULL;

	typedef struct __attribute__ ((__packed__)) {
		uint32_t iteration;
		V val;
	} ValueItem;

	V m_default_value;


	void* mp_io_buffer = NULL;
	size_t m_io_buffer_offset = 0;
	size_t m_io_buffer_bytes = 0;
	bool m_io_buffer_dirty = false;
	static const size_t m_io_buffer_alloc_size = (1024*1024);
	
	std::string m_temp_directory;
	int m_vertex_data_fd;
	int m_active_vertices_fd;

	uint32_t m_cur_iteration = 0;
	size_t m_active_cnt = 0;

};

#endif
