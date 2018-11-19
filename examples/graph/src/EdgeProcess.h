#ifndef __EDGEPROCESS_H__
#define __EDGEPROCESS_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <errno.h>

#include <string>

#include "types.h"
#include "sortreduce.h"

template <class K, class V>
class EdgeProcess {
public:
	EdgeProcess(std::string ridx_path, std::string matrix_path, V(*edge_program)(K,V,K));
	void SetSortReduceEndpoint(SortReduce<uint32_t,uint32_t>::IoEndpoint* ep);
	void SourceVertex(K key, V val);
private:
	V (*mp_edge_program)(K,V,K) = NULL;
	int m_ridx_fd;
	int m_matrix_fd;

	size_t m_idx_buffer_offset;
	size_t m_idx_buffer_bytes;
	void* mp_idx_buffer;

	size_t m_edge_buffer_offset;
	size_t m_edge_buffer_bytes;
	void* mp_edge_buffer;
	static const size_t m_buffer_alloc_bytes = (1024*1024);
	SortReduce<uint32_t,uint32_t>::IoEndpoint* m_sr_ep;

/*
	typedef struct __attribute__ ((__packed__)) {
		uint32_t iteration;
		V val;
	} ValueItem;
*/
};

#endif
