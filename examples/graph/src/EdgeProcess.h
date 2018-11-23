#ifndef __EDGEPROCESS_H__
#define __EDGEPROCESS_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <libaio.h>

#include <errno.h>

#include <string>
#include <thread>
#include <queue>
#include <atomic>

#include "types.h"
#include "utils.h"
#include "sortreduce.h"

#define MAX_EDGEPROC_REQS 16
#define MAX_EDGEPROC_THREADS 8

template <class K, class V>
class EdgeProcess {
public:
	EdgeProcess(std::string ridx_path, std::string matrix_path, V(*edge_program)(K,V,K));
	~EdgeProcess();
	size_t GetVertexCount();
	void SetSortReduceEndpoint(typename SortReduce<K,V>::IoEndpoint* ep);
	void AddSortReduceEndpoint(typename SortReduce<K,V>::IoEndpoint* ep);
	void SourceVertex(K key, V val, bool write);
	void Start();
	void Finish();
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
	static const size_t m_buffer_alloc_bytes = (1024*512);
	typename SortReduce<K,V>::IoEndpoint* mp_sr_ep;


private:
	typename SortReduce<K,V>::IoEndpoint* ma_sr_ep[MAX_EDGEPROC_REQS];
	std::thread ma_sr_thread[MAX_EDGEPROC_REQS];
	int m_thread_count;
	int m_ep_count;
	bool m_kill_threads;

	SortReduceTypes::Block m_cur_out_block;
	size_t m_cur_out_idx;

	SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* mq_req_blocks;
	SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* mq_free_block;
	static const size_t m_block_bytes = (1024*1024*8);

	void WorkerThread(int i);
	
	//typedef struct __attribute__ ((__packed__)) {
	typedef struct {
		K key;
		V val;
	} KvPair;

private:
	std::atomic<size_t> m_index_blocks_read;
	std::atomic<size_t> m_edge_blocks_read;

};

#endif
