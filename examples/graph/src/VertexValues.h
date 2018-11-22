#ifndef __VERTEXSOURCE_H__
#define __VERTEXSOURCE_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <mutex>
#include <atomic>

#include "types.h"
#include "utils.h"

#define MAX_VERTEXVAL_REQS 16
#define MAX_VERTEXVAL_THREADS 8

template <class K, class V>
class VertexValues {
public:
	VertexValues(std::string temp_directory, K key_count, V default_value,bool(*isactive)(V,V,bool), int thread_count = 0);
	~VertexValues();
	void Start();
	bool Update(K key, V val);
	void NextIteration();
	size_t GetActiveCount();
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
	void* mp_active_buffer = NULL;
	size_t m_active_buffer_idx = 0;
	size_t m_io_buffer_offset = 0;
	size_t m_io_buffer_bytes = 0;
	bool m_io_buffer_dirty = false;
	static const size_t m_io_buffer_alloc_items = 1024*32;
	static const size_t m_io_buffer_alloc_size = (m_io_buffer_alloc_items*sizeof(ValueItem));
	
	std::string m_temp_directory;
	int m_vertex_data_fd;
	int m_active_vertices_fd;

	uint32_t m_cur_iteration = 0;
	size_t m_active_cnt = 0;
	size_t ma_active_count[MAX_VERTEXVAL_THREADS];
	
private:
	int m_max_thread_count = 0;
	int m_cur_thread_count = 0;

	std::atomic<uint64_t> m_next_buffer_write_order;
	std::atomic<uint64_t> m_cur_buffer_write_order;

	std::thread ma_worker_thread[MAX_VERTEXVAL_THREADS];
	std::mutex m_mutex;
	bool m_kill_threads;

	SortReduceTypes::Block m_cur_out_block;
	size_t m_cur_out_idx;
	
	SortReduceTypes::Block ma_cur_out_block[MAX_VERTEXVAL_THREADS];
	size_t ma_cur_out_idx[MAX_VERTEXVAL_THREADS];
	SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* maq_req_blocks[MAX_VERTEXVAL_THREADS];
	SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* mq_free_block;
	void WorkerThread(int i);
	typedef struct __attribute__ ((__packed__)) {
		K key;
		V val;
	} KvPair;

};

#endif
