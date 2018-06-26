#ifndef __BLOCKSORT_H__
#define __BLOCKSORT_H__

#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <algorithm>

#include <time.h>

#include "types.h"
#include "utils.h"

class BlockSorterThread {
public:
	BlockSorterThread(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, BufferQueueInOut* buffer_queue);
	void Exit();

	typedef struct __attribute__ ((__packed__)) {uint32_t key; uint32_t val;} tK32_V32;
	typedef struct __attribute__ ((__packed__)) {uint32_t key; uint64_t val;} tK32_V64;
	typedef struct __attribute__ ((__packed__)) {uint64_t key; uint32_t val;} tK64_V32;
	typedef struct __attribute__ ((__packed__)) {uint64_t key; uint64_t val;} tK64_V64;
private:
	void SorterThread();
	std::thread m_thread;
	bool m_exit;

	SortReduceTypes::KeyType m_key_type;
	SortReduceTypes::ValType m_val_type;

	template <class tKV>
	static void SortKV(void* buffer, size_t bytes);

	template <class tKV>
	static bool CompareKV(tKV a, tKV b);


	BufferQueueInOut* m_buffer_queue;
};

class BlockSorter {
public:
	BlockSorter(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, int max_threads);
	void PutBlock(void* buffer, size_t bytes);
	size_t GetBlock(void* buffer);
	void CheckSpawnThreads();

private:
	size_t m_maximum_threads;
	int m_in_queue_spawn_limit_blocks;
	int m_out_queue_spawn_limit_blocks;
	std::vector<BlockSorterThread*> mv_sorter_threads;
	timespec m_last_thread_check_time;

	SortReduceTypes::KeyType m_key_type;
	SortReduceTypes::ValType m_val_type;
	
	BufferQueueInOut* m_buffer_queue;

};

#endif
