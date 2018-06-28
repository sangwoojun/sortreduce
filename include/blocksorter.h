#ifndef __BLOCKSORT_H__
#define __BLOCKSORT_H__

#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <algorithm>

#include <time.h>

#include "types.h"
#include "utils.h"
#include "tempfilemanager.h"
#include "reducer.h"

class BlockSorterThread {
public:
	BlockSorterThread(SortReduceTypes::Config* config, SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type,SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* buffer_queue, TempFileManager* file_manager, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, SortReduceTypes::ComponentStatus* status);
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


	SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* m_buffer_queue_in;

	TempFileManager* mp_file_manager;
	
	SortReduceUtils::MutexedQueue<SortReduceTypes::File>* mq_temp_files;

	SortReduceTypes::ComponentStatus* mp_status;
	SortReduceTypes::Config* mp_config;
};

class BlockSorter {
public:
	BlockSorter(SortReduceTypes::Config* config, SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, SortReduceUtils::MutexedQueue<SortReduceTypes::File>* temp_files, std::string temp_path, size_t buffer_size, int buffer_count, int max_threads);
	~BlockSorter();

	void PutBlock(void* buffer, size_t bytes);

	//size_t GetBlock(void* buffer);
	void CheckSpawnThreads();

	size_t BytesInFlight();


private:
	size_t m_maximum_threads;
	size_t m_in_queue_spawn_limit_blocks;
	size_t m_out_queue_spawn_limit_blocks;
	std::vector<BlockSorterThread*> mv_sorter_threads;
	timespec m_last_thread_check_time;

	SortReduceTypes::KeyType m_key_type;
	SortReduceTypes::ValType m_val_type;
	
	SortReduceUtils::MutexedQueue<SortReduceTypes::Block>* m_buffer_queue_in;

	TempFileManager* mp_temp_file_manager;
	SortReduceUtils::MutexedQueue<SortReduceTypes::File>* mq_temp_files;

	size_t m_buffer_size;
	int m_buffer_count;


	void** mpp_managed_buffers;
	std::queue<int> mq_free_managed_buffers;

	SortReduceTypes::ComponentStatus m_status;

	SortReduceTypes::Config* mp_config;
};

#endif
