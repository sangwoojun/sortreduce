#ifndef __SORTREDUCE_H__
#define __SORTREDUCE_H__

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

#include <fcntl.h>
#include <libaio.h>

#include <time.h>

#include <queue>
#include <string>
#include <thread>

#include "blocksorter.h"
#include "reducer.h"
#include "types.h"
#include "utils.h"

template <class K, class V>
class SortReduce {
public:



public:
	SortReduce(SortReduceTypes::Config<K,V>* config);
	~SortReduce();
	bool PutBlock(void* buffer, size_t bytes);
	size_t GetBlock(void* buffer);
	SortReduceTypes::Status CheckStatus();

public:
	//write to m_cur_update_block until it's full
	//returns false if no remaining buffers
	bool Update(K key, V val);
private:
	SortReduceTypes::Block m_cur_update_block;
	size_t m_cur_update_offset;

private:
	SortReduceTypes::Config<K,V>* m_config;

	BlockSorter<K,V>* mp_block_sorter;

	std::thread manager_thread;
	void ManagerThread();

	SortReduceUtils::MutexedQueue<SortReduceTypes::File>* mq_temp_files;

};

#endif
