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

class SortReduce {
public:
	typedef enum {
		MERGER_2TO1,
		MERGER_16TO1,
	} MergerType;


	class Status {
	public:
		Status();
		bool done;
	private:
	};


public:
	SortReduce(SortReduceTypes::Config* config);
	~SortReduce();
	bool PutBlock(void* buffer, size_t bytes);
	size_t GetBlock(void* buffer);
	SortReduce::Status CheckStatus();
private:
	SortReduceTypes::Config* m_config;

	BlockSorter* mp_block_sorter;

	std::thread manager_thread;
	void ManagerThread();

	SortReduceUtils::MutexedQueue<SortReduceTypes::File>* mq_temp_files;

};

#endif
