#ifndef __SORTREDUCE_H__
#define __SORTREDUCE_H__

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include <fcntl.h>
#include <libaio.h>

#include <time.h>

#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "alignedbuffermanager.h"
#include "blocksorter.h"
#include "filekvreader.h"
#include "reducer.h"
#include "types.h"
#include "utils.h"

template <class K, class V>
class SortReduce {
public:
	SortReduce(SortReduceTypes::Config<K,V>* config);
	~SortReduce();
	bool PutBlock(void* buffer, size_t bytes, bool last);
	
	SortReduceTypes::Block GetFreeManagedBlock();
	void PutManagedBlock(SortReduceTypes::Block block);

public:
	//write to m_cur_update_block until it's full
	//returns false if no remaining buffers
	bool Update(K key, V val, bool last);
	std::tuple<K,V,bool> Next();
	
	SortReduceTypes::Status CheckStatus();

public:
	class IoEndpoint {
	public:
		IoEndpoint(SortReduce<K,V>* sr);
		bool Update(K key, V val);
		void Finish();
		std::tuple<K,V,bool> Next();
	private:
		SortReduce<K,V>* mp_sortreduce;
	
		SortReduceTypes::Block m_cur_update_block;
		size_t m_cur_update_offset;

		bool m_done;
	};
	IoEndpoint* GetEndpoint();
	std::vector<IoEndpoint*> mv_endpoints;

private:
	SortReduceTypes::Block m_cur_update_block;
	size_t m_cur_update_offset;

private:
	SortReduceTypes::Config<K,V>* m_config;

	BlockSorter<K,V>* mp_block_sorter;

	std::thread manager_thread;
	void ManagerThread();

	SortReduceUtils::MutexedQueue<SortReduceTypes::File>* mq_temp_files;

	bool m_done_input;
	bool m_done_inmem;
	bool m_done_external;

	std::vector<SortReduceReducer::StreamMergeReducer<K,V>*> mv_stream_mergers_from_mem;
	std::vector<SortReduceReducer::StreamMergeReducer<K,V>*> mv_stream_mergers_from_storage;

	std::priority_queue<SortReduceTypes::File*,std::vector<SortReduceTypes::File*>, SortReduceTypes::CompareFileSize> m_file_priority_queue;

	SortReduceUtils::FileKvReader<K,V>* mp_file_kv_reader = NULL;

};

#endif
