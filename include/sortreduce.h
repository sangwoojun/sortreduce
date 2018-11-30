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

#include <sys/statvfs.h>

#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <chrono>

#include "alignedbuffermanager.h"
#include "blocksorter.h"
#include "filekvreader.h"
#include "reducer.h"
#include "mergereducer_multitree.h"
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
	bool Update(K key, V val);
	void Finish();
	std::tuple<K,V,bool> Next();
	
	SortReduceTypes::Status CheckStatus();

	void CheckInputDone();

public:
	class IoEndpoint {
	public:
		IoEndpoint(SortReduce<K,V>* sr, bool input_only);
		bool Update(K key, V val);
		void Finish();
		std::tuple<K,V,bool> Next();

		bool IsDone() { return m_done; };
private:
		SortReduce<K,V>* mp_sortreduce;
	
		SortReduceTypes::Block m_cur_update_block;
		size_t m_cur_update_offset;

		// Used only for data injection.
		// Next() does nothing
		bool m_input_only;

		bool m_done;
	};
	IoEndpoint* GetEndpoint(bool input_only);
	std::vector<IoEndpoint*> mv_endpoints;

private:
	SortReduceTypes::Block m_cur_update_block;
	size_t m_cur_update_offset;


private:
	SortReduceTypes::Config<K,V>* m_config;

	BlockSorter<K,V>* mp_block_sorter;

	std::thread manager_thread;
	std::mutex m_mutex;
	void ManagerThread();

	int m_maximum_threads;

	SortReduceUtils::MutexedQueue<SortReduceTypes::File>* mq_temp_files;

	bool m_done_input;
	bool m_done_input_main;
	bool m_done_inmem;
	bool m_done_external;

	bool m_reduce_phase;

	std::vector<SortReduceReducer::MergeReducer<K,V>*> mv_stream_mergers_from_mem;
	std::vector<SortReduceReducer::MergeReducer<K,V>*> mv_stream_mergers_from_storage;

	std::priority_queue<SortReduceTypes::File*,std::vector<SortReduceTypes::File*>, SortReduceTypes::CompareFileSize> m_file_priority_queue;

	SortReduceUtils::FileKvReader<K,V>* mp_file_kv_reader = NULL;
	SortReduceReducer::BlockSourceReader<K,V>* mp_result_stream_reader = NULL;


	SortReduceTypes::File* mp_output_file = NULL;
public:
	SortReduceTypes::File* GetOutFile() {return mp_output_file;};
	std::queue<SortReduceReducer::MergeReducer<K,V>*> mq_delayed_dete_mergereducer;
};

#endif
