#ifndef __REDUCER_MULTITREE_H__
#define __REDUCER_MULTITREE_H__

#include <mutex>
#include <vector>
#include <tuple>
#include <chrono>

#include "reducer.h"
#include "mergereducer_accel.h"

#include "alignedbuffermanager.h"
#include "tempfilemanager.h"
#include "types.h"
#include "utils.h"

namespace SortReduceReducer {
	template <class K, class V>
	class MergeReducer_MultiTree : public MergeReducer<K,V> {
	public:
		MergeReducer_MultiTree(V (*update)(V,V), std::string temp_directory, int maximum_threads, std::string filename = "", bool verbose = false);
		~MergeReducer_MultiTree();

		void PutBlock(SortReduceTypes::Block block);
		void PutFile(SortReduceTypes::File* file);
		void Start(); // returns the number of threads
		bool IsDone();

		SortReduceTypes::File* GetOutFile();

		size_t GetInputFileBytes() { return this->m_input_file_bytes; };
		int GetThreadCount() { return m_thread_count; };
		bool AcceleratorAvailable();
		void UserAccelerator(bool val) { m_use_accelerator = val;};


		//for ReducerNodeStream
		BlockSourceReader<K,V>* GetResultReader();
	private:

		bool m_started;
		bool m_done;

		V (*mp_update)(V,V);

		int m_maximum_threads;
		int m_thread_count;

		bool m_use_accelerator;



		StreamFileReader* mp_stream_file_reader;
		int m_file_input_cnt;
		std::vector<BlockSource<K,V>*> mv_src_reader;
		std::vector<std::vector<BlockSource<K,V>*>> mvv_tree_nodes;
		std::vector<BlockSource<K,V>*> mv_tree_nodes_seq; // for easy deleting

		std::string m_temp_directory;
		std::string m_filename;
		ReducerNode<K,V>* mp_reducer_node_to_file;
		ReducerNodeStream<K,V>* mp_reducer_node_stream;
		MergerNodeAccel<K,V>* mp_merger_accel;

		BlockSourceReader<K,V>* mp_block_source_reader;

		SortReduceTypes::File* mp_out_file;
		size_t m_input_file_bytes = 0;

	};
}

#endif
