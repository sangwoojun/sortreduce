#ifndef __REDUCER_MULTITREE_H__
#define __REDUCER_MULTITREE_H__

#include <mutex>
#include <vector>
#include <tuple>
#include <chrono>

#include "reducer.h"

#include "alignedbuffermanager.h"
#include "tempfilemanager.h"
#include "types.h"
#include "utils.h"

namespace SortReduceReducer {
	template <class K, class V>
	class MergeReducer_MultiTree : public MergeReducer<K,V> {
	public:
		MergeReducer_MultiTree(V (*update)(V,V), std::string temp_directory, std::string filename = "", bool verbose = false);
		~MergeReducer_MultiTree();

		void PutBlock(SortReduceTypes::Block block);
		void PutFile(SortReduceTypes::File* file);
		void Start();
		bool IsDone();

		SortReduceTypes::File* GetOutFile() {return this->mp_reducer_node->GetOutFile(); };

		size_t GetInputFileBytes() { return this->m_input_file_bytes; };
	private:

		bool m_started;
		bool m_done;

		StreamFileReader* mp_stream_file_reader;
		std::vector<BlockSource<K,V>*> mv_file_reader;
		std::vector<std::vector<BlockSource<K,V>*>> mvv_tree_nodes;
		std::vector<BlockSourceNode<K,V>*> mv_tree_nodes_seq; // for easy deleting

		ReducerNode<K,V>* mp_reducer_node;

		SortReduceTypes::File* mp_out_file;
		size_t m_input_file_bytes = 0;

	};
}

#endif
