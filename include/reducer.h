#ifndef __REDUCER_H__
#define __REDUCER_H__

#include "types.h"

namespace SortReduceReducer {
	template <class K, class V>
	size_t ReduceInBuffer(V (*update)(V,V), void* buffer, size_t bytes);

	template <class K, class V>
	class StreamMergeReducer {
	public:
		virtual ~StreamMergeReducer() = 0;
		virtual int GetFreeSrc() = 0;
		virtual bool PutBlock(SortReduceTypes::Block block, int src) = 0;
		virtual SortReduceTypes::Block GetBlock() = 0;
	private:
	};

	template <class K, class V>
	class StreamMergeReducer_SinglePriority : StreamMergeReducer<K,V> {
	public:
		StreamMergeReducer_SinglePriority(int src_count, V (*update)(V,V));
		~StreamMergeReducer_SinglePriority();
		bool PutBlock(SortReduceTypes::Block block, int src);
		int GetFreeSrc();
		SortReduceTypes::Block GetBlock();
	private:
		typedef struct {
			K key;
			V val;
			int src;
		} KvPair;
		class CompareKv {
		public:
			bool operator() (KvPair a, KvPair b)
			{
				return (a.key < b.key);
			}
		};
		std::priority_queue<KvPair,std::vector<KvPair>, CompareKv> m_priority_queue;
	};

}

#endif
