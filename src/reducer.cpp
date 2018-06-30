#include "reducer.h"
#include "utils.h"
	
template <class K, class V>
size_t 
SortReduceReducer::ReduceInBuffer(V (*update)(V,V), void* buffer, size_t bytes) {
	printf( "Reducing!! %s:%d\n", __FILE__, __LINE__ );
	size_t key_bytes = sizeof(K);
	size_t val_bytes = sizeof(V);
	size_t in_offset = 0;
	size_t out_offset = key_bytes+val_bytes;
	while ( in_offset < bytes ) {
		K in_key = *(K*)(((uint8_t*)buffer)+in_offset);
		K out_key = *(K*)(((uint8_t*)buffer)+out_offset);
		if ( in_key == out_key ) {
			V* p_in_val = (V*)(((uint8_t*)buffer)+in_offset+key_bytes);
			V in_val = *p_in_val;
			V out_val = *(V*)(((uint8_t*)buffer)+out_offset+key_bytes);
			*p_in_val = update(in_val,out_val);
		} else {
			out_offset += key_bytes+val_bytes;
		}
		in_offset += key_bytes+val_bytes;
	}
	return out_offset;
}

template size_t SortReduceReducer::ReduceInBuffer<uint32_t, uint32_t>(uint32_t (*update)(uint32_t,uint32_t), void* buffer, size_t bytes);
template size_t SortReduceReducer::ReduceInBuffer<uint32_t, uint64_t>(uint64_t (*update)(uint64_t,uint64_t), void* buffer, size_t bytes);
template size_t SortReduceReducer::ReduceInBuffer<uint64_t, uint32_t>(uint32_t (*update)(uint32_t,uint32_t), void* buffer, size_t bytes);
template size_t SortReduceReducer::ReduceInBuffer<uint64_t, uint64_t>(uint64_t (*update)(uint64_t,uint64_t), void* buffer, size_t bytes);

template <class K, class V>
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::StreamMergeReducer_SinglePriority(int src_count, V (*update)(V,V)) {
}

template <class K, class V>
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::~StreamMergeReducer_SinglePriority() {
}

template <class K, class V>
bool
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::PutBlock(
	SortReduceTypes::Block block, int src) {

	return false;
}

template <class K, class V>
int
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::GetFreeSrc() {
	return 0;
}

template <class K, class V>
SortReduceTypes::Block
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::GetBlock() {
	SortReduceTypes::Block ret;
	return ret;
}


//template size_t SortReduceReducer::ReduceInBuffer<uint32_t,uint32_t>(uint32_t (*update)(uint32_t,uint32_t), void* buffer, size_t bytes);
