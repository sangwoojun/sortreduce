#ifndef __REDUCER_H__
#define __REDUCER_H__

#include "types.h"

namespace SortReduceReducer {
	template <class K, class V>
	size_t ReduceInBuffer(V (*update)(V,V), void* buffer, size_t bytes);
}

#endif
