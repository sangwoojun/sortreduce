#ifndef __UTILS_H__
#define __UTILS_H__

#include <queue>
#include <mutex>

#include <time.h>

#include "types.h"

class BufferQueueInOut {
private:
	std::mutex in_mutex;
	std::mutex out_mutex;
	std::queue<SortReduceTypes::Block> in_queue;
	std::queue<SortReduceTypes::Block> out_queue;
public:
	void enq_in(void* buffer, size_t bytes);
	size_t deq_in(void** buffer);
	int in_count();
	void enq_out(void* buffer, size_t bytes);
	size_t deq_out(void** buffer);
	int out_count();
};

class SortReduceUtils {
public:
	static double TimespecDiffSec( timespec start, timespec end );
};

#endif
