#include "utils.h"

void
BufferQueueInOut::enq_in(void* buffer, size_t bytes) {
	in_mutex.lock();
	SortReduceTypes::Block ib;
	ib.buffer = buffer;
	ib.bytes = bytes;
	in_queue.push(ib);
	in_mutex.unlock();
}

size_t 
BufferQueueInOut::deq_in (void** buffer) {
	size_t ret = 0;
	in_mutex.lock();
	if ( !in_queue.empty() ) {
		SortReduceTypes::Block b = in_queue.front();
		in_queue.pop();
		*buffer = b.buffer;
		ret = b.bytes;
	}
	in_mutex.unlock();
	return ret;
}
int 
BufferQueueInOut::in_count() { 
	int ret = 0;
	in_mutex.lock();
	ret = in_queue.size(); 
	in_mutex.unlock();
	return ret;
}

void 
BufferQueueInOut::enq_out(void* buffer, size_t bytes) {
	out_mutex.lock();

	SortReduceTypes::Block ib;
	ib.buffer = buffer;
	ib.bytes = bytes;

	out_queue.push(ib);

	out_mutex.unlock();
}

size_t 
BufferQueueInOut::deq_out(void** buffer) {
	size_t ret = 0;
	out_mutex.lock();
	if ( !out_queue.empty() ) {
		SortReduceTypes::Block b = out_queue.front();
		out_queue.pop();

		*buffer = b.buffer;
		ret = b.bytes;
	}
	out_mutex.unlock();
	return ret;
}

int 
BufferQueueInOut::out_count() { 
	int ret = 0;
	out_mutex.lock();
	ret = out_queue.size(); 
	out_mutex.unlock();
	return ret;
}

double
SortReduceUtils::TimespecDiffSec(timespec start, timespec end) {
	double t = end.tv_sec - start.tv_sec;
	t += ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	return t;
}
