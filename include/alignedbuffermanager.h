#ifndef __ALIGNED_BUFFER_MANAGER_H__
#define __ALIGNED_BUFFER_MANAGER_H__

#include <mutex>
#include <stack>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

#include "types.h"


#define ALIGNED_INSTANCE_COUNT 4

class AlignedBufferManager {
public:
	static AlignedBufferManager* GetInstance(int idx);
	void Init(size_t buffer_size, int buffer_count);
	SortReduceTypes::Block GetBuffer();
	SortReduceTypes::Block WaitBuffer();
	void ReturnBuffer(SortReduceTypes::Block block);

	int GetFreeCount();
	void ClearBuffers();

private:
	static AlignedBufferManager* mp_instance[ALIGNED_INSTANCE_COUNT];
	int m_instance_idx;
	AlignedBufferManager();

	size_t m_buffer_size;
	int m_buffer_count;

	std::stack<int> ms_free_buffers;
	void** mpp_buffers;

	std::mutex m_mutex;


	bool m_init_done = false;
};

#endif
