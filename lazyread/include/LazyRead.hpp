#ifndef __LAZYREAD_H__
#define __LAZYREAD_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

// move to config later
#define WRITEBUF_BYTES 512

class LazyRead {
public:
	LazyRead(int target_file_fd, size_t target_element_bytes, size_t target_buffer_bytes, size_t write_buffer_bytes);
	void StartTarget(uint64_t key);
	void ReadRequest(size_t offset, size_t bytes);
	void EndTarget();
private:
	uint32_t m_target_element_count;
	void* mp_target_buffer;

	//preallocated circular queue
	uint32_t* mp_free_target_buffer_q;
	uint32_t m_free_target_buffer_q_head;
	uint32_t m_free_target_buffer_q_tail;

	void** mpp_write_buffer;
};

#endif
