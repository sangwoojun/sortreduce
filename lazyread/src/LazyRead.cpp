#include "LazyRead.hpp"

LazyRead::LazyRead(int target_file_fd, size_t target_element_bytes, size_t target_buffer_bytes, size_t write_buffer_bytes) {
	size_t element_bytes = target_element_bytes + sizeof(uint32_t)+sizeof(uint64_t); // + count + key
	size_t element_count = target_element_bytes/element_bytes;
	mp_target_buffer = malloc(element_bytes*element_count);
	mp_free_target_buffer_q = (uint32_t*)malloc(element_bytes*sizeof(uint32_t));
	for ( uint32_t i = 0; i < element_count; i++ ) {
		mp_free_target_buffer_q[i] = i;
	}
	m_free_target_buffer_q_head = 0;
	m_free_target_buffer_q_tail = element_count;
	m_target_element_count = element_count;


	size_t write_buffer_count = write_buffer_bytes/WRITEBUF_BYTES;
	mpp_write_buffer = (void**)malloc(sizeof(void*)*write_buffer_count);
	for ( size_t i = 0; i < write_buffer_count; i++ ) {
		mpp_write_buffer[i] = malloc(WRITEBUF_BYTES);
	}
}

void 
LazyRead::StartTarget(uint64_t key) {
	if ( m_free_target_buffer_q_tail+1%m_target_element_count == m_free_target_buffer_q_head ) {
		//TODO perform reads until empty space exists
	}

	// Get free target slot
}

void 
LazyRead::ReadRequest(size_t offset, size_t bytes) {
	// add count to current target
	// enqueue to mpp_write_buffer
	// if write buffer full, write to disk
}
	
void 
LazyRead::EndTarget() {
}
