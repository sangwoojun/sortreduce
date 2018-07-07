#include "alignedbuffermanager.h"

AlignedBufferManager*
AlignedBufferManager::mp_instance = NULL;

AlignedBufferManager*
AlignedBufferManager::GetInstance() {
	if ( mp_instance == NULL ) {
		mp_instance = new AlignedBufferManager();
	}
	return mp_instance;
}

void 
AlignedBufferManager::Init(size_t buffer_size, int buffer_count) {
	if ( m_init_done ) {
		fprintf( stderr, "AlignedBufferManager Init called again! %s:%d\n", __FILE__, __LINE__ );
		return;
	}

	m_buffer_size = buffer_size;
	m_buffer_count = buffer_count;

	mpp_buffers = (void**)malloc(sizeof(void*)*buffer_count);
	for ( int i = 0; i < buffer_count; i++ ) {
		mpp_buffers[i] = aligned_alloc(512, buffer_size);
		mq_free_buffers.push(i);
	}

	m_init_done = true;
}

SortReduceTypes::Block 
AlignedBufferManager::GetBuffer() {
	SortReduceTypes::Block ret;
	ret.valid = false;

	m_mutex.lock();
	
	if ( !mq_free_buffers.empty() ) {
		int idx = mq_free_buffers.front();
		mq_free_buffers.pop();

		ret.buffer = mpp_buffers[idx];
		ret.bytes = m_buffer_size;
		ret.managed = true;
		ret.managed_idx = idx;
		ret.valid = true;
	}
	
	m_mutex.unlock();

	return ret;
}

SortReduceTypes::Block 
AlignedBufferManager::WaitBuffer() {
	SortReduceTypes::Block ret;
	ret.valid = false;

	while (ret.valid == false) {
		ret = GetBuffer();
	}
	
	return ret;
}


AlignedBufferManager::AlignedBufferManager() {
	m_init_done = false;
}
