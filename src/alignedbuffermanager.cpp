#include "alignedbuffermanager.h"

/**
TODO deconstructor
**/

AlignedBufferManager* AlignedBufferManager::mp_instance[ALIGNED_INSTANCE_COUNT] = {NULL,NULL,NULL,NULL};

AlignedBufferManager*
AlignedBufferManager::GetInstance(int idx) {
	if ( idx >= ALIGNED_INSTANCE_COUNT ) {
		fprintf(stderr, "AlignedBufferManager::GetInstance idx too large %d\n", idx );
		return NULL;
	}

	if ( mp_instance[idx] == NULL ) {
		mp_instance[idx] = new AlignedBufferManager();
		mp_instance[idx]->m_instance_idx = idx;
	}
	return mp_instance[idx];
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
		//mpp_buffers[i] = aligned_alloc(512, buffer_size);
		mpp_buffers[i] = NULL;
		ms_free_buffers.push(i);
	}

	m_init_done = true;
}

SortReduceTypes::Block 
AlignedBufferManager::GetBuffer() {
	SortReduceTypes::Block ret;
	ret.valid = false;

	if ( m_init_done == false ) {
		fprintf( stderr, "AlignedBufferManager Init not called! %s:%d\n", __FILE__, __LINE__ );
		return ret;
	}

	m_mutex.lock();
	
	if ( !ms_free_buffers.empty() ) {
		int idx = ms_free_buffers.top();
		ms_free_buffers.pop();

		if ( mpp_buffers[idx] == NULL ) {
			mpp_buffers[idx] = aligned_alloc(512, m_buffer_size);
		}

		ret.buffer = mpp_buffers[idx];
		ret.bytes = m_buffer_size;
		ret.managed = true;
		ret.managed_idx = idx;
		ret.valid = true;
		//printf( "Buffer granted %d -> %d\n", m_instance_idx, mq_free_buffers.size() );
	}
	
	m_mutex.unlock();

	return ret;
}

void 
AlignedBufferManager::ReturnBuffer(SortReduceTypes::Block block) {
	m_mutex.lock();
	//TODO check duplicates!
	ms_free_buffers.push(block.managed_idx);
	//if ( m_instance_idx == 0 ) printf( "Buffer returned %d -> %d\n", m_instance_idx, mq_free_buffers.size() );
	m_mutex.unlock();
}

int
AlignedBufferManager::GetFreeCount() {
	m_mutex.lock();

	int ret = ms_free_buffers.size();

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

void
AlignedBufferManager::ClearBuffers() {
	for ( int i = 0; i < m_buffer_count; i++ ) {
		void* ptr = mpp_buffers[i];
		mpp_buffers[i] = NULL;
		free(ptr);
	}
}
