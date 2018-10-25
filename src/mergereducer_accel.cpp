#ifdef HW_ACCEL

#include "mergereducer_accel.h"

template <class K, class V>
bool
SortReduceReducer::MergerNodeAccel<K,V>::m_instance_exist = false;


template <class K, class V>
SortReduceReducer::MergerNodeAccel<K,V>::MergerNodeAccel(V (*update)(V,V)) {
	m_started = false;
	m_kill = false;
	mp_update = update;

	m_cur_write_buffer_idx = m_write_buffer_idx_start;
	m_write_buffers_inflight = 0;
	m_total_out_bytes = 0;

	m_source_count = 0;
	// Make into argument?
	int block_bytes = 1024*1024;
	int block_count = 8;

	for ( int i = 0; i < block_count; i++ ) {
		SortReduceTypes::Block block;
		block.valid = true;
		block.last = false;
		block.managed = true;
		block.managed_idx = i;
		block.buffer = malloc(block_bytes);
		//block.valid_bytes = block_bytes;
		block.bytes = block_bytes;
		this->ma_blocks.push_back(block);

		this->mq_free_idx.push(i);
	}
	m_out_offset = 0;

	m_cur_out_idx = mq_free_idx.front();
	mq_free_idx.pop();

	for ( int i = 0; i < HW_MAXIMUM_SOURCES; i++ ) {
		ma_sources_offset[i] = 0;
		ma_cur_read_blocks[i].valid = false;
		ma_cur_read_blocks[i].valid_bytes = 0;
		ma_last_sent[i] = false;
	}

	for ( int i = 0; i < m_write_buffer_idx_start; i++ ) mq_free_dma_idx.push(i);


}
template <class K, class V>
SortReduceReducer::MergerNodeAccel<K,V>::~MergerNodeAccel() {
	m_kill = true;
	m_worker_thread.join();
	m_instance_exist = false;
}

template <class K, class V>
void
SortReduceReducer::MergerNodeAccel<K,V>::AddSource(BlockSource<K,V>* src) {
	if ( m_started ) return;

	ma_sources[m_source_count] = src;
	m_source_count++;
}

template <class K, class V>
bool
SortReduceReducer::MergerNodeAccel<K,V>::Start() {
	if ( this->m_instance_exist == true ) return false;

	this->m_instance_exist = true;

	if ( m_source_count <= HW_MAXIMUM_SOURCES ) {
		m_worker_thread = std::thread(&MergerNodeAccel<K,V>::WorkerThread,this);
	} else {
		fprintf(stderr, "MergerNodeAccel called with more then available sources %d\n", m_source_count );
	}

	return true;
}

template <class K, class V>
void
SortReduceReducer::MergerNodeAccel<K,V>::WorkerThread() {
	int source_count = m_source_count;
	BdbmPcie* pcie = BdbmPcie::getInstance();
	
	printf( "MergerNodeAccel WorkerThread started with %d sources!\n", source_count ); fflush(stdout);

	SendWriteBlock();
	SendWriteBlock();
	SendWriteBlock();
	SendWriteBlock();

	
	for ( int i = 0; i < HW_MAXIMUM_SOURCES; i++ ) {
		if ( i < m_source_count ) {
			for ( int j = 0; j < 2; j++ ) {
				if ( !SendReadBlock(ma_sources[i], i) ) {
					SendReadBlockDone(i);
					break;
				}
			}
		} else {
			SendReadBlockDone(i);
		}
	}

	bool last = false;
	while (true) {



		uint32_t res0 = pcie->userReadWord(32*4);
		uint32_t res1 = pcie->userReadWord(33*4);
		uint32_t res2 = pcie->userReadWord(34*4);
		while ( res0 != 0xffffffff || res1 != 0xffffffff || res2 != 0xffffffff ) {
			
			printf( "Sort result %x - %x : %d\n", res0, res1, res2 );

			res0 = pcie->userReadWord(32*4);
			res1 = pcie->userReadWord(33*4);
			res2 = pcie->userReadWord(34*4);
		}







		uint32_t read_done = pcie->userReadWord(0);
		while (read_done != 0xffffffff) {
			
			if ( maq_read_buffers_inflight[read_done].empty() ) {
				fprintf(stderr, "MergerNodeAccel returned read buffer that was not issued\n" );
			}
			int returned_dma_idx = maq_read_buffers_inflight[read_done].front();
			maq_read_buffers_inflight[read_done].pop();
			mq_free_dma_idx.push(returned_dma_idx);

			printf( "Read done from source %d\n", read_done );
			if ( !SendReadBlock(ma_sources[read_done], read_done) ) {
				SendReadBlockDone(read_done);
			}
			read_done = pcie->userReadWord(0);
		}
		uint32_t write_done = pcie->userReadWord(1*4);
		while (write_done != 0xffffffff) {
			uint32_t idx = ((write_done>>16)&0x3fff);
			last = ((write_done>>31) > 0 ? true : false);
			uint32_t bytes = (write_done & 0xffff);

			EmitBlock(idx*1024*4, bytes, last);

			if ( last ) break;

			SendWriteBlock();
			write_done = pcie->userReadWord(1*4);
		}
		if ( last ) break;
	}

	printf( "Done!!\n" );

}

template <class K, class V>
SortReduceTypes::Block 
SortReduceReducer::MergerNodeAccel<K,V>::GetBlock() {
	SortReduceTypes::Block block;
	block.valid = false;

	m_mutex.lock();
	if ( !mq_ready_idx.empty() ) {
		int idx = mq_ready_idx.front();
		block = ma_blocks[idx];
		mq_ready_idx.pop();
	}
	m_mutex.unlock();


	return block;
}

template <class K, class V>
void 
SortReduceReducer::MergerNodeAccel<K,V>::ReturnBlock(SortReduceTypes::Block block) {
	if ( block.managed_idx < 0 || block.valid == false || block.managed == false ) {
		fprintf( stderr, "ERROR: MergerNodeAccel::ReturnBlock called with invalid block %s:%d\n", __FILE__, __LINE__ );
		return;
	}
	printf( "Ready Block returned %d\n", block.managed_idx );
	m_mutex.lock();
	mq_free_idx.push(block.managed_idx);
	m_mutex.unlock();
}

template <class K, class V>
bool
SortReduceReducer::MergerNodeAccel<K,V>::SendReadBlock(BlockSource<K,V>* src, int idx) {

	printf( "MergerNodeAccel::SendReadBlock called\n" );

	if ( ma_last_sent[idx] ) return false;

	if ( ma_sources_offset[idx] >= ma_cur_read_blocks[idx].valid_bytes ) {
		if ( ma_cur_read_blocks[idx].valid ) {
			src->ReturnBlock(ma_cur_read_blocks[idx]);
			ma_cur_read_blocks[idx].valid = false;
		}

		SortReduceTypes::Block block;
		block.valid = false;
		while ( block.valid == false) {
			block = src->GetBlock();
		}
		ma_cur_read_blocks[idx] = block;
		ma_sources_offset[idx] = 0;

		printf( "SendReadBlock getting new source block -- %lx %s\n", block.valid_bytes, block.last?"last":"not last" );

		if ( block.last ) {
			src->ReturnBlock(block);
			return false;
		}
	}
	
	BdbmPcie* pcie = BdbmPcie::getInstance();

	m_mutex.lock();


	while ( mq_free_dma_idx.empty() ) {
		m_mutex.unlock();
		m_mutex.lock();
	}
	int di = mq_free_dma_idx.front();
	mq_free_dma_idx.pop();

	uint8_t* dmabuf = (uint8_t*)pcie->dmaBuffer();
	size_t dmaoff = (size_t)di * 4096;
	size_t avail = ma_cur_read_blocks[idx].valid_bytes - ma_sources_offset[idx];
	if ( avail >= 4096 ) {
		memcpy(dmabuf+dmaoff, ((uint8_t*)ma_cur_read_blocks[idx].buffer)+ma_sources_offset[idx], 4096);
	} else {
		memcpy(dmabuf+dmaoff, ((uint8_t*)ma_cur_read_blocks[idx].buffer)+ma_sources_offset[idx], avail);
		memset(dmabuf+dmaoff+avail, 0xff, 4096-avail);
	}
	ma_sources_offset[idx] += 4096;

	uint32_t cmd = (idx<<16)|di;
	pcie->userWriteWord(0,cmd);
	maq_read_buffers_inflight[idx].push(di);

	m_mutex.unlock();

	printf( "MergerNodeAccel::SendReadBlock done\n" );

	return true;
}

template <class K, class V>
void
SortReduceReducer::MergerNodeAccel<K,V>::SendReadBlockDone(int idx) {
	printf( "MergerNodeAccel::SendReadBlockDone called to %d\n", idx );
	if ( ma_last_sent[idx] ) return;
	m_mutex.lock();
	BdbmPcie* pcie = BdbmPcie::getInstance();


	uint32_t cmd = (idx<<16)|0xffff;
	pcie->userWriteWord(0,cmd);

	ma_last_sent[idx] = true;
	m_mutex.unlock();
	printf( "MergerNodeAccel::SendReadBlockDone done\n" );
	return;
}


template <class K, class V>
void
SortReduceReducer::MergerNodeAccel<K,V>::SendWriteBlock() {
	m_mutex.lock();
	BdbmPcie* pcie = BdbmPcie::getInstance();

	if ( m_cur_write_buffer_idx < m_write_buffer_idx_start ) m_cur_write_buffer_idx = m_write_buffer_idx_start;
	if ( m_cur_write_buffer_idx >= m_write_buffer_idx_end ) m_cur_write_buffer_idx = m_write_buffer_idx_start;

	pcie->userWriteWord(1*4,m_cur_write_buffer_idx);
	m_cur_write_buffer_idx ++;

	m_mutex.unlock();
}

template <class K, class V>
void
SortReduceReducer::MergerNodeAccel<K,V>::EmitBlock(size_t offset, size_t bytes, bool last) {
	BdbmPcie* pcie = BdbmPcie::getInstance();
	uint8_t* dmabuf = (uint8_t*)pcie->dmaBuffer();



	printf( "EmitBlock called at %lx %lx -> %lx (%lx)\n", bytes, offset, m_out_offset, m_total_out_bytes );
	uint32_t* dmas = (uint32_t*)(dmabuf + offset);
	uint32_t* dmae = (uint32_t*)(dmabuf + offset + bytes)-4;
	printf( "+> %x %x %x %x\n", dmas[0], dmas[1], dmas[2], dmas[3] );
	printf( "+< %x %x %x %x\n", dmae[0], dmae[1], dmae[2], dmae[3] );


	uint8_t* ptr = (dmabuf+offset);
	SortReduceTypes::Block block = ma_blocks[m_cur_out_idx];

	if ( bytes + m_out_offset < block.bytes ) {
		memcpy(((uint8_t*)block.buffer)+m_out_offset, ptr, bytes);
		m_out_offset += bytes;
	} else {
		size_t avail = block.bytes - m_out_offset;
		memcpy(((uint8_t*)block.buffer)+m_out_offset, ptr, avail);

		m_mutex.lock();
		ma_blocks[m_cur_out_idx].valid = true;
		ma_blocks[m_cur_out_idx].last = false;
		ma_blocks[m_cur_out_idx].valid_bytes = block.bytes;
		mq_ready_idx.push(m_cur_out_idx);

		printf( "Ready Block enqueued %d\n", m_cur_out_idx );

		m_out_offset = 0;

		while (mq_free_idx.empty()){
			m_mutex.unlock();
			m_mutex.lock();
		}

		m_cur_out_idx = mq_free_idx.front();
		mq_free_idx.pop();

		printf( "New free block %d\n", m_cur_out_idx );
		
		block = ma_blocks[m_cur_out_idx];
		memcpy(block.buffer, ptr+avail, bytes-avail);
		m_out_offset = bytes-avail;

		m_mutex.unlock();
	}
	m_total_out_bytes += bytes;


	if ( last ) {
		m_mutex.lock();
		ma_blocks[m_cur_out_idx].valid = true;
		ma_blocks[m_cur_out_idx].last = false;
		ma_blocks[m_cur_out_idx].valid_bytes = m_out_offset;
		mq_ready_idx.push(m_cur_out_idx);
		printf( "Ready Block enqueued %d -- flushing\n", m_cur_out_idx );
		
		while (mq_free_idx.empty()){
			m_mutex.unlock();
			m_mutex.lock();
		}

		m_cur_out_idx = mq_free_idx.front();
		mq_free_idx.pop();
		ma_blocks[m_cur_out_idx].valid = true;
		ma_blocks[m_cur_out_idx].last = true;
		mq_ready_idx.push(m_cur_out_idx);
		m_mutex.unlock();
		printf( "Ready Block enqueued %d -- last\n", m_cur_out_idx );
	}
}

TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::MergerNodeAccel)
#endif // HW_ACCEL
