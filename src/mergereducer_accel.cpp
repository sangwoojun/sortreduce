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

	// Make into argument?
	size_t block_bytes = fpga_buffer_size;
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

	for ( int i = 0; i < HW_MAXIMUM_SOURCES; i++ ) {
		ma_done[i] = false;
		ma_flushed[i] = false;
		ma_read_buffers_inflight[i] = 0;
	}


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

	ma_sources.push_back(src);
}

template <class K, class V>
bool
SortReduceReducer::MergerNodeAccel<K,V>::Start() {
	if ( this->m_instance_exist == true ) return false;

	this->m_instance_exist = true;

	if ( ma_sources.size() <= 8 ) {
		m_worker_thread = std::thread(&MergerNodeAccel<K,V>::WorkerThread,this);
	} else {
		fprintf(stderr, "MergerNodeAccel called with more then 8 sources %ld\n", ma_sources.size() );
	}

	return true;
}

template <class K, class V>
void
SortReduceReducer::MergerNodeAccel<K,V>::WorkerThread() {
	int source_count = ma_sources.size();
	
	printf( "MergerNodeAccel WorkerThread started with %d sources!\n", source_count ); fflush(stdout);

	DRAMHostDMA* dma = DRAMHostDMA::GetInstance();

	// 1 GB of 4 MB chunks
	for ( int i = 0; i < 1024/4; i++ ) {
		mq_fpga_free_buffer_idx.push(i);
	}


	// Send write buffer for Sort-Reduce root
	SendWriteBlock();

	int active_sources_cnt = 0;
	
	// Send read buffers for Sort-Reduce leaves
	for ( int i = 0; i < HW_MAXIMUM_SOURCES; i++ ) {
		if ( i < source_count ) {
			// send buffer~i to FPGA
			if (SendReadBlock(ma_sources[i], i) ) {
				active_sources_cnt++;
				ma_read_buffers_inflight[i] = 2; // two done signals req. bc/ buffer read start issues a done signal too
			}
			else SendReadBlockDone(i);
		} else {
			// send done~i to FPGA
			SendReadBlockDone(i);
		}
	}

	//TODO RETURN FPGA BUFFER IDXs

	bool last_write_block = false;
	BdbmPcie* pcie = BdbmPcie::getInstance();

	// Read blocks return a done signal at start of first buffer read
	// resulting in two buffers in flight at any time (yay)
	while (true) {
		int done = GetDoneBuffer();
		if ( done < 0 ) continue;

		if ( done < HW_MAXIMUM_SOURCES ) {
			// Read buffer done
			printf( "MergerNodeAccel read buffer done\n" );
			if ( !SendReadBlock(ma_sources[done], done) ) {
				// is last!
				printf( "MergerNodeAccel read source done\n" );
				SendReadBlockDone(done);
				if ( ma_read_buffers_inflight[done] > 0 ) {
					ma_read_buffers_inflight[done] --;
					if ( ma_read_buffers_inflight[done] == 0 ) {
						active_sources_cnt--;
					}
				} else {
					fprintf( stderr, "%s:%d should not happen!\n", __FILE__, __LINE__ );
				}
			}
		} else {
			// Write buffer done
			printf( "MergerNodeAccel write buffer done\n" );
			int blockidx = m_cur_write_buffer_idx;
			if ( !last_write_block ) {
				SendWriteBlock();
			}
			uint32_t bufferwritebytes = pcie->userReadWord(17*4);
			
			// emit
			size_t fpga_addr = fpga_buffer_size*blockidx;
			dma->CopyFromFPGA(fpga_addr, ma_blocks[blockidx].buffer, fpga_buffer_size);
			ma_blocks[blockidx].valid = true;
			ma_blocks[blockidx].managed_idx = blockidx;
			ma_blocks[blockidx].last = false;
			ma_blocks[blockidx].valid_bytes = bufferwritebytes;
			mq_ready_idx.push(blockidx);

			if ( bufferwritebytes != fpga_buffer_size ) {
				printf( "Write buffer is not 4 MB -- %x\n", bufferwritebytes );
			}

			if ( last_write_block ) break;
		}

		//if reader all done, break
		if ( !last_write_block && HardwareDone() ) {
			m_mutex.lock();
			bool free_exist = !mq_free_idx.empty();
			m_mutex.unlock();

			while ( !free_exist ) {
				m_mutex.lock();
				free_exist = !mq_free_idx.empty();
				m_mutex.unlock();
			}
			m_mutex.lock();
			int idx = mq_free_idx.front();
			mq_free_idx.pop();
			ma_blocks[idx].valid = true;
			ma_blocks[idx].managed_idx = idx;
			ma_blocks[idx].last = true;

			mq_ready_idx.push(idx);
			m_mutex.unlock();

			last_write_block = true;
		}
	}

	// wait until all write done

	//m_instance_exist = false;

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
	m_mutex.lock();
	mq_free_idx.push(block.managed_idx);
	m_mutex.unlock();
}

template <class K, class V>
void
SortReduceReducer::MergerNodeAccel<K,V>::SendDone(int src) {
}

template <class K, class V>
int 
SortReduceReducer::MergerNodeAccel<K,V>::CopyBlockToFPGA(SortReduceTypes::Block block) {
	if ( mq_fpga_free_buffer_idx.empty() ) return -1;
	if ( !block.valid ) {
		fprintf( stderr, "MergerNodeAccel CopyBlockToFPGA with invalid block\n" );
		return -1;
	}
	if ( block.valid_bytes > 1024*1024*4 ) {
		fprintf( stderr, "MergerNodeAccel CopyBlockToFPGA with block size > 4!\n" );
		return -1;
	}
	int fpga_buf_idx = mq_fpga_free_buffer_idx.front();
	mq_fpga_free_buffer_idx.pop();

	DRAMHostDMA* dma = DRAMHostDMA::GetInstance();

	size_t fpga_addr = (size_t)fpga_buf_idx * 4 * 1024;
	dma->CopyToFPGA(fpga_addr, block.buffer, block.valid_bytes);

	return fpga_buf_idx;
}

template <class K, class V>
void 
SortReduceReducer::MergerNodeAccel<K,V>::CopyBlockFromFPGA(SortReduceTypes::Block block, int idx){
}

template <class K, class V>
bool
SortReduceReducer::MergerNodeAccel<K,V>::SendReadBlock(BlockSource<K,V>* src, int idx) {

	if ( ma_done[idx] ) return false;
	
	printf( "MergerNodeAccel::SendReadBlock called\n" );

	SortReduceTypes::Block block;
	block.valid = false;
	while ( block.valid == false) {
		block = src->GetBlock();
	}

	if ( block.last ) {
		src->ReturnBlock(block);
		ma_done[idx] = true;
		return false;
	}
	
	DRAMHostDMA* dma = DRAMHostDMA::GetInstance();
	BdbmPcie* pcie = BdbmPcie::getInstance();

	m_mutex.lock();

	while ( mq_fpga_free_buffer_idx.empty() ) {
		m_mutex.unlock();
		m_mutex.lock();
	}

	int fpgaidx = mq_fpga_free_buffer_idx.front();
	mq_fpga_free_buffer_idx.pop();

	size_t fpga_buffer_offset = fpga_buffer_size*fpgaidx;
	dma->CopyToFPGA(fpga_buffer_offset, block.buffer, block.valid_bytes);

	pcie->userWriteWord(0, 0);
	pcie->userWriteWord(4*1, (uint32_t)fpga_buffer_offset);
	pcie->userWriteWord(4*2, fpga_buffer_size);
	pcie->userWriteWord(4*9, idx);

	m_mutex.unlock();

	src->ReturnBlock(block);

	printf( "MergerNodeAccel::SendReadBlock done\n" );

	return true;
}

template <class K, class V>
bool
SortReduceReducer::MergerNodeAccel<K,V>::SendReadBlockDone(int idx) {
	if ( ma_flushed[idx] ) return false;
	
	printf( "MergerNodeAccel::SendReadBlockDone called\n" );
	BdbmPcie* pcie = BdbmPcie::getInstance();

	m_mutex.lock();

	pcie->userWriteWord(0, 0);
	pcie->userWriteWord(4*1, 0);
	pcie->userWriteWord(4*2, 0);
	pcie->userWriteWord(4*9, idx);

	ma_flushed[idx] = true;

	m_mutex.unlock();
	printf( "MergerNodeAccel::SendReadBlockDone done\n" );
	return true;
}


// Blocks until sent!
template <class K, class V>
bool
SortReduceReducer::MergerNodeAccel<K,V>::SendWriteBlock() {
	printf( "MergerNodeAccel::SendWriteBlock called\n" );
	m_mutex.lock();
	BdbmPcie* pcie = BdbmPcie::getInstance();

	while ( mq_fpga_free_buffer_idx.empty() ) {
		m_mutex.unlock();
		m_mutex.lock();
	}

	bool free_exist = !mq_free_idx.empty();
	m_mutex.unlock();

	while ( !free_exist ) {
		m_mutex.lock();
		free_exist = !mq_free_idx.empty();
		m_mutex.unlock();
	}
	m_mutex.lock();

	int fpgaidx = mq_fpga_free_buffer_idx.front();
	mq_fpga_free_buffer_idx.pop();
	size_t fpga_buffer_offset = fpga_buffer_size*fpgaidx;

	m_cur_write_buffer_idx = mq_free_idx.front();
	mq_free_idx.pop();

	pcie->userWriteWord(0, 0);
	pcie->userWriteWord(4*1, (uint32_t)fpga_buffer_offset);
	pcie->userWriteWord(4*2, fpga_buffer_size);
	pcie->userWriteWord(4*8, 0);

	m_mutex.unlock();
	printf( "MergerNodeAccel::SendWriteBlock done\n" );
	return true;
}

template <class K, class V>
int
SortReduceReducer::MergerNodeAccel<K,V>::GetDoneBuffer() {
	BdbmPcie* pcie = BdbmPcie::getInstance();
	uint32_t donebuffer = pcie->userReadWord(16*4);
	if ( donebuffer > 1024 ) return -1;

	return (int)donebuffer;
}
template <class K, class V>
bool 
SortReduceReducer::MergerNodeAccel<K,V>::HardwareDone() {
	BdbmPcie* pcie = BdbmPcie::getInstance();
	uint32_t doneReached = pcie->userReadWord(1*4);

	if ( doneReached > 0 ) return true;
	
	return false;
}

TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::MergerNodeAccel)
#endif // HW_ACCEL
