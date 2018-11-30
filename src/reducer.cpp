#include "reducer.h"
#include "utils.h"
#include "types.h"
/*************
 TODO: 
 rewrite StreamFileWriterNode using FileWriterNode
 block-level emit methods in BlockSource, rewrite BlockSourceNode
***************/

template <class K, class V>
SortReduceReducer::FileWriterNode<K,V>::FileWriterNode() {
	this->m_out_offset = 0;
	this->m_out_file_offset = 0;
	this->mp_buffer_manager = AlignedBufferManager::GetInstance(1);
	
	this->mp_temp_file_manager = NULL;
	
	// wait until data actually available
	m_out_block.valid = false;
}
template <class K, class V>
SortReduceReducer::FileWriterNode<K,V>::~FileWriterNode() {
	if (mp_temp_file_manager!= NULL && m_out_offset > 0 && m_out_block.valid ) {
		EmitFlush();
	}
	if ( this->mp_temp_file_manager != NULL ) {
		delete this->mp_temp_file_manager;
	}
}

template <class K, class V>
void
SortReduceReducer::FileWriterNode<K,V>::CreateFile(std::string temp_directory, std::string filename) {
	this->mp_temp_file_manager = new TempFileManager(temp_directory);
	this->m_out_file = this->mp_temp_file_manager->CreateEmptyFile(filename);
	//printf( "MergerNodeAccel Created file %s %s -- %p\n", temp_directory.c_str(), filename.c_str(), m_out_file );
}

template <class K, class V>
void
SortReduceReducer::FileWriterNode<K,V>::EmitBlock(void* buffer, size_t bytes) {
	while (!m_out_block.valid) {
		mp_temp_file_manager->CheckDone();
		m_out_block = mp_buffer_manager->GetBuffer();
	}

	if ( m_out_offset + bytes <= m_out_block.bytes ) {
		memcpy(((uint8_t*)m_out_block.buffer)+m_out_offset, buffer, bytes);
		m_out_offset += bytes;
	} else {
		size_t available = m_out_block.bytes - m_out_offset;
		size_t debt = bytes - available;
		memcpy(((uint8_t*)m_out_block.buffer)+m_out_offset, buffer, available);
		m_out_block.valid_bytes = m_out_block.bytes;
		while ( !this->mp_temp_file_manager->Write(this->m_out_file, m_out_block, m_out_file_offset) ) usleep(50);
		m_out_file_offset += m_out_block.valid_bytes;

		m_out_block = mp_buffer_manager->GetBuffer();
		while (!m_out_block.valid) {
			this->mp_temp_file_manager->CheckDone();
			m_out_block = mp_buffer_manager->GetBuffer();
		}

		memcpy(m_out_block.buffer, ((uint8_t*)buffer)+available, debt);
		m_out_offset = debt;
	}
}
template <class K, class V>
void
SortReduceReducer::FileWriterNode<K,V>::EmitFlush() {
	m_out_block.valid_bytes = m_out_offset;
	while ( !this->mp_temp_file_manager->Write(this->m_out_file, m_out_block, m_out_file_offset) ) usleep(50);
	m_out_file_offset += m_out_block.valid_bytes;
	m_out_offset = 0;

	while (this->mp_temp_file_manager->CountInFlight() > 0 ) this->mp_temp_file_manager->CheckDone();

	close(m_out_file->fd);
	m_out_file->fd = -1;
}

template <class K, class V>
SortReduceReducer::StreamFileWriterNode<K,V>::StreamFileWriterNode(std::string temp_directory, std::string filename) {
	this->m_out_offset = 0;
	this->m_out_file_offset = 0;
	this->m_last_key = 0;
	this->mp_buffer_manager = AlignedBufferManager::GetInstance(1);
	this->mp_temp_file_manager = new TempFileManager(temp_directory);
	this->m_out_file = this->mp_temp_file_manager->CreateEmptyFile(filename);
	//printf( "%s %d \n", m_out_file->path.c_str(), m_out_file->fd );
	
	m_out_block = mp_buffer_manager->GetBuffer();
	while (!m_out_block.valid) {
		mp_temp_file_manager->CheckDone();
		m_out_block = mp_buffer_manager->GetBuffer();
	}
}
template <class K, class V>
SortReduceReducer::StreamFileWriterNode<K,V>::~StreamFileWriterNode() {
	if ( m_out_offset > 0 ) EmitFlush();

	delete this->mp_temp_file_manager;
}

template <class K, class V>
inline void
SortReduceReducer::StreamFileWriterNode<K,V>::EmitKv(K key, V val) {
	const size_t kv_bytes = sizeof(K)+sizeof(V);

/*
	if ( key < m_last_key ) {
		printf( "StreamFileWriterNode key order wrong %lx %lx\n", m_last_key, key );
	}
	m_last_key = key;
*/

	if ( m_out_offset + kv_bytes <= m_out_block.bytes ) {
		ReducerUtils<K,V>::EncodeKvp(m_out_block.buffer, m_out_offset, key, val);
		m_out_offset += kv_bytes;
	} else {
		size_t available = m_out_block.bytes - m_out_offset;
		size_t debt = kv_bytes - available;
		if ( available >= sizeof(K) ) {
			ReducerUtils<K,V>::EncodeKey(m_out_block.buffer, m_out_offset, key);
			available -= sizeof(K);
			memcpy(((uint8_t*)m_out_block.buffer)+m_out_offset+sizeof(K), &val, available);

			m_out_block.valid_bytes = m_out_block.bytes;
			while ( !this->mp_temp_file_manager->Write(this->m_out_file, m_out_block, m_out_file_offset) ) usleep(50);
			m_out_file_offset += m_out_block.valid_bytes;

			m_out_block = mp_buffer_manager->GetBuffer();
			while (!m_out_block.valid) {
				this->mp_temp_file_manager->CheckDone();
				m_out_block = mp_buffer_manager->GetBuffer();
			}

			memcpy(m_out_block.buffer, ((uint8_t*)&val)+available, debt);
			m_out_offset = debt;
		} else {
			memcpy(((uint8_t*)m_out_block.buffer)+m_out_offset, &key, available);

			m_out_block.valid_bytes = m_out_block.bytes;
			while ( !this->mp_temp_file_manager->Write(this->m_out_file, m_out_block, m_out_file_offset) ) usleep(50);
			m_out_file_offset += m_out_block.valid_bytes;

			m_out_block = mp_buffer_manager->GetBuffer();
			while (!m_out_block.valid) {
				this->mp_temp_file_manager->CheckDone();
				m_out_block = mp_buffer_manager->GetBuffer();
			}

			memcpy(m_out_block.buffer, ((uint8_t*)&key)+available, sizeof(K)-available);
			ReducerUtils<K,V>::EncodeVal(m_out_block.buffer, sizeof(K)-available, val);
			m_out_offset = debt;
		}

	}
}
template <class K, class V>
inline void
SortReduceReducer::StreamFileWriterNode<K,V>::EmitFlush() {
	m_out_block.valid_bytes = m_out_offset;
	while ( !this->mp_temp_file_manager->Write(this->m_out_file, m_out_block, m_out_file_offset) ) usleep(50);
	m_out_file_offset += m_out_block.valid_bytes;
	m_out_offset = 0;

	while (this->mp_temp_file_manager->CountInFlight() > 0 ) this->mp_temp_file_manager->CheckDone();
	
	close(m_out_file->fd);
	m_out_file->fd = -1;

}

SortReduceReducer::StreamFileReader::StreamFileReader(std::string temp_directory, bool verbose) {
	this->m_total_reads_inflight = 0;
	this->m_started = false;
	this->m_verbose = verbose;

	this->mp_buffer_manager = AlignedBufferManager::GetInstance(1);
	this->mp_temp_file_manager = new TempFileManager(temp_directory, verbose);
}

SortReduceReducer::StreamFileReader::~StreamFileReader() {
	for ( size_t i = 0; i < mv_file_sources.size(); i++ ) {
		mp_temp_file_manager->Close(mv_file_sources[i]);
	}
	delete this->mp_temp_file_manager;
}

void
SortReduceReducer::StreamFileReader::PutFile(SortReduceTypes::File* file) {
	if ( this->m_started ) {
		fprintf(stderr, "ERROR: Attempting to add data source to started reducer\n" );
		return;
	}
	
	int cur_count = mv_file_sources.size();

	mv_file_sources.push_back(file);

	//mv_read_offset.push_back(0);
	mv_file_offset.push_back(0);
	mv_file_eof.push_back(false);
	mv_reads_inflight.push_back(0);
	
	mvq_ready_blocks.push_back(std::queue<SortReduceTypes::Block>());

	m_input_file_bytes += file->bytes;

	//FileReadReq(cur_count);
	LoadNextFileBlock(cur_count);

	if ( m_verbose ) {
		printf( "StreamFileReader reading file %s : %ld\n", file->path.c_str(), file->bytes );
		fflush(stdout);
	}
}

// This method is not thread-safe!
void
SortReduceReducer::StreamFileReader::FileReadReq(int src) {
	SortReduceTypes::File* file = this->mv_file_sources[src];

	while ( !mv_file_eof[src] && mvq_ready_blocks[src].size() + mv_reads_inflight[src] < m_file_reads_inflight_target ) {
		SortReduceTypes::Block block = mp_buffer_manager->GetBuffer();
		while (!block.valid) {
			mp_temp_file_manager->CheckDone();
			block = mp_buffer_manager->GetBuffer();
		}


		if ( block.bytes + mv_file_offset[src] < file->bytes ) {
			block.valid_bytes = block.bytes;
		} else {
			block.valid_bytes = file->bytes - mv_file_offset[src];
			mv_file_eof[src] = true;
		}
		
		if ( block.valid_bytes > 0 ) {
			//size_t req_bytes = block.valid_bytes;
			//if ( req_bytes % 512 > 0 ) req_bytes = ((req_bytes/512)+1)*512;
			while ( 0 > this->mp_temp_file_manager->Read(file, mv_file_offset[src], block.bytes, block.buffer) );

			mv_file_offset[src] += block.valid_bytes;
			mv_reads_inflight[src] ++;
			m_total_reads_inflight ++;

			mq_read_request_order.push(std::make_tuple(src,block));
		} else {
			mp_buffer_manager->ReturnBuffer(block);
		}
		if ( m_verbose ) {
			printf("StreamFileReader reading %d %lx %ld\n", src, mv_file_offset[src], block.valid_bytes );
			fflush(stdout);
		}
	}
	//printf( "Block %ld %d\n",block.bytes,block.managed_idx ); fflush(stdout);
}

SortReduceTypes::Block
SortReduceReducer::StreamFileReader::LoadNextFileBlock(int src, bool pop) {
	SortReduceTypes::Block ret;
	ret.valid = true;

	m_mutex.lock();

	FileReadReq(src);

	while ( mvq_ready_blocks[src].size() < 1 && mv_reads_inflight[src] > 0 ) {
		int read_done = mp_temp_file_manager->ReadStatus(true);
		m_total_reads_inflight -= read_done;

		for ( int i = 0; i < read_done; i++ ) {
			if ( mq_read_request_order.empty() ) {
				fprintf(stderr, "ERROR: mq_read_request_order empty! %s:%d\n", __FILE__, __LINE__ );
				exit(1);
			}

			std::tuple<int,SortReduceTypes::Block> req = mq_read_request_order.front();
			mq_read_request_order.pop();
			int dst = std::get<0>(req);
			SortReduceTypes::Block block = std::get<1>(req);

			mv_reads_inflight[dst] --;

			mvq_ready_blocks[dst].push(block);

			//printf( "File read %d\n", src ); fflush(stdout);
		}

		if ( m_total_reads_inflight < 0 ) {
			fprintf(stderr, "ERROR: Total_reads_inflight is negative! %d %s:%d\n", m_total_reads_inflight, __FILE__, __LINE__ );
		}
	}

	//FileReadReq(src);
	if ( m_verbose ){
		printf( "StreamFileReader loading from %d %s\n", src, pop?"pop":"nopop" );
		fflush(stdout);
	}

	if ( !mvq_ready_blocks[src].empty() ) {
		ret = mvq_ready_blocks[src].front();
		if ( pop ) mvq_ready_blocks[src].pop();
	} else {
		ret.last = true;
		ret.valid_bytes = 0;
	}
	
	m_mutex.unlock();

	return ret;
}

void
SortReduceReducer::StreamFileReader::ReturnBuffer(SortReduceTypes::Block block) {
	if ( block.valid && !block.last ) {
		mp_buffer_manager->ReturnBuffer(block);
	}
}







/***********************************************
** FileReaderNode start
*********************************************/


template <class K, class V>
SortReduceReducer::FileReaderNode<K,V>::FileReaderNode(StreamFileReader* src, int idx) {
	mp_reader = src;
	m_idx = idx;
}

template <class K, class V>
SortReduceTypes::Block
SortReduceReducer::FileReaderNode<K,V>::GetBlock() {
	SortReduceTypes::Block ret = mp_reader->LoadNextFileBlock(m_idx, true);
	return ret;
}

template <class K, class V>
void
SortReduceReducer::FileReaderNode<K,V>::ReturnBlock(SortReduceTypes::Block block) {
	mp_reader->ReturnBuffer(block);
}

/******************************************
** FileReaderNode end
******************************************************/

/***********************************************
** BlockReaderNode start
*********************************************/


template <class K, class V>
SortReduceReducer::BlockReaderNode<K,V>::BlockReaderNode(SortReduceTypes::Block block) {
	m_done = false;
	m_block = block;
	m_block.last = false;
}

template <class K, class V>
SortReduceTypes::Block
SortReduceReducer::BlockReaderNode<K,V>::GetBlock() {
	if ( !m_done ) {
		m_done = true;
		//printf( "-- %s %s %lx\n", m_block.valid?"valid":"not valid", m_block.last?"last":"not last", m_block.valid_bytes);
		return m_block;
	}

	SortReduceTypes::Block ret;
	ret.valid = true;
	ret.last = true;
	ret.valid_bytes = 0;
	return ret;
}

template <class K, class V>
void
SortReduceReducer::BlockReaderNode<K,V>::ReturnBlock(SortReduceTypes::Block block) {
	//FIXME 
	AlignedBufferManager* buffer_manager = AlignedBufferManager::GetInstance(0);
	//printf( "!!!!!!\n" ); fflush(stdout);
	if ( block.last == false ) {
		buffer_manager->ReturnBuffer(block);
	}
}

/******************************************
** BlockReaderNode end
******************************************************/


template <class K, class V>
SortReduceReducer::MergeReducer<K,V>::~MergeReducer() {
}

template <class K, class V>
SortReduceReducer::StreamMergeReducer<K,V>::StreamMergeReducer() {
	this->m_out_offset = 0;
	this->m_out_file_offset = 0;
	this->m_total_reads_inflight = 0;

	this->mp_buffer_manager = AlignedBufferManager::GetInstance(1);
	
	m_out_block = mp_buffer_manager->GetBuffer();
	while (!m_out_block.valid) {
		mp_temp_file_manager->CheckDone();
		m_out_block = mp_buffer_manager->GetBuffer();
	}
}

template <class K, class V>
inline void
SortReduceReducer::StreamMergeReducer<K,V>::EmitKv(K key, V val) {
	const size_t kv_bytes = sizeof(K)+sizeof(V);

	if ( m_out_offset + kv_bytes <= m_out_block.bytes ) {
		ReducerUtils<K,V>::EncodeKvp(m_out_block.buffer, m_out_offset, key, val);
		m_out_offset += kv_bytes;
	} else {
		size_t available = m_out_block.bytes - m_out_offset;
		size_t debt = kv_bytes - available;
		if ( available >= sizeof(K) ) {
			ReducerUtils<K,V>::EncodeKey(m_out_block.buffer, m_out_offset, key);
			available -= sizeof(K);
			memcpy(((uint8_t*)m_out_block.buffer)+m_out_offset+sizeof(K), &val, available);

			m_out_block.valid_bytes = m_out_block.bytes;
			while ( !this->mp_temp_file_manager->Write(this->m_out_file, m_out_block, m_out_file_offset) ) usleep(50);
			m_out_file_offset += m_out_block.valid_bytes;

			m_out_block = mp_buffer_manager->GetBuffer();
			while (!m_out_block.valid) {
				this->mp_temp_file_manager->CheckDone();
				m_out_block = mp_buffer_manager->GetBuffer();
			}

			memcpy(m_out_block.buffer, ((uint8_t*)&val)+available, debt);
			m_out_offset = debt;
		} else {
			memcpy(((uint8_t*)m_out_block.buffer)+m_out_offset, &key, available);

			m_out_block.valid_bytes = m_out_block.bytes;
			while ( !this->mp_temp_file_manager->Write(this->m_out_file, m_out_block, m_out_file_offset) ) usleep(50);
			m_out_file_offset += m_out_block.valid_bytes;

			m_out_block = mp_buffer_manager->GetBuffer();
			while (!m_out_block.valid) {
				this->mp_temp_file_manager->CheckDone();
				m_out_block = mp_buffer_manager->GetBuffer();
			}

			memcpy(m_out_block.buffer, ((uint8_t*)&key)+available, sizeof(K)-available);
			ReducerUtils<K,V>::EncodeVal(m_out_block.buffer, sizeof(K)-available, val);
			m_out_offset = debt;
		}

	}
}

template <class K, class V>
void
SortReduceReducer::StreamMergeReducer<K,V>::EmitFlush() {
	m_out_block.valid_bytes = m_out_offset;
	while ( !this->mp_temp_file_manager->Write(this->m_out_file, m_out_block, m_out_file_offset) ) usleep(50);
	m_out_file_offset += m_out_block.valid_bytes;

	while (this->mp_temp_file_manager->CountInFlight() > 0 ) this->mp_temp_file_manager->CheckDone();
	
	close(m_out_file->fd);
	m_out_file->fd = -1;
}

template <class K, class V>
void
SortReduceReducer::StreamMergeReducer<K,V>::PutBlock(SortReduceTypes::Block block) {
	if ( this->m_started ) {
		fprintf(stderr, "ERROR: Attempting to add data source to started reducer\n" );
		return;
	}

	int cur_count = mv_input_sources.size();

	DataSource source;
	source.from_file = false;
	source.block = block;
	source.file = NULL;
	mv_input_sources.push_back(source);

	//mv_read_offset.push_back(0);
	mv_file_offset.push_back(0);
	mv_file_eof.push_back(false);
	mv_reads_inflight.push_back(0);

	mvq_ready_blocks.push_back(std::queue<SortReduceTypes::Block>());
	
	mvq_ready_blocks[cur_count].push(block);
}

template <class K, class V>
void
SortReduceReducer::StreamMergeReducer<K,V>::PutFile(SortReduceTypes::File* file) {
	if ( this->m_started ) {
		fprintf(stderr, "ERROR: Attempting to add data source to started reducer\n" );
		return;
	}
	
	int cur_count = mv_input_sources.size();

	DataSource source;
	source.from_file = true;
	source.block.valid = false;
	source.file = file;
	mv_input_sources.push_back(source);

	//mv_read_offset.push_back(0);
	mv_file_offset.push_back(0);
	mv_file_eof.push_back(false);
	mv_reads_inflight.push_back(0);
	
	mvq_ready_blocks.push_back(std::queue<SortReduceTypes::Block>());

	m_input_file_bytes += file->bytes;

	//FileReadReq(cur_count);
	GetNextFileBlock(cur_count);

}

// This method is not thread-safe!
template <class K, class V>
void
SortReduceReducer::StreamMergeReducer<K,V>::FileReadReq(int src) {
	SortReduceTypes::File* file = this->mv_input_sources[src].file;

	if ( !mv_input_sources[src].from_file ) return;

	while ( !mv_file_eof[src] && mvq_ready_blocks[src].size() + mv_reads_inflight[src] < (size_t)m_file_reads_inflight_target ) {
		SortReduceTypes::Block block = mp_buffer_manager->GetBuffer();
		while (!block.valid) {
			mp_temp_file_manager->CheckDone();
			block = mp_buffer_manager->GetBuffer();
		}


		if ( block.bytes + mv_file_offset[src] <= file->bytes ) {
			block.valid_bytes = block.bytes;
		} else {
			block.valid_bytes = file->bytes - mv_file_offset[src];
			mv_file_eof[src] = true;
		}
		
		if ( block.valid_bytes > 0 ) {
			//size_t req_bytes = block.valid_bytes;
			//if ( req_bytes % 512 > 0 ) req_bytes = ((req_bytes/512)+1)*512;
			while ( 0 > this->mp_temp_file_manager->Read(file, mv_file_offset[src], block.bytes, block.buffer) );

			mv_file_offset[src] += block.bytes;
			mv_reads_inflight[src] ++;
			m_total_reads_inflight ++;

			mq_read_request_order.push(std::make_tuple(src,block));
		} else {
			mp_buffer_manager->ReturnBuffer(block);
		}
	}
	//printf( "Block %ld %d\n",block.bytes,block.managed_idx ); fflush(stdout);
}

template <class K, class V>
SortReduceTypes::Block 
SortReduceReducer::StreamMergeReducer<K,V>::GetNextFileBlock(int src) {
	SortReduceTypes::Block ret;
	ret.valid = false;
	if ( !mv_input_sources[src].from_file ) {
		fprintf( stderr, "ERROR: GetNextFileBlock called on in-memory block\n" );
		return ret;
	}
	
	m_mutex.lock();

	FileReadReq(src);

	while ( mvq_ready_blocks[src].size() < 2 && mv_reads_inflight[src] > 0 ) {
		int read_done = mp_temp_file_manager->ReadStatus(true);
		m_total_reads_inflight -= read_done;

		for ( int i = 0; i < read_done; i++ ) {
			if ( mq_read_request_order.empty() ) {
				fprintf(stderr, "ERROR: mq_read_request_order empty! %s:%d\n", __FILE__, __LINE__ );
				exit(1);
			}

			std::tuple<int,SortReduceTypes::Block> req = mq_read_request_order.front();
			mq_read_request_order.pop();
			int dst = std::get<0>(req);
			SortReduceTypes::Block block = std::get<1>(req);

			mv_reads_inflight[dst] --;

			mvq_ready_blocks[dst].push(block);

			//printf( "File read %d\n", src ); fflush(stdout);
		}

		if ( m_total_reads_inflight < 0 ) {
			fprintf(stderr, "ERROR: Total_reads_inflight is negative! %d %s:%d\n", m_total_reads_inflight, __FILE__, __LINE__ );
		}
	}

	//FileReadReq(src);
	
	m_mutex.unlock();

	return ret;
}


template <class K, class V>
inline K
SortReduceReducer::ReducerUtils<K,V>::DecodeKey(void* buffer, size_t offset) {
	return *(K*)(((uint8_t*)buffer)+offset);
}

template <class K, class V>
inline V
SortReduceReducer::ReducerUtils<K,V>::DecodeVal(void* buffer, size_t offset) {
	return *(V*)(((uint8_t*)buffer)+offset);
}
template <class K, class V>
inline SortReduceTypes::KvPair<K,V>
SortReduceReducer::ReducerUtils<K,V>::DecodeKvp(void* buffer, size_t offset) {
	SortReduceTypes::KvPair<K,V> ret;
	ret.key = *(K*)(((uint8_t*)buffer)+offset);
	ret.val = *(V*)(((uint8_t*)buffer)+offset+sizeof(K));
	return ret;
}
template <class K, class V>
inline void
SortReduceReducer::ReducerUtils<K,V>::EncodeKvp(void* buffer, size_t offset, K key, V val) {
	*(K*)(((uint8_t*)buffer)+offset) = key;
	*(V*)(((uint8_t*)buffer)+offset+sizeof(K)) = val;
}
template <class K, class V>
inline void 
SortReduceReducer::ReducerUtils<K,V>::EncodeKey(void* buffer, size_t offset, K key) {
	*(K*)(((uint8_t*)buffer)+offset) = key;
}

template <class K, class V>
inline void 
SortReduceReducer::ReducerUtils<K,V>::EncodeVal(void* buffer, size_t offset, V val) {
	*(V*)(((uint8_t*)buffer)+offset) = val;
}

// THIS IS ACTUALLY BUGGY!
template <class K, class V>
inline SortReduceTypes::KvPair<K,V>
SortReduceReducer::ReducerUtils<K,V>::DecodeKvPair(SortReduceTypes::Block* p_block, size_t* p_off, BlockSource<K,V>* src, bool* p_kill) {
	SortReduceTypes::KvPair<K,V> kvp = {0};
	//if ( p_block == NULL ) return kvp;

	size_t valid_bytes = p_block->valid_bytes;
	size_t off = *p_off;
	void* buffer = p_block->buffer;


	size_t avail = valid_bytes - off;
	if ( avail > sizeof(K) + sizeof(V) ) {

		kvp.key = DecodeKey(buffer, off);
		kvp.val = DecodeVal(buffer, off+sizeof(K));
		*p_off += sizeof(K)+sizeof(V);
	} else if ( avail == sizeof(K)+sizeof(V) ) {
		kvp.key = DecodeKey(buffer, off);
		kvp.val = DecodeVal(buffer, off+sizeof(K));

		SortReduceTypes::Block block;
		block.valid = false;
		while ( block.valid == false && !*p_kill ) {
			block = src->GetBlock();
		}
		src->ReturnBlock(*p_block);

		*p_block = block;
		*p_off = 0;
	} else if ( avail >= sizeof(K) ) {
		kvp.key = DecodeKey(buffer, off);
		size_t debt = sizeof(K) + sizeof(V) - avail;
		off += sizeof(K);
		memcpy(&kvp.val, ((uint8_t*)buffer+off), avail-sizeof(K));

		//src->ReturnBlock(*p_block);

		SortReduceTypes::Block block;
		block.valid = false;
		while ( block.valid == false && !*p_kill) {
			block = src->GetBlock();
		}
		
		if (block.last) {
			printf("o!!!!!\n");
			fflush(stdout);
		}

		memcpy(((uint8_t*)&kvp.val)+avail-sizeof(K), block.buffer, debt);

		src->ReturnBlock(*p_block);
		*p_block = block;
		*p_off = debt;
		
		size_t nvalid_bytes = p_block->valid_bytes;
		size_t navail = nvalid_bytes - *p_off;
		if ( navail == 0 ) {
			printf( "!!!!!!!!!\n" );
			fflush(stdout);
		}
	} else {
		int debt = sizeof(K) - avail;
		memcpy(&kvp.key, ((uint8_t*)buffer+off), avail);
			
		//src->ReturnBlock(*p_block);

		SortReduceTypes::Block block;
		block.valid = false;
		while ( block.valid == false && !*p_kill ) {
			block = src->GetBlock();
		}
		if (block.last) {
			printf("o!!!!!\n");
			fflush(stdout);
		}
		memcpy(((uint8_t*)&kvp.key)+avail, block.buffer, debt);
		kvp.val = DecodeVal(block.buffer, debt);
		
		src->ReturnBlock(*p_block);
		*p_block = block;
		*p_off = debt+sizeof(V);
		
		size_t nvalid_bytes = p_block->valid_bytes;
		size_t navail = nvalid_bytes - *p_off;
		if ( navail == 0 ) {
			printf( "!!!!!!!!!\n" );
			fflush(stdout);
		}
	}

	return kvp;
}

template <class K, class V>
SortReduceReducer::BlockSource<K,V>::~BlockSource() {
}

template <class K, class V>
SortReduceReducer::BlockSourceNode<K,V>::BlockSourceNode(size_t block_bytes, int block_count, int my_id)
	: SortReduceReducer::BlockSource<K,V>::BlockSource() {

	m_my_id = my_id;

	m_block_count = block_count;
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

	m_cur_free_idx = mq_free_idx.front();
	mq_free_idx.pop();
}


/**
IMPORTANT: Reducer three must be deleted in reverse order!
(Root->Leaf)
because root destructor calls ReturnBlock of leaf nodes
**/
template <class K, class V>
SortReduceReducer::BlockSourceNode<K,V>::~BlockSourceNode() {
	for ( size_t i = 0; i < ma_blocks.size(); i++ ) {
		free(ma_blocks[i].buffer);
	}
}

template <class K, class V>
SortReduceTypes::Block
SortReduceReducer::BlockSourceNode<K,V>::GetBlock() {
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
SortReduceReducer::BlockSourceNode<K,V>::ReturnBlock(SortReduceTypes::Block block) {
	if ( block.managed_idx < 0 || block.valid == false || block.managed == false ) {
		fprintf( stderr, "ERROR: BlockSourceNode::ReturnBlock called with invalid block %s:%d\n", __FILE__, __LINE__ );
		return;
	}

	m_mutex.lock();
	mq_free_idx.push(block.managed_idx);
	m_mutex.unlock();
}

template <class K, class V>
inline void
SortReduceReducer::BlockSourceNode<K,V>::EmitKvPair(K key, V val) {
/*
	if ( m_last_key > key ) {
		printf("EmitKvPair order wrong %d %lx %lx\n", m_my_id, (uint64_t)m_last_key, (uint64_t)key);
	}
	m_last_key = key;
*/
	SortReduceTypes::Block block = ma_blocks[m_cur_free_idx];
	size_t avail = block.bytes - m_out_offset;

	if ( avail >= sizeof(K)+sizeof(V) ) {
		ReducerUtils<K,V>::EncodeKvp(block.buffer, m_out_offset, key, val);
		m_out_offset += sizeof(K) + sizeof(V);
	} else {
		m_mutex.lock();
		ma_blocks[m_cur_free_idx].valid_bytes = m_out_offset; 
		mq_ready_idx.push(m_cur_free_idx);

		bool free_exist = !mq_free_idx.empty();
		m_mutex.unlock();

		while ( !free_exist ) {
			m_mutex.lock();
			free_exist = !mq_free_idx.empty();
			m_mutex.unlock();
		}

		m_mutex.lock();
		m_cur_free_idx = mq_free_idx.front();
		mq_free_idx.pop();
		m_mutex.unlock();

		block = ma_blocks[m_cur_free_idx];

		ReducerUtils<K,V>::EncodeKvp(block.buffer, 0, key, val);
		m_out_offset = sizeof(K) + sizeof(V);
	}
}

template <class K, class V>
void
SortReduceReducer::BlockSourceNode<K,V>::FinishEmit() {
	m_mutex.lock();
	//int cur_block_idx = mq_free_idx.front();
	ma_blocks[m_cur_free_idx].valid_bytes = m_out_offset;
	m_out_offset = 0;
	mq_ready_idx.push(m_cur_free_idx);
	//mq_free_idx.pop();
	bool free_exist = !mq_free_idx.empty();
	m_mutex.unlock();

		
	while ( !free_exist ) {
		m_mutex.lock();
		free_exist = !mq_free_idx.empty();
		m_mutex.unlock();
	}

	m_mutex.lock();
	m_cur_free_idx = mq_free_idx.front();
	m_mutex.unlock();

	SortReduceTypes::Block* block = &ma_blocks[m_cur_free_idx];

	block->last = true;
	block->valid_bytes = 0;
	
	m_mutex.lock();
	mq_ready_idx.push(m_cur_free_idx);
	m_mutex.unlock();
}

template <class K, class V>
SortReduceReducer::BlockKvReader<K,V>::BlockKvReader(BlockSource<K,V>* src) {
	mp_src = src;
	m_cur_block.valid = false;
	m_offset = 0;
	m_done = false;

	if ( mp_src == NULL ) {
		m_done = true;
		return;
	}

	while ( m_cur_block.valid == false ) {
		m_cur_block = mp_src->GetBlock();
	}
	if ( m_cur_block.last ) m_done = true;
}

template <class K, class V>
SortReduceReducer::BlockKvReader<K,V>::~BlockKvReader() {
	mp_src->ReturnBlock(m_cur_block);
}

// invariant : offset == valid_bytes only if no more
template <class K, class V>
inline bool
SortReduceReducer::BlockKvReader<K,V>::IsEmpty() {
	// The ordering REQUIRES valid_bytes of .last blocks are 0

	if ( m_done == true ) return true;
	if ( m_cur_block.last ) return true;
	if ( m_offset < m_cur_block.valid_bytes ) return false;

	if ( m_offset >= m_cur_block.valid_bytes ) {
		m_offset = 0;
		SortReduceTypes::Block block;
		block.valid = false;
		while ( block.valid == false ) {
			block = mp_src->GetBlock();
		}
		mp_src->ReturnBlock(m_cur_block);
		m_cur_block = block;
		if ( m_cur_block.last ) m_done = true;
	}

	return m_done;
}

template <class K, class V>
inline SortReduceTypes::KvPair<K,V>
SortReduceReducer::BlockKvReader<K,V>::GetNext() {
	SortReduceTypes::KvPair<K,V> ret;
	size_t avail = m_cur_block.valid_bytes - m_offset;
	if ( avail >= sizeof(K)+sizeof(V) ) {
		ret = ReducerUtils<K,V>::DecodeKvp(m_cur_block.buffer, m_offset);
		m_offset += sizeof(K) + sizeof(V);
	} else if ( avail > sizeof(K) ) {
		ret.key = ReducerUtils<K,V>::DecodeKey(m_cur_block.buffer, m_offset);
		size_t debt = sizeof(K) + sizeof(V) - avail;
		m_offset += sizeof(K);
		memcpy(&ret.val, ((uint8_t*)m_cur_block.buffer+m_offset), avail-sizeof(K));
		mp_src->ReturnBlock(m_cur_block);
		
		SortReduceTypes::Block block;
		block.valid = false;
		while ( block.valid == false ) {
			block = mp_src->GetBlock();
		}
		memcpy(((uint8_t*)&ret.val)+avail-sizeof(K), block.buffer, debt);
		m_cur_block = block;
		m_offset = debt;

		//if ( m_offset >= block.valid_bytes ) m_done = true;
	} else {
		size_t debt = sizeof(K) - avail;
		memcpy(&ret.key, ((uint8_t*)m_cur_block.buffer+m_offset), avail);
			
		mp_src->ReturnBlock(m_cur_block);
		
		SortReduceTypes::Block block;
		block.valid = false;
		while ( block.valid == false ) {
			block = mp_src->GetBlock();
		}
		memcpy(((uint8_t*)&ret.key)+avail, block.buffer, debt);
		ret.val = ReducerUtils<K,V>::DecodeVal(block.buffer, debt);
		m_cur_block = block;
		m_offset = debt + sizeof(V);
		//if ( m_offset >= block.valid_bytes ) m_done = true;
	}

	return ret;
}

template <class K, class V>
SortReduceReducer::MergerNode<K,V>::MergerNode(size_t block_bytes, int block_count, int my_id) 
	: SortReduceReducer::BlockSourceNode<K,V>(block_bytes, block_count, my_id) {

	m_started = false;
	m_kill = false;
	mp_update = NULL;
}

template <class K, class V>
SortReduceReducer::MergerNode<K,V>::MergerNode(size_t block_bytes, int block_count, V (*update)(V,V), int my_id) 
	: SortReduceReducer::BlockSourceNode<K,V>(block_bytes, block_count, my_id) {

	m_started = false;
	m_kill = false;
	mp_update = update;
}
template <class K, class V>
SortReduceReducer::MergerNode<K,V>::~MergerNode() {
	m_kill = true;
	m_worker_thread.join();
}

template <class K, class V>
void
SortReduceReducer::MergerNode<K,V>::AddSource(BlockSource<K,V>* src) {
	if ( m_started ) return;

	ma_sources.push_back(src);
}

template <class K, class V>
void
SortReduceReducer::MergerNode<K,V>::Start() {
	if ( ma_sources.size() == 2 ) {
		m_worker_thread = std::thread(&MergerNode<K,V>::WorkerThread2,this);
	} else {
		m_worker_thread = std::thread(&MergerNode<K,V>::WorkerThreadN,this);
	}
}


template <class K, class V>
void
SortReduceReducer::MergerNode<K,V>::WorkerThread2() {
	//uint32_t rid = rand();
	//printf( "MergerNode WorkerThread2 started! %x\n", rid ); fflush(stdout);


	BlockKvReader<K,V>* readers[2];
	readers[0] = new BlockKvReader<K,V>(ma_sources[0]);
	readers[1] = new BlockKvReader<K,V>(ma_sources[1]);

	SortReduceTypes::KvPair<K,V> kvp[2] = {0};
		
	for ( int i = 0; i < 2; i++ ) {
		if ( !readers[i]->IsEmpty() ) {
			kvp[i] = readers[i]->GetNext();
		}
	}

	bool done_src_0 = false;
	if ( !readers[0]->IsEmpty() && !readers[1]->IsEmpty() ) {
		while (true) {
			if ( kvp[0].key < kvp[1].key ) {
				this->EmitKvPair(kvp[0].key, kvp[0].val);

				if ( !readers[0]->IsEmpty() ) {
					kvp[0] = readers[0]->GetNext();
				} else {
					done_src_0 = true;
					break;
				}
			} else {
				this->EmitKvPair(kvp[1].key, kvp[1].val);
				if ( !readers[1]->IsEmpty() ) {
					kvp[1] = readers[1]->GetNext();
				} else {
					done_src_0 = false;
					break;
				}
			}
		}
	}

	// Note: At this point, either kvp[0] or kvp[1] is valid

	if ( done_src_0 ) {
		// fast forward readers[1]
		while ( true ) {
			this->EmitKvPair(kvp[1].key, kvp[1].val);
			if ( !readers[1]->IsEmpty() ) {
				kvp[1] = readers[1]->GetNext();
			} else break;
		}
	} else {
		// fast forward readers[0]
		while (true) {
			this->EmitKvPair(kvp[0].key, kvp[0].val);
			if ( !readers[0]->IsEmpty() ) {
				kvp[0] = readers[0]->GetNext();
			} else break;
		}
	}

	//printf( "MergerNode end reached -- %ld!\n", cnt ); fflush(stdout);

	this->FinishEmit();

	for ( int i = 0; i < 2; i++ ) {
		delete readers[i];
	}
}

template <class K, class V>
void
SortReduceReducer::MergerNode<K,V>::WorkerThreadN() {
	int source_count = ma_sources.size();
	
	//printf( "MergerNode WorkerThreadN started with %d sources!\n", source_count ); fflush(stdout);

	std::priority_queue<SortReduceTypes::KvPairSrc<K,V>,std::vector<SortReduceTypes::KvPairSrc<K,V> >, CompareKv> priority_queue;

	//std::vector<K> last_keys;
	SortReduceTypes::KvPair<K,V> last_kvp = {0};
	
	std::vector<BlockKvReader<K,V>*> readers;

	//uint64_t cnt = 0;

	for ( int i = 0; i < source_count; i++ ) {
		BlockKvReader<K,V>* reader = new BlockKvReader<K,V>(ma_sources[i]);
		readers.push_back(reader);


		if ( !readers[i]->IsEmpty() ) {
			SortReduceTypes::KvPair<K,V> kvp = readers[i]->GetNext();
			SortReduceTypes::KvPairSrc<K,V> kvps;
			kvps.key = kvp.key;
			kvps.val = kvp.val;
			kvps.src = i;
			priority_queue.push(kvps);
		}
	}

	//K last_key = 0;

	if ( !priority_queue.empty() ) {
		SortReduceTypes::KvPairSrc<K,V> kvp = priority_queue.top();
		priority_queue.pop();
		int src = kvp.src;

		if ( mp_update == NULL ) {
			this->EmitKvPair(kvp.key, kvp.val);
		} else {
			last_kvp.key = kvp.key;
			last_kvp.val = kvp.val;
		}
		
		if ( !readers[src]->IsEmpty() ) {
			SortReduceTypes::KvPair<K,V> kvp = readers[src]->GetNext();
			SortReduceTypes::KvPairSrc<K,V> kvps;
			kvps.key = kvp.key;
			kvps.val = kvp.val;
			kvps.src = src;
			priority_queue.push(kvps);
		}
	}

	while( !priority_queue.empty() ) {
		SortReduceTypes::KvPairSrc<K,V> kvp = priority_queue.top();
		priority_queue.pop();
		int src = kvp.src;

/*
		if ( last_key > kvp.key ) {
			printf("WorkerThreadN key order wrong %lx %lx!\n", last_key, kvp.key);
			fflush(stdout);
		}
		last_key = kvp.key;
*/
		if ( mp_update == NULL ) {
			this->EmitKvPair(kvp.key, kvp.val);
		} else {
			if ( last_kvp.key == kvp.key ) {
				last_kvp.val = mp_update(last_kvp.val, kvp.val);
			} else {
				this->EmitKvPair(last_kvp.key, last_kvp.val);
				last_kvp.key = kvp.key;
				last_kvp.val = kvp.val;
			}
		}
		//cnt++;


		if ( !readers[src]->IsEmpty() ) {
			SortReduceTypes::KvPair<K,V> kvp = readers[src]->GetNext();
			SortReduceTypes::KvPairSrc<K,V> kvps;
			kvps.key = kvp.key;
			kvps.val = kvp.val;
			kvps.src = src;
			priority_queue.push(kvps);
		}
	}
	if (mp_update != NULL) {
		this->EmitKvPair(last_kvp.key, last_kvp.val);
	}
	
	//printf( "MergerNodeN flushing emit!\n" ); fflush(stdout);

	this->FinishEmit();
	
	//printf( "MergerNodeN end reached!\n" ); fflush(stdout);
	
	for ( int i = 0; i < source_count; i++ ) {
		delete readers[i];
	}
	
}

template <class K, class V>
SortReduceReducer::ReducerNode<K,V>::ReducerNode(V (*update)(V,V), std::string temp_directory, std::string filename)
	: SortReduceReducer::StreamFileWriterNode<K,V>::StreamFileWriterNode(temp_directory, filename) {
	m_done = false;

	mp_update = update;
	m_kill = false;
}

template <class K, class V>
SortReduceReducer::ReducerNode<K,V>::~ReducerNode() {
	m_kill = true;
	m_worker_thread.join();
}


template <class K, class V>
void
SortReduceReducer::ReducerNode<K,V>::SetSource( BlockSource<K,V>* src) {
	mp_src = src;
	m_worker_thread = std::thread(&ReducerNode::WorkerThread,this);
}

template <class K, class V>
void
SortReduceReducer::ReducerNode<K,V>::WorkerThread() {
	size_t in_off = 0;
	SortReduceTypes::Block in_block;

	while ( in_block.valid == false && !m_kill ) {
		in_block = mp_src->GetBlock();
	}
	
	SortReduceTypes::KvPair<K,V> last_kvp = {0};

	uint64_t cnt = 0;
	uint64_t rcnt = 0;

	bool ignore_rest = false;
	if (!in_block.last) {
		last_kvp = ReducerUtils<K,V>::DecodeKvPair(&in_block, &in_off, mp_src, &m_kill);
		rcnt++;
	
		while (in_block.last == false && !m_kill) {
			SortReduceTypes::KvPair<K,V> kvp = ReducerUtils<K,V>::DecodeKvPair(&in_block, &in_off, mp_src, &m_kill);
			rcnt++;
			if ( kvp.key == last_kvp.key ) {
				last_kvp.val = mp_update(last_kvp.val, kvp.val);
			} else {
				this->EmitKv(last_kvp.key, last_kvp.val);

				cnt++;
				if ( !ignore_rest ) {
					if ( last_kvp.key > kvp.key ) {
						printf("WARNING: ReducerNode ignoring wrong order key %lx %lx -- %lx\n", rcnt, (uint64_t)last_kvp.key, (uint64_t)kvp.key ); fflush(stdout);
						ignore_rest = true;
					//} else {
						//printf("Correct order! %lx %lx -- %lx\n", rcnt, (uint64_t)last_kvp.key, (uint64_t)kvp.key ); fflush(stdout);
					} else {
						last_kvp = kvp;
					}
				}

			}
		}
		if ( !ignore_rest ) {
			this->EmitKv(last_kvp.key, last_kvp.val);
		}
		cnt++;
	}

	mp_src->ReturnBlock(in_block);
	this->EmitFlush();

	//printf( "ReducerNode read %ld emitted %ld\n", rcnt, cnt );

	m_done = true;
}

template <class K, class V>
SortReduceReducer::ReducerNodeStream<K,V>::ReducerNodeStream(V (*update)(V,V), size_t block_bytes, int block_count)
	: SortReduceReducer::BlockSourceNode<K,V>(block_bytes, block_count, -1) {

	m_done = false;
	mp_update = update;
	m_kill = false;
}

template <class K, class V>
SortReduceReducer::ReducerNodeStream<K,V>::~ReducerNodeStream() {
	m_kill = true;
	m_worker_thread.join();
}


template <class K, class V>
void
SortReduceReducer::ReducerNodeStream<K,V>::SetSource( BlockSource<K,V>* src) {
	mp_src = src;
	mp_reader = new BlockKvReader<K,V>(mp_src);
	m_worker_thread = std::thread(&ReducerNodeStream::WorkerThread,this);
}


template <class K, class V>
bool
SortReduceReducer::ReducerNodeStream<K,V>::IsDone() {
	bool ret = m_done;
	this->m_mutex.lock();
	if ( !this->mq_ready_idx.empty()
		&& this->mq_free_idx.size() == (size_t)this->m_block_count ) ret = true;
	this->m_mutex.unlock();

	return ret;
}


template <class K, class V>
void
SortReduceReducer::ReducerNodeStream<K,V>::WorkerThread() {

	SortReduceTypes::KvPair<K,V> last_kvp = {0};

	uint64_t cnt = 0;
	uint64_t rcnt = 0;

	bool ignore_rest = false;
	if (!mp_reader->IsEmpty()) {
		last_kvp = mp_reader->GetNext();
		rcnt++;
	
		while (!mp_reader->IsEmpty() && !m_kill) {
			SortReduceTypes::KvPair<K,V> kvp = mp_reader->GetNext();
			rcnt++;
			if ( kvp.key == last_kvp.key ) {
				last_kvp.val = mp_update(last_kvp.val, kvp.val);
			} else {
				this->EmitKvPair(last_kvp.key, last_kvp.val);

				cnt++;
				if ( !ignore_rest ) {
					if ( last_kvp.key > kvp.key ) {
						printf("WARNING: ReducerNode ignoring wrong order key! %lx %lx -- %lx\n", rcnt, (uint64_t)last_kvp.key, (uint64_t)kvp.key ); fflush(stdout);
						ignore_rest = true;
					} else {
						last_kvp = kvp;
					}
				}

			}
		}
		if ( !ignore_rest ) {
			this->EmitKvPair(last_kvp.key, last_kvp.val);
			cnt++;
		}
	}

	this->FinishEmit();
	
	printf( "ReducerNode read %ld emitted %ld -- %ld\n", rcnt, cnt );


	m_done = true;
}


template <class K, class V>
SortReduceReducer::BlockSourceReader<K,V>::BlockSourceReader() {
	m_done = false;
	m_kill = false;
}
template <class K, class V>
SortReduceReducer::BlockSourceReader<K,V>::BlockSourceReader(BlockSource<K,V>* src) {
	m_done = false;
	m_kill = false;

	mp_reader = new BlockKvReader<K,V>(src);
}

template <class K, class V>
void
SortReduceReducer::BlockSourceReader<K,V>::AddSource(BlockSource<K,V>* src) {
	mp_reader = new BlockKvReader<K,V>(src);
}

template <class K, class V>
inline SortReduceTypes::KvPair<K,V>
SortReduceReducer::BlockSourceReader<K,V>::GetNext() {
	SortReduceTypes::KvPair<K,V> kvp = {0};
		
	if ( !mp_reader->IsEmpty() ) {
		kvp = mp_reader->GetNext();
	}

	return kvp;
}

template <class K, class V>
inline bool
SortReduceReducer::BlockSourceReader<K,V>::Empty() {
	return mp_reader->IsEmpty();
}







template <class K, class V>
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::StreamMergeReducer_SinglePriority(V (*update)(V,V), std::string temp_directory, std::string filename, bool verbose) : SortReduceReducer::StreamMergeReducer<K,V>() {
	this->m_done = false;
	this->m_started = false;

	//this->ms_temp_directory = temp_directory;
	this->mp_update = update;

	this->mp_temp_file_manager = new TempFileManager(temp_directory, verbose);
	
	this->m_out_file = this->mp_temp_file_manager->CreateEmptyFile(filename);
}

template <class K, class V>
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::~StreamMergeReducer_SinglePriority() {
	m_worker_thread.join();
	delete this->mp_temp_file_manager;
	//printf( "Worker thread joined\n" );
}

template <class K, class V>
void
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::Start() {
	this->m_started = true;

	//printf( "StreamMergeReducer_SinglePriority started with %lu inputs\n", this->mv_input_sources.size() ); fflush(stdout);

	m_worker_thread = std::thread(&StreamMergeReducer_SinglePriority<K,V>::WorkerThread,this);
}


template <class K, class V>
void
SortReduceReducer::StreamMergeReducer_SinglePriority<K,V>::WorkerThread() {
	const size_t kv_bytes = sizeof(K)+sizeof(V);
	const int source_count = this->mv_input_sources.size();
	
	std::vector<size_t> read_offset(source_count, 0);
	std::vector<uint64_t> last_key_src(source_count, 0);

	std::chrono::high_resolution_clock::time_point last_time;
	std::chrono::milliseconds duration_milli;
	last_time = std::chrono::high_resolution_clock::now();
	
	AlignedBufferManager* buffer_manager = AlignedBufferManager::GetInstance(1);

	for ( int i = 0; i < source_count; i++ ) {
		if ( this->mv_input_sources[i].from_file ) {
			//this->FileReadReq(i);
			this->GetNextFileBlock(i);
		}
	}
	for ( int i = 0; i < source_count; i++ ) {
		if ( this->mv_input_sources[i].from_file ) {
			this->GetNextFileBlock(i);
		}
	}

	// ready_blocks is fully populated

	for ( int i = 0; i < source_count; i++ ) {
		if ( this->mvq_ready_blocks[i].empty() ) {
			fprintf(stderr, "ERROR: ready_blocks is not fully populated! %s:%d\n", __FILE__, __LINE__ );
			continue;
		} 

		SortReduceTypes::Block block = this->mvq_ready_blocks[i].front();

		K key = ReducerUtils<K,V>::DecodeKey(block.buffer, 0);
		V val = ReducerUtils<K,V>::DecodeVal(block.buffer, sizeof(K));
		KvPairSrc kvp;
		kvp.key = key;
		kvp.val = val;
		kvp.src = i;
		m_priority_queue.push(kvp);
		//printf( "Pushing initial values %lx %lx @ %d\n", (uint64_t)key, (uint64_t)val, i );
		//fflush(stdout);

		read_offset[i] += kv_bytes;
	}
	
	K last_key = 0;
	V last_val = 0;
	bool first_kvp = true;

	//size_t cur_count = 0;

	while (!m_priority_queue.empty() ) {
		KvPairSrc kvp = m_priority_queue.top();
		m_priority_queue.pop();
		int src = kvp.src;

		//cur_count ++;

		//printf( "Next: %lx %lx from %d\n", (uint64_t)kvp.key, (uint64_t)kvp.val, src );

		if ( first_kvp ) {
			last_key = kvp.key;
			last_val = kvp.val;
			first_kvp = false;
		} else {

			if ( last_key == kvp.key ) {
				last_val = this->mp_update(last_val, kvp.val);
			} else {
			/*
				if ( last_key > kvp.key ) {
					printf( "StreamMergeReducer_SinglePriority order wrong! %lx %lx @ %lu (%d - %lu -- %lu %lu)\n", (uint64_t)last_key, (uint64_t)kvp.key, cur_count, src, read_offset[src], this->mv_file_offset[src], this->mv_input_sources[src].file->bytes );
				}
			*/

				this->EmitKv(last_key, last_val);
				last_key = kvp.key;
				last_val = kvp.val;
			}
		}
		
		SortReduceTypes::Block block = this->mvq_ready_blocks[src].front();
			
		if ( block.valid_bytes >= read_offset[src] + kv_bytes ) {
			K key = ReducerUtils<K,V>::DecodeKey(block.buffer, read_offset[src]);
			V val = ReducerUtils<K,V>::DecodeVal(block.buffer, read_offset[src]+sizeof(K));
			KvPairSrc kvp;
			kvp.key = key;
			kvp.val = val;
			kvp.src = src;
			m_priority_queue.push(kvp);

			read_offset[src] += kv_bytes;

			/*
			if ( last_key_src[src] > key ) {
				printf( "Source key order wrong %lx %lx @ %d -- %lu\n", (uint64_t)last_key_src[src], (uint64_t)key, src ,read_offset[src] );
			}
			last_key_src[src] = key;
			*/
		} else if ( this->mv_input_sources[src].from_file ) {
			this->GetNextFileBlock(src);

			if ( this->mvq_ready_blocks[src].size() > 1 ) { // If not, we're done!
				size_t debt = read_offset[src] + kv_bytes - block.valid_bytes;
				size_t available = kv_bytes - debt;
					
				this->mvq_ready_blocks[src].pop();
				SortReduceTypes::Block next_block = this->mvq_ready_blocks[src].front();

				if ( next_block.valid && next_block.valid_bytes > 0 ) {
					K key;
					V val;
		
					if ( available >= sizeof(K) ) {
						// cut within V
						key = ReducerUtils<K,V>::DecodeKey(block.buffer, read_offset[src]);
						read_offset[src] += sizeof(K);
						memcpy(&val, ((uint8_t*)block.buffer+read_offset[src]), available-sizeof(K));

						memcpy(((uint8_t*)&val)+available-sizeof(K), next_block.buffer, debt);
						read_offset[src] = debt;
					} else {
						// cut within K
						memcpy(&key, ((uint8_t*)block.buffer+read_offset[src]), available);

						memcpy(((uint8_t*)&key)+available, next_block.buffer, sizeof(K)-available);
						read_offset[src] = sizeof(K)-available;
						val = ReducerUtils<K,V>::DecodeVal(next_block.buffer, read_offset[src]);
						read_offset[src] += sizeof(V);
					}

					KvPairSrc kvp;
					kvp.key = key;
					kvp.val = val;
					kvp.src = src;
					m_priority_queue.push(kvp);

					if ( last_key_src[src] > key ) {
						printf( "Source key order wrong %lx %lx @ %d\n", (uint64_t)last_key_src[src], (uint64_t)key, src );
					}
					last_key_src[src] = key;

				} else {
					//printf( "This should not happen %lu\n", cur_count ); fflush(stdout);
				}
			} else {
				SortReduceTypes::File* file = this->mv_input_sources[src].file;
				this->mp_temp_file_manager->Close(file);
				//printf( "File closed! %2d\n", src ); fflush(stdout);
			}

			buffer_manager->ReturnBuffer(block);
		} else {
			// If from in-memory block, there is no more!
			SortReduceTypes::Block block = this->mv_input_sources[src].block;
			if ( block.managed ) {
				//printf( "Returning managed in-memory sort buffer\n" ); fflush(stdout);
				AlignedBufferManager* managed_buffers = AlignedBufferManager::GetInstance(0);
				managed_buffers->ReturnBuffer(block);
			} else {
				//printf( "Freeing in-memory sort buffer\n" ); fflush(stdout);
				free(block.buffer);
			}
		}
	}

	//flush out_block
	this->EmitKv(last_key, last_val);
	this->EmitFlush();

	//printf( "Reducer done!\n" ); fflush(stdout);

	this->m_done = true;

	std::chrono::high_resolution_clock::time_point now;
	now = std::chrono::high_resolution_clock::now();
	duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-last_time);

	//printf( "StreamMergeReducer_SinglePriority %d elapsed: %lu ms\n", source_count, duration_milli.count() );
}










TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::ReducerUtils)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::StreamFileWriterNode)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::FileWriterNode)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::BlockSource)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::BlockSourceNode)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::ReducerNode)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::ReducerNodeStream)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::MergeReducer)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::MergerNode)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::FileReaderNode)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::BlockReaderNode)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::BlockSourceReader)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::StreamMergeReducer)
TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::StreamMergeReducer_SinglePriority)

