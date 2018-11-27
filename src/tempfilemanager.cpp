#include "tempfilemanager.h"


uint32_t
TempFileManager::ms_filename_idx = 0;

TempFileManager::TempFileManager(std::string path, bool verbose) {
	m_verbose = verbose;
	m_base_path = "./";
	m_read_ready_count = 0;
	
	memset(&m_io_ctx, 0, sizeof(m_io_ctx));
	if( io_setup( AIO_DEPTH, &m_io_ctx ) != 0 ) {
		fprintf(stderr, "%s %d io_setup error\n", __FILE__, __LINE__);
	}
	m_base_path = path;
	
	for ( int i = 0; i < AIO_DEPTH; i++ ) {
		ma_request_args[i].idx = i;
		mq_free_bufs.push(i);
	}
}

TempFileManager::~TempFileManager() {
	CheckDone();
	while ( mq_free_bufs.size() != AIO_DEPTH ) {
		CheckDone();
		printf( "TempFileManager destructor still waiting for results %ld\n", AIO_DEPTH-mq_free_bufs.size() );
	}
	io_destroy(m_io_ctx);
}


SortReduceTypes::File*
TempFileManager::CreateEmptyFile(std::string filename) {
	//printf( "Creating empty file\n" ); fflush(stdout);
	
	m_mutex.lock();
	
	// FIXME
	char tmp_filename[128];
	if ( m_base_path.length() + filename.length() + 16 >= 128 ) {
		fprintf(stderr, "temp file path too long! FIXME %s:%d\n", __FILE__, __LINE__ );
	}

	int fd = -1;
	if ( filename == "" ) {
		sprintf(tmp_filename, "%s/tmp_%09u.bin", m_base_path.c_str(), ms_filename_idx);
		ms_filename_idx++;
		fd = open(tmp_filename, O_RDWR|O_DIRECT|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
	} else {
		sprintf(tmp_filename, "%s/%s", m_base_path.c_str(), filename.c_str());
		fd = open(tmp_filename, O_RDWR|O_DIRECT|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
	}
	
	m_mutex.unlock();

	if ( fd < 0 ) {
		printf( "TempFileManager CreateEmptyFile error withd %s %d\n", strerror(errno), errno );
	}


	SortReduceTypes::File* ret = new SortReduceTypes::File();
	ret->fd = fd;
	ret->bytes = 0;
	ret->path = tmp_filename;


	return ret;
}


bool 
TempFileManager::Write(SortReduceTypes::File* file, SortReduceTypes::Block block, size_t offset) {
	bool ret = false;

	int fd = file->fd;
	
	CheckDone();
	
	m_mutex.lock();
	

	size_t bytes = block.valid_bytes;
	if ( !mq_free_bufs.empty() ) {
		int idx = mq_free_bufs.front();
		mq_free_bufs.pop();
		
		size_t frag = bytes & 0x1ff;
		if ( frag != 0 ) {
			bytes = bytes - frag + 0x200;
		}
		size_t ofrag = offset & 0x1ff;
		if ( ofrag != 0 ) {
			offset = bytes-frag;
		}

		memset(&ma_iocb[idx], 0, sizeof(ma_iocb[idx]));
		io_prep_pwrite(&ma_iocb[idx], fd, block.buffer, bytes, offset);

		IocbArgs* args = &ma_request_args[idx];
		args->bytes = block.valid_bytes;
		args->buffer = block.buffer;
		args->write = true;
		args->busy = true;
		//args->free_buffer_after_done = !block.managed;
		args->block = block;
		args->file = file;

		ma_iocb[idx].data = args;

		struct iocb* iocbs = &ma_iocb[idx];
		int ret_count = io_submit(m_io_ctx, 1, &iocbs);
		if ( ret_count > 0 ) {
			ret = true;
			if ( m_verbose ) {
				printf( "Writing file %lx size %lx (%lx) -- %lu success: %s\n", offset, block.valid_bytes, bytes, mq_free_bufs.size(), ret?"yes":"no" );
				fflush(stdout);
			}
		} else {
			if ( ret_count < 0 ) {
				//printf( "TempFileManager write returned %d\n", ret_count );
			}
			mq_free_bufs.push(idx);
		}
	}

	m_mutex.unlock();
	

	return ret;
}

/**
Returns -1 on fail
Returns tag on success
**/
int 
TempFileManager::Read(SortReduceTypes::File* file, size_t offset, size_t bytes, void* buffer) {
	int ret = -1;

	int fd = file->fd;

	if ( fd < 0 ) {
		fd = open(file->path.c_str(), O_RDONLY|O_DIRECT, S_IRUSR|S_IWUSR);
		file->fd = fd;
	}

	CheckDone();
	
	m_mutex.lock();

	if ( !mq_free_bufs.empty() ) {
		int idx = mq_free_bufs.front();
		mq_free_bufs.pop();


		size_t frag = bytes & 0x1ff;
		if ( frag != 0 ) {
			bytes = bytes-frag + 0x200;
		}
		size_t ofrag = offset & 0x1ff;
		if ( ofrag != 0 ) {
			offset = offset-ofrag;
			printf("WARNING: TempFileManager::Read offset should always be 512 bytes aligned!\n");
			fflush(stdout);
		}

		//memset(buffer, 0xcc, bytes);

		memset(&ma_iocb[idx], 0, sizeof(ma_iocb[idx]));
		io_prep_pread(&ma_iocb[idx], fd, buffer, bytes, offset);

		IocbArgs* args = &ma_request_args[idx];
		args->bytes = bytes;
		args->buffer = buffer;
		args->write = false;
		args->busy = true;
		args->file = file;

		ma_iocb[idx].data = args;

		struct iocb* iocbs = &ma_iocb[idx];
		int ret_count = io_submit(m_io_ctx, 1, &iocbs);
		if ( ret_count > 0 ) {
			ret = idx;
			mq_read_order_idx.push(idx);

			if ( m_verbose ) {
				printf( "Reading file %lx size %lx -- %lu success: %d\n", offset, bytes, mq_free_bufs.size(), ret ); 
				fflush(stdout);
			}

		} else {
			if ( ret_count < 0 ) {
				//printf( "TempFileManager Read returned %d\n", ret_count );
			}
			mq_free_bufs.push(idx);
		}
	}

	m_mutex.unlock();

	return ret;
}

/**
Returns number of done reads
**/
int
TempFileManager::ReadStatus(bool clear) {
	int ret = 0;
	
	CheckDone();
	
	m_mutex.lock();
	ret = m_read_ready_count;

	if ( clear ) {
		m_read_ready_count -= ret;
	}

	m_mutex.unlock();

	return ret;
}


void
TempFileManager::CheckDone() {
	int num_events = io_getevents(m_io_ctx, 0, AIO_DEPTH, ma_events, NULL);

	m_mutex.lock();
	for ( int i = 0; i < num_events; i++ ) {
		struct io_event event = ma_events[i];
		IocbArgs* arg = (IocbArgs*)event.data;
		arg->busy = false;
			
		if ( arg->write ) {
			mq_free_bufs.push(arg->idx);
			if ( arg->file != NULL ) arg->file->bytes += arg->bytes;
			else {
				printf( "Write used without file argument!\n" ); fflush(stdout);
			}

			if ( arg->block.managed ) {
				AlignedBufferManager* buffer_manager = AlignedBufferManager::GetInstance(1);
				buffer_manager->ReturnBuffer(arg->block);
			} else {
				free(arg->buffer);
			}
		}
	}

	while ( !mq_read_order_idx.empty() ) {
		int next = mq_read_order_idx.front();
		if ( ma_request_args[next].busy == false ) {
			mq_free_bufs.push(next);
			//printf( "Read done!\n" ); fflush(stdout);
			mq_read_order_idx.pop();
			m_read_ready_count ++;
		} else {
			break;
		}
	}
	m_mutex.unlock();
}

/*
void
TempFileManager::WaitWrite(int fd, void* buffer, size_t bytes, size_t valid_bytes, off_t offset, bool free_buffer_after_done) {
	while (!Write(fd, buffer, bytes, valid_bytes, offset, free_buffer_after_done)) {
		usleep(50);
	}
}
*/

int
TempFileManager::CountInFlight() {
	m_mutex.lock();
	int ret = AIO_DEPTH - mq_free_bufs.size();
	m_mutex.unlock();
	return ret;
}

int
TempFileManager::CountFreeBuffers() {
	m_mutex.lock();
	int ret = mq_free_bufs.size();
	m_mutex.unlock();
	return ret;
}

/**
**/
void
TempFileManager::Close(SortReduceTypes::File* file) {
	unlink(file->path.c_str());
	if ( file->fd >= 0 ) {
		close(file->fd);
	}
}

