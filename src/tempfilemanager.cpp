#include "tempfilemanager.h"

TempFileManager::TempFileManager(std::string path) {
	m_base_path = "./";
	
	memset(&m_io_ctx, 0, sizeof(m_io_ctx));
	if( io_setup( AIO_DEPTH, &m_io_ctx ) != 0 ) {
		fprintf(stderr, "%s %d io_setup error\n", __FILE__, __LINE__);
	}
	m_base_path = path;
	m_read_ready_count = 0;
	
	for ( int i = 0; i < AIO_DEPTH; i++ ) {
		ma_request_args[i].idx = i;
		mq_free_bufs.push(i);
	}
}


SortReduceTypes::File 
TempFileManager::CreateFile(void* buffer, size_t bytes) {
	int fd = open(m_base_path.c_str(), O_RDWR|O_DIRECT|O_TMPFILE, S_IRUSR|S_IWUSR);

	WaitWrite(fd, buffer, bytes, 0);

	SortReduceTypes::File ret;
	ret.fd = fd;
	//ret.bytes = size;

	return ret;
}

bool 
TempFileManager::Write(int fd, void* buffer, size_t bytes, off_t offset) {
	bool ret = false;
		
	int num_events = io_getevents(m_io_ctx, 0, AIO_DEPTH, ma_events, NULL);

	m_mutex.lock();
	for ( int i = 0; i < num_events; i++ ) {
		struct io_event event = ma_events[i];
		IocbArgs* arg = (IocbArgs*)event.data;
		arg->busy = false;

		if ( arg->write ) {
			mq_free_bufs.push(arg->idx);
			if ( arg->free_buffer_after_done ) {
				free(arg->buffer);
			}
		}
	}

	while ( !mq_read_order_idx.empty() ) {
		int next = mq_read_order_idx.front();
		if ( ma_request_args[next].busy == false ) {
			mq_free_bufs.push(next);
			mq_read_order_idx.pop();
			m_read_ready_count ++;
		} else {
			break;
		}
	}

	if ( !mq_free_bufs.empty() ) {
		int idx = mq_free_bufs.front();
		mq_free_bufs.pop();

		memset(&ma_iocb[idx], 0, sizeof(ma_iocb[idx]));
		io_prep_pwrite(&ma_iocb[idx], fd, buffer, bytes, offset);
		ma_request_args[idx].write = true;
		ma_request_args[idx].busy = true;
		ma_request_args[idx].buffer = buffer;
		ma_request_args[idx].free_buffer_after_done = true;

		ma_iocb[idx].data = &ma_request_args[idx];

		struct iocb* iocbs = &ma_iocb[idx];
		int ret_count = io_submit(m_io_ctx, 1, &iocbs);
		if ( ret_count > 0 ) {
			ret = true;
		} else {
			mq_free_bufs.push(idx);
		}
	}

	m_mutex.unlock();

	return ret;
}

void
TempFileManager::WaitWrite(int fd, void* buffer, size_t bytes, off_t offset) {
	while (!Write(fd, buffer, bytes, offset)) {
		usleep(50);
	}
}

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
Closing O_TMPFILE automatically deletes it
**/
void
TempFileManager::Close(int fd) {
	close(fd);
}

