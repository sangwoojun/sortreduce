#ifndef __TEMPFILEMANAGER_H__
#define __TEMPFILEMANAGER_H__

#include <string>
#include <queue>
#include <map>
#include <mutex>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

#include <fcntl.h>
#include <libaio.h>

#include "types.h"

#define AIO_DEPTH 128

class TempFileManager {
public:
	TempFileManager(std::string path);

	SortReduceTypes::File CreateFile(void* buffer, size_t bytes, size_t valid_bytes, bool free_buffer_after_done);
	bool Write(int fd, void* buffer, size_t bytes, size_t valid_bytes, off_t offset, bool free_buffer_after_done);
	void WaitWrite(int fd, void* buffer, size_t bytes, size_t valid_bytes, off_t offset, bool free_buffer_after_done);
	void Close(int fd);

	int CountInFlight();
	int CountFreeBuffers();

	void CheckDone();
	size_t BytesInFlight() { return m_writing_bytes; };
private:
	std::mutex m_mutex;

	std::string m_base_path;

	io_context_t m_io_ctx;
	struct io_event ma_events[AIO_DEPTH];
	struct iocb ma_iocb[AIO_DEPTH];
	std::queue<int> mq_free_bufs;
	typedef struct {
		int idx;
		bool write;
		bool busy;

		void* buffer;
		size_t bytes;
		//TODO If free_buffer_after_done is not set, return to buffer pool
		bool free_buffer_after_done;
	} IocbArgs;
	IocbArgs ma_request_args[AIO_DEPTH];
	std::queue<int> mq_read_order_idx;

	int m_read_ready_count;

	// Size (in bytes) of buffers waiting to be written to storage
	size_t m_writing_bytes;

};


#endif
