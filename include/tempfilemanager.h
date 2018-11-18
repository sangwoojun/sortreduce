#ifndef __TEMPFILEMANAGER_H__
#define __TEMPFILEMANAGER_H__

#include <string>
#include <queue>
#include <map>
#include <mutex>
#include <atomic>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

#include <fcntl.h>
#include <libaio.h>

#include "types.h"
#include "alignedbuffermanager.h"

#define AIO_DEPTH 128

class TempFileManager {
public:
	TempFileManager(std::string path, bool verbose = false);
	~TempFileManager();

	//SortReduceTypes::File* CreateFile(void* buffer, size_t bytes, size_t valid_bytes, bool free_buffer_after_done);
	SortReduceTypes::File* CreateEmptyFile(std::string filename = "");
	//bool Write(int fd, void* buffer, size_t bytes, size_t valid_bytes, off_t offset, bool free_buffer_after_done);
	bool Write(SortReduceTypes::File* file, SortReduceTypes::Block block, size_t offset);
	int Read(SortReduceTypes::File* file, size_t offset, size_t bytes, void* buffer);
	int ReadStatus(bool clear);
	//int PeekReadStatus();

	void Close(SortReduceTypes::File* file);

	int CountInFlight();
	int CountFreeBuffers();

	void CheckDone();
private:
	bool m_verbose;
	std::mutex m_mutex;

	std::string m_base_path;

	static uint32_t ms_filename_idx;

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

		SortReduceTypes::Block block;
		SortReduceTypes::File* file = NULL;
	} IocbArgs;
	IocbArgs ma_request_args[AIO_DEPTH];

	std::queue<int> mq_read_order_idx;
	int m_read_ready_count;



	// Size (in bytes) of buffers waiting to be written to storage
};


#endif
