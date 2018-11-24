#ifndef __FILEREADERAIO_H__
#define __FILEREADERAIO_H__

#include <queue>
#include <mutex>

#include <time.h>
#include <fcntl.h>
#include <libaio.h>

//NOTE NOT THREAD SAFE
template <class V>
class FileReaderAio {
	public:
		FileReaderAio(std::string path, size_t buffer_bytes=1024*128, size_t depth = 1024*1024);
		FileReaderAio(int fd, size_t buffer_bytes=1024*128, size_t depth = 1024*1024);
		~FileReaderAio();
		bool ReadReq(size_t idx);
		bool Available();
		V GetNext();
		size_t GetInFlight() { return m_inflight; };
	private:
		int m_fd;
		io_context_t m_io_ctx;
		typedef struct {
			int buf_idx;
			size_t offset;
			size_t bytes;
			void* buffer;
			bool done;
		} IocbArgs;
		std::queue<int> mq_aio_order_read;
		std::queue<int> mq_free_iocb;
		size_t m_req_depth;
		size_t m_buffer_bytes;

		std::queue<size_t> mq_req_offset;
		std::queue<V> mq_read_result;


		static const int m_aio_depth = 128;
		struct io_event ma_events[m_aio_depth];
		struct iocb ma_iocb[m_aio_depth];
		IocbArgs ma_request_args[m_aio_depth];

		size_t m_req_buffer_offset;
		size_t m_req_buffer_bytes;
		size_t m_resp_buffer_offset;
		size_t m_resp_buffer_bytes;
		void* mp_resp_buffer;
		int m_resp_arg_idx;

		size_t m_inflight;

};


#endif
