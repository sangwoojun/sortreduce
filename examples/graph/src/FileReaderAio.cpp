#include "FileReaderAio.h"

template <class V>
FileReaderAio<V>::FileReaderAio(std::string path, size_t buffer_bytes, size_t depth) {
	m_req_depth = depth;
	m_buffer_bytes = buffer_bytes;


	m_req_buffer_offset = 0;
	m_req_buffer_bytes = 0;
	m_resp_buffer_offset = 0;
	m_resp_buffer_bytes = 0;

	m_resp_arg_idx = -1;
	m_inflight = 0;

	m_fd = open(path.c_str(), O_RDONLY|O_DIRECT, S_IRUSR|S_IWUSR);

	memset(&m_io_ctx, 0, sizeof(m_io_ctx));
	if( io_setup( m_aio_depth, &m_io_ctx ) != 0 ) {
		fprintf(stderr, "%s %d io_setup error\n", __FILE__, __LINE__);
		return;
	}
	for ( int i = 0; i < m_aio_depth; i++ ) {
		ma_request_args[i].buffer = aligned_alloc(512, m_buffer_bytes);
		mq_free_iocb.push(i);
	}
}

template <class V>
FileReaderAio<V>::FileReaderAio(int fd, size_t buffer_bytes, size_t depth) {
	m_req_depth = depth;
	m_buffer_bytes = buffer_bytes;


	m_req_buffer_offset = 0;
	m_req_buffer_bytes = 0;
	m_resp_buffer_offset = 0;
	m_resp_buffer_bytes = 0;

	m_resp_arg_idx = -1;
	m_inflight = 0;

	m_fd = fd;

	memset(&m_io_ctx, 0, sizeof(m_io_ctx));
	if( io_setup( m_aio_depth, &m_io_ctx ) != 0 ) {
		fprintf(stderr, "%s %d io_setup error\n", __FILE__, __LINE__);
		return;
	}
	for ( int i = 0; i < m_aio_depth; i++ ) {
		ma_request_args[i].buffer = aligned_alloc(512, m_buffer_bytes);
		mq_free_iocb.push(i);
	}
}

template <class V>
FileReaderAio<V>::~FileReaderAio() {
	for ( int i = 0; i < m_aio_depth; i++ ) {
		free(ma_request_args[i].buffer);
	}
	io_destroy(m_io_ctx);
}

template <class V>
inline bool 
FileReaderAio<V>::ReadReq(size_t idx) {
	if ( mq_req_offset.size() >= m_req_depth ) return false;

	size_t byte_offset = idx * sizeof(V);
	if ( byte_offset < m_req_buffer_offset || byte_offset + sizeof(V) > m_req_buffer_offset + m_req_buffer_bytes ) {
		if ( mq_free_iocb.empty() ) return false;

		int idx = mq_free_iocb.front();
				
		size_t offset_aligned = byte_offset-(byte_offset%m_buffer_bytes);

		IocbArgs* args = &ma_request_args[idx];
		io_prep_pread(&ma_iocb[idx], m_fd, args->buffer, m_buffer_bytes, offset_aligned);
		args->buf_idx = idx;
		args->offset = offset_aligned;
		args->bytes = m_buffer_bytes;
		args->done = false;
		ma_iocb[idx].data = args;
		struct iocb* iocbs = &ma_iocb[idx];
		int ret_count = io_submit(m_io_ctx, 1, &iocbs);
		if ( ret_count > 0 ) {
			mq_free_iocb.pop();
			mq_aio_order_read.push(idx);

			m_req_buffer_offset = offset_aligned;
			m_req_buffer_bytes = m_buffer_bytes;
		} else {
			return false;
		}
	}

	mq_req_offset.push(byte_offset);
	m_inflight++;

	return true;
}
template <class V>
inline bool
FileReaderAio<V>::Available() {
	if ( !mq_read_result.empty() ) return true;

	int num_events = io_getevents(m_io_ctx, 0, m_aio_depth, ma_events, NULL);
	for ( int i = 0; i < num_events; i++ ) {
		struct io_event event = ma_events[i];
		IocbArgs* arg = (IocbArgs*)event.data;
		arg->done = true;
	}


	while ( !mq_req_offset.empty() ) {
		size_t off = mq_req_offset.front();

		if ( off < m_resp_buffer_offset || off +sizeof(V) > m_resp_buffer_offset+m_resp_buffer_bytes ) {
			if ( mq_aio_order_read.empty() ) return !mq_read_result.empty();
			int aio_idx = mq_aio_order_read.front();
			
			if ( ma_request_args[aio_idx].done ) {
				mq_aio_order_read.pop();
				ma_request_args[aio_idx].done = false;

				if ( m_resp_arg_idx >= 0 ) {
					mq_free_iocb.push(m_resp_arg_idx);
				}

				IocbArgs* arg = &ma_request_args[aio_idx];
				m_resp_buffer_offset = arg->offset;
				m_resp_buffer_bytes = arg->bytes;
				mp_resp_buffer = arg->buffer;
				m_resp_arg_idx = aio_idx;

			} else {
				return !mq_read_result.empty();
			}
		}
		
		mq_req_offset.pop();

		size_t internal_offset = off - m_resp_buffer_offset;
		V vi = *((V*)((uint8_t*)mp_resp_buffer + internal_offset));

		mq_read_result.push(vi);

	}
	return !mq_read_result.empty();
}

template <class V>
inline V 
FileReaderAio<V>::GetNext() {
	V res = mq_read_result.front();
	mq_read_result.pop();
	m_inflight--;
	return res;
}

template class FileReaderAio<uint64_t>;
template class FileReaderAio<uint32_t>;
