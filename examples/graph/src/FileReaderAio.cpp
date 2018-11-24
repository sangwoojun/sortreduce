#include "FileReaderAio.h"

template <class V>
FileReaderAio<V>::FileReaderAio(std::string path, size_t buffer_bytes, size_t depth) {
	m_req_depth = depth;
	m_buffer_bytes = buffer_bytes;


	m_req_buffer_offset = 0;
	m_req_buffer_bytes = 0;
	m_resp_buffer_offset = 0;
	m_resp_buffer_bytes = 0;

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
FileReaderAio<V>::~FileReaderAio() {
	for ( int i = 0; i < m_aio_depth; i++ ) {
		free(ma_request_args[i].buffer);
	}
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

	return true;
}
template <class V>
inline V 
FileReaderAio<V>::GetNext() {
	return 0;
}

template class FileReaderAio<uint64_t>;
template class FileReaderAio<uint32_t>;
