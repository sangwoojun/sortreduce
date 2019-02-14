#ifndef __LAZYREAD_H__
#define __LAZYREAD_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

class LazyRead {
public:
	LazyRead(int target_file_fd, size_t target_buffer_bytes, size_t write_buffer_bytes);
	void StartTarget();
	void ReadRequest(size_t offset, size_t bytes);
	void EndTarget();
private:
};

#endif
