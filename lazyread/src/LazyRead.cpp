#include "LazyRead.hpp"

LazyRead::LazyRead(int target_file_fd, size_t target_buffer_bytes, size_t write_buffer_bytes) {
}

void 
LazyRead::StartTarget() {
}

void 
LazyRead::ReadRequest(size_t offset, size_t bytes) {
	printf("...");
}
	
void 
LazyRead::EndTarget() {
}
