#ifndef __TYPES_H__
#define __TYPES_H__

#include <stdint.h>

#include <atomic>
#include <queue>
#include <string>
#include <thread>

namespace SortReduceTypes {
	typedef enum {
		KEY_BINARY32,
		KEY_BINARY64,
	} KeyType;
	typedef enum {
		VAL_BINARY32,
		VAL_BINARY64,
	} ValType;

	typedef struct {
		void* buffer;
		size_t bytes;

		// Buffers are allocated in the library and reused, instead of free'd
		bool managed;
	} Block;

	typedef struct {
		int fd;
		//size_t bytes;
	} File;

	typedef struct {
		std::atomic<size_t> bytes_inflight;
	}ComponentStatus;


	class Config {
	public:
		Config(SortReduceTypes::KeyType key_type, SortReduceTypes::ValType val_type, int file_input, int file_output, std::string temporary_directory);
		void SetUpdateFunction(uint32_t (*update32)(uint32_t,uint32_t) );
		void SetUpdateFunction(uint64_t (*update64)(uint64_t,uint64_t) );
		void SetManagedBufferSize(size_t buffer_size, int buffer_count);
		void SetMaxBytesInFlight(size_t bytes);
	//private:

		SortReduceTypes::KeyType key_type;
		SortReduceTypes::ValType val_type;

		int file_input;
		int file_output;

		std::string temporary_directory;

		int maximum_threads;

		uint32_t (*update32)(uint32_t,uint32_t);
		uint64_t (*update64)(uint64_t,uint64_t);

		size_t buffer_size;
		int buffer_count;

		size_t max_bytes_inflight;
	};
}

#endif
