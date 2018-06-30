#ifndef __TYPES_H__
#define __TYPES_H__

#include <stdint.h>

#include <atomic>
#include <queue>
#include <string>
#include <thread>

namespace SortReduceTypes {
	typedef struct {
		void* buffer = NULL;
		size_t bytes = 0;
		size_t valid_bytes = 0;

		// Buffers are allocated in the library and reused, instead of free'd
		bool managed = false;
		int managed_idx = -1;

		bool last = false;
	} Block;

	typedef struct {
		int fd;
		//size_t bytes;
	} File;

	typedef struct {
		std::atomic<size_t> bytes_inflight;
	}ComponentStatus;


	template <class K, class V>
	class Config {
	public:
		Config(int file_input, int file_output, std::string temporary_directory);
		void SetUpdateFunction(V (*update)(V,V) );
		//void SetUpdateFunction(uint64_t (*update64)(uint64_t,uint64_t) );
		void SetManagedBufferSize(size_t buffer_size, int buffer_count);
		void SetMaxBytesInFlight(size_t bytes);
	//private:

		int file_input;
		int file_output;

		std::string temporary_directory;

		int maximum_threads;

		V (*update)(V,V);

		size_t buffer_size;
		int buffer_count;

		size_t max_bytes_inflight;
	};

	class Status {
	public:
		Status();
		bool done;
	private:
	};
}

#endif
