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
		int managed_idx = -1;
		bool managed = false;

		bool last = false;

		// Used to indicate null values (e.g., return from empty queue)
		// Blocks with no data should be indicated with bytes=0
		bool valid = false;
	} Block;

	class File {
	public:
		int fd;
		size_t bytes;
	};
	class CompareFileSize {
	public:
		bool operator() (File* a, File* b) {
			return (a->bytes < b->bytes);
		}
	};

	typedef struct {
		std::atomic<size_t> bytes_inflight;
	} ComponentStatus;


	template <class K, class V>
	class Config {
	public:
		Config(std::string temporary_directory, std::string output_filename = "");
		void SetUpdateFunction(V (*update)(V,V) );
		//void SetUpdateFunction(uint64_t (*update64)(uint64_t,uint64_t) );
		void SetManagedBufferSize(size_t buffer_size, int buffer_count);
		void SetMaxBytesInFlight(size_t bytes);
	//private:

		std::string temporary_directory;
		std::string output_filename;

		int maximum_threads;

		V (*update)(V,V);

		size_t buffer_size;
		int buffer_count;

		size_t max_bytes_inflight;
	};

	class Status {
	public:
		Status();
		bool done_input;
		bool done_inmem;
		bool done_external;

		int external_count;
		File* done_file;
	private:
	};
}

#endif
