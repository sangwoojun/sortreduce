#ifndef __TYPES_H__
#define __TYPES_H__

#include <stdint.h>

#include <atomic>
#include <queue>
#include <string>
#include <thread>

#ifdef KVTYPES1
#define TEMPLATE_EXPLICIT_INSTANTIATION1(X) template class X<KVTYPES1>;
#else 
#define TEMPLATE_EXPLICIT_INSTANTIATION1(X) ;
#endif

#ifdef KVTYPES2
#define TEMPLATE_EXPLICIT_INSTANTIATION2(X) template class X<KVTYPES2>;
#else 
#define TEMPLATE_EXPLICIT_INSTANTIATION2(X) 
#endif

#ifdef KVTYPES3
#define TEMPLATE_EXPLICIT_INSTANTIATION3(X) template class X<KVTYPES3>;
#else 
#define TEMPLATE_EXPLICIT_INSTANTIATION3(X) 
#endif


#ifdef KVTYPES4
#define TEMPLATE_EXPLICIT_INSTANTIATION4(X) template class X<KVTYPES4>;
#else 
#define TEMPLATE_EXPLICIT_INSTANTIATION4(X) 
#endif

#define TEMPLATE_EXPLICIT_INSTANTIATION(X) \
TEMPLATE_EXPLICIT_INSTANTIATION1(X) \
TEMPLATE_EXPLICIT_INSTANTIATION2(X) \
TEMPLATE_EXPLICIT_INSTANTIATION3(X) \
TEMPLATE_EXPLICIT_INSTANTIATION4(X)


namespace SortReduceTypes {
	typedef struct {
		void* buffer = NULL;
		size_t bytes = 0;
		size_t valid_bytes = 0;

		// Buffers are allocated in the library and reused, instead of free'd
		int managed_idx = -1;
		bool managed = false;

		//when used, do not use buffer and bytes
		bool last = false;

		// Used to indicate null values (e.g., return from empty queue)
		// Blocks with no data should be indicated with bytes=0
		bool valid = false;
	} Block;

	class File {
	public:
		int fd;
		size_t bytes;
		std::string path;
	};
	class CompareFileSize {
	public:
		bool operator() (File* a, File* b) {
			return (a->bytes > b->bytes); // This ordering feels wrong, but this is correct

		}
	};

	typedef struct {
		std::atomic<size_t> bytes_inflight;
	} ComponentStatus;


	template <class K, class V>
	class Config {
	public:
		Config(std::string temporary_directory, std::string output_filename = "", int max_threads = -1);
		void SetUpdateFunction(V (*update)(V,V) );
		//void SetUpdateFunction(uint64_t (*update64)(uint64_t,uint64_t) );
		void SetManagedBufferSize(size_t buffer_size, int buffer_count);
		void SetMaxBytesInFlight(size_t bytes);
		void SetMaxStorageBytes(size_t bytes) {max_storage_allocatd_bytes = bytes;};
	//private:

		std::string temporary_directory;
		std::string output_filename;

		int maximum_threads;

		V (*update)(V,V);

		size_t buffer_size;
		int buffer_count;

		size_t max_bytes_inflight;

		size_t max_storage_allocatd_bytes = 0;
	};

	template <class K, class V>
	struct KvPair {
		K key;
		V val;
	};
	template <class K, class V>
	struct KvPairSrc {
		K key;
		V val;
		int src;
	};

	class Status {
	public:
		Status();
		bool done_input;
		bool done_inmem;
		bool done_external;

		int external_count;
		int internal_count;

		int sorted_count;
		int file_count;
		File* done_file;
	private:
	};
}

#endif
