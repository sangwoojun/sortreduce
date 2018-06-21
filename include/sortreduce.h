#ifndef __SORTREDUCE_H__
#define __SORTREDUCE_H__

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

#include <fcntl.h>
#include <libaio.h>

#include <time.h>

#include <queue>
#include <string>
#include <thread>

class SortReduce {
public:
	typedef enum {
		MERGER_2TO1,
		MERGER_16TO1,
	} MergerType;
	typedef enum {
		KEY_BINARY32,
		KEY_BINARY64,
	} KeyType;
	typedef enum {
		VAL_BINARY32,
		VAL_BINARY64,
	} ValType;

	class Config {
	public:
		Config(SortReduce::KeyType key_type, SortReduce::ValType val_type, int file_input, int file_output, std::string temporary_directory);
		void SetUpdateFunction(uint32_t (*update32)(uint32_t,uint32_t) );
		void SetUpdateFunction(uint64_t (*update64)(uint64_t,uint64_t) );
	//private:
		SortReduce::MergerType merger_type;

		SortReduce::KeyType key_type;
		SortReduce::ValType val_type;

		int file_input;
		int file_output;

		std::string temporary_directory;

		int maximum_threads;
		size_t buffer_size;

		uint32_t (*update32)(uint32_t,uint32_t);
		uint64_t (*update64)(uint64_t,uint64_t);

	};

	class Status {
	public:
		Status();
		bool done;
	private:
	};


public:
	SortReduce(SortReduce::Config* config);
	void PutBlock(void* buffer, size_t bytes);
	size_t GetBlock(void* buffer);
	SortReduce::Status CheckStatus();
private:
	SortReduce::Config* config;

	int block_sorter_thread_count;
	int merge_sorter_thread_count;
	int reducer_thread_count;

	std::thread manager_thread;

	void ManagerThread();
};

#endif
