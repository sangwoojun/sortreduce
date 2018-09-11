#include <stdio.h>
#include <stdlib.h>

#include <chrono>
#include <ctime>
#include <map>

#include "sortreduce.h"
#include "types.h"

uint32_t update_function(uint32_t a, uint32_t b) {
/*
	uint32_t a1 = a & 0xffff;
	uint32_t a2 = (a>>16)&0xffff;
	uint32_t b1 = b & 0xffff;
	uint32_t b2 = (b>>16)&0xffff;
	uint32_t ret = ((a1+b1)&0xffff) | (((a2+b2)&0xffff)<<16);

	//printf( "%x %x -> %x\n", a,b,ret );
*/
	uint32_t ret = a+b;
	return ret;
}

int main(int argc, char** argv) {
	srand(time(0));

	if ( argc < 3 ) {
		fprintf(stderr, "usage: %s directory element_count\n", argv[0] );
		exit(1);
	}

	char* tmp_dir = argv[1];
	uint64_t element_count = strtoull(argv[2], NULL, 10);

	SortReduceTypes::Config<uint64_t,uint32_t>* conf = new SortReduceTypes::Config<uint64_t,uint32_t>(tmp_dir, "t", 16);
	//SortReduceTypes::Config<uint64_t,uint32_t>* conf = new SortReduceTypes::Config<uint64_t,uint32_t>(tmp_dir, "output.dat", 16);
	conf->SetUpdateFunction(&update_function);
	conf->SetMaxBytesInFlight(1024*1024*1024); //1GB
	//conf->SetManagedBufferSize(1024*1024*32, 64); // 2 GB
	conf->SetManagedBufferSize(1024*1024*32, 64); // 4 GB

	std::map<uint64_t,uint32_t> golden_map;
	
	SortReduce<uint64_t,uint32_t>* sr = new SortReduce<uint64_t,uint32_t>(conf);

	printf( "Started!\n" ); fflush(stdout);

	//for ( uint32_t i = 0; i < 1024*1024*128; i++ ) {
	for ( uint64_t i = 0; i < element_count; i++ ) { //  12 GB
		uint64_t key = (uint64_t)(rand()&0xfffff);
		//uint64_t key = 1;
		//while ( !sr->Update(key, (1<<16)|1, false) );
		while ( !sr->Update(key, 1) ) {
			//printf( "!" );
		}

		if ( golden_map.find(key) == golden_map.end() ) {
			golden_map[key] = 1;
		} else {
			uint32_t v = golden_map[key];
			golden_map[key] = v+1;
		}
	}
	sr->Finish();

	printf( "Input done\n" ); fflush(stdout);

	SortReduceTypes::Status status = sr->CheckStatus();
	while ( status.done_external == false ) {
		sleep(1);
		status = sr->CheckStatus();
		printf( "%s %s:%d-%d %s:%d-%d\n", status.done_input?"yes":"no", status.done_inmem?"yes":"no",status.internal_count, status.sorted_count, status.done_external?"yes":"no", status.external_count, status.file_count );
		fflush(stdout);
	}

	printf( "All done!\n" ); fflush(stdout);


	uint64_t last_key = 0;
	uint64_t total_count = 0;
	uint64_t nonexist_count = 0;
	uint64_t mismatch_count = 0;
	std::tuple<uint64_t,uint32_t,bool> kvp = sr->Next();
	while ( std::get<2>(kvp) ) {
		uint64_t key = std::get<0>(kvp);
		uint32_t val = std::get<1>(kvp);
		total_count ++;

		//printf( "%lx %x\n", key, val );

		if ( last_key > key ) {
			printf( "Result key order wrong %lx %lx -- %x\n", last_key, key, val );
		}
		last_key = key;

		if ( golden_map.find(key) == golden_map.end() ) {
			nonexist_count ++;
			printf( "nonexist %lx -- %x\n", key, val );
		} else {
			if ( golden_map[key] != val ) {
				mismatch_count ++;
				printf( "mismatch %lx: %x -- %x\n", key, golden_map[key], val );
			}
			golden_map.erase(golden_map.find(key));
		}

		kvp = sr->Next();
	}

	printf( "total: %lu \nnonexist: %lu \nmismatch: %lu \nleft: %lu\n", total_count, nonexist_count, mismatch_count, golden_map.size() );




	/*

	uint32_t* input_buffer = (uint32_t*)aligned_alloc(512,1024*1024*32); //32MB
	for ( uint32_t i = 0; i < 1024*1024*32/sizeof(uint32_t)/2; i++ ) {
		input_buffer[i*2] = rand();
		input_buffer[i*2+1] = 0xcc;
	}
	if ( !sr->PutBlock(input_buffer, 1024*1024*32, true) ) {
		printf( "PutBlock failed\n" ); fflush(stdout);
	}

	sleep(5);
	*/
	/*
	uint32_t last_key = 0;
	for ( uint32_t i = 0; i < 1024*1024*32/sizeof(uint32_t)/2; i++ ) {
		uint32_t key = input_buffer[i*2];
		if ( key < last_key ) {
			printf( "Wrong order! %10d %10d\n", last_key, key );
			break;
		}
		last_key = key;
	}
	*/
	
}
