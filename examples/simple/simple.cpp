#include <stdio.h>
#include <stdlib.h>

#include <chrono>
#include <ctime>

#include "sortreduce.h"

uint32_t update_function(uint32_t a, uint32_t b) {
	return a+b;
}

int main(int argc, char** argv) {
	srand(time(0));

	SortReduceTypes::Config<uint32_t,uint32_t>* conf = new SortReduceTypes::Config<uint32_t,uint32_t>(0, 0, "./");
	conf->SetUpdateFunction(&update_function);
	//conf->SetMaxBytesInFlight(1024*1024*1024); //1GB
	conf->SetManagedBufferSize(1024*1024*32, 64); // 2 GB
	
	SortReduce<uint32_t,uint32_t>* sr = new SortReduce<uint32_t,uint32_t>(conf);

	uint32_t* input_buffer = (uint32_t*)aligned_alloc(512,1024*1024*32); //32MB
	for ( uint32_t i = 0; i < 1024*1024*32/sizeof(uint32_t)/2; i++ ) {
		input_buffer[i*2] = rand();
		input_buffer[i*2+1] = 0xcc;
	}
	if ( !sr->PutBlock(input_buffer, 1024*1024*32, true) ) {
		printf( "PutBlock failed\n" ); fflush(stdout);
	}

	sleep(5);
	
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
