#include <stdio.h>
#include <stdlib.h>

#include "sortreduce.h"

uint32_t update_function(uint32_t a, uint32_t b) {
	return a+b;
}

int main(int argc, char** argv) {
	srand(time(0));

	SortReduce::Config* conf = new SortReduce::Config(SortReduceTypes::KEY_BINARY32, SortReduceTypes::VAL_BINARY32, 0, 0, "./");
	conf->SetUpdateFunction(&update_function);
	
	SortReduce* sr = new SortReduce(conf);

	uint32_t* input_buffer = (uint32_t*)aligned_alloc(512,1024*1024*32); //32MB
	for ( uint32_t i = 0; i < 1024*1024*32/sizeof(uint32_t)/2; i++ ) {
		input_buffer[i*2] = rand();
		input_buffer[i*2+1] = 0xcc;
	}
	sr->PutBlock(input_buffer, 1024*1024*32);

	sleep(5);
	
	
}
