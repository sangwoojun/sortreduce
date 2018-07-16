#include <stdio.h>
#include <stdlib.h>

#include <chrono>
#include <ctime>
#include <map>
#include <vector>

#include "sortreduce.h"
#include "types.h"

inline  uint32_t update_function(uint32_t a, uint32_t b) {
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

template<class K, class V>
class InputGenerator{
public:
	InputGenerator(SortReduce<K,V>* sr, uint64_t count, unsigned int seed);
	void WorkerThread();

private:
	SortReduce<K,V>* mp_sr = NULL;
	uint64_t mp_count;
	std::thread m_worker_thread;
	unsigned int m_random_seed;
};

template<class K, class V>
InputGenerator<K,V>::InputGenerator(SortReduce<K,V>* sr, uint64_t count, unsigned int seed) {
	mp_sr = sr;
	mp_count = count;
	m_worker_thread = std::thread(&InputGenerator::WorkerThread, this);
	m_random_seed = seed;
}

template<class K, class V>
void
InputGenerator<K,V>::WorkerThread() {
	typename SortReduce<K,V>::IoEndpoint* ep = mp_sr->GetEndpoint(true);

	printf( "Data input thread started\n" ); fflush(stdout);

	unsigned int rand_seed = m_random_seed;

	for ( uint64_t i = 0; i < mp_count; i++ ) {
		//uint64_t key = 1;
		//uint64_t key = (uint64_t)(rand_r(&rand_seed));
		uint64_t key = i;//(uint64_t)(rand_r(&rand_seed));
		while ( !ep->Update(key, 1) ) {}
	}
	ep->Finish();

	printf( "Data input thread done\n" );
	fflush(stdout);
}


int main(int argc, char** argv) {
	srand(time(0));

	if ( argc < 3 ) {
		fprintf(stderr, "usage: %s thread_count element_count\n", argv[0] );
		exit(1);
	}

	int thread_count = atoi(argv[1]);
	uint64_t element_count = strtoull(argv[2], NULL, 10);

	printf( "Element count: %lu\n", element_count );



	SortReduceTypes::Config<uint64_t,uint32_t>* conf = new SortReduceTypes::Config<uint64_t,uint32_t>("/mnt/md0/wjun/", "output.dat", 16);
	conf->SetUpdateFunction(&update_function);
	conf->SetMaxBytesInFlight(1024*1024*1024); //1GB
	//conf->SetManagedBufferSize(1024*1024*32, 64); // 2 GB
	conf->SetManagedBufferSize(1024*1024*32, 64); // 4 GB

	SortReduce<uint64_t,uint32_t>* sr = new SortReduce<uint64_t,uint32_t>(conf);


	std::chrono::high_resolution_clock::time_point start, end;
	std::chrono::milliseconds duration_milli;



	start = std::chrono::high_resolution_clock::now();

	std::vector<InputGenerator<uint64_t,uint32_t>*> input_generators;
	for ( int i = 0; i < thread_count; i++ ){
		InputGenerator<uint64_t,uint32_t>* ig = new InputGenerator<uint64_t, uint32_t>(sr, element_count, rand());
		input_generators.push_back(ig);
	}

	printf( "Started!\n" ); fflush(stdout);
	

	//for ( uint32_t i = 0; i < 1024*1024*128; i++ ) {
	for ( uint64_t i = 0; i < element_count; i++ ) { //  8*12 GB
		//uint64_t key = (uint64_t)(rand()&0xffff);
		uint64_t key = i;
		//uint64_t key = (uint64_t)(rand());
		//uint64_t key = 1;
		//while ( !sr->Update(key, (1<<16)|1, false) );
		while ( !sr->Update(key, 1) ) {
			printf( "!" );
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
	end = std::chrono::high_resolution_clock::now();
	duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (end-start);


	printf( "Sort-reduce done! Elapsed: %lu ms\n", duration_milli.count() ); fflush(stdout);


	uint64_t last_key = 0;
	std::tuple<uint64_t,uint32_t,bool> kvp = sr->Next();
	while ( std::get<2>(kvp) ) {
		uint64_t key = std::get<0>(kvp);
		//uint32_t val = std::get<1>(kvp);

		if ( last_key > key ) {
			printf( "Result key order wrong %lu %lu\n", last_key, key );
		} else {
			if ( last_key + 1 != key ) {
				printf( "Skipped %lu!\n", last_key+1 );
			}
			last_key = key;
		}

		kvp = sr->Next();
	}

	printf( "Largest key: %lu\n",last_key );




	printf( "All done! Exiting...\n" ); fflush(stdout);
}
