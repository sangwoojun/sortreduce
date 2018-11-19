#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <chrono>
#include <ctime>
#include <map>
#include <tuple>

#include "sortreduce.h"
#include "types.h"

#include "EdgeProcess.h"
#include "VertexValues.h"

inline uint32_t vertex_update(uint32_t a, uint32_t b) {
	uint32_t ret = a+b;
	return ret;
}
inline uint32_t edge_program(uint32_t vid, uint32_t value, uint32_t fanout) {
	return vid;
}

inline bool is_active(uint32_t old, uint32_t newv, bool marked) {
	//printf( "Comparing %lx %lx %s\n", old, newv, marked?"Y":"N" );
	if ( old != 0xffffffff ) return false;
	return true;
}

int main(int argc, char** argv) {
	srand(time(0));

	if ( argc < 3 ) {
		fprintf(stderr, "usage: %s ridx matrix directory \n", argv[0] );
		exit(1);
	}

	char* tmp_dir = argv[1];
	char* idx_path = argv[2];
	char* mat_path = argv[3];


	SortReduceTypes::Config<uint32_t,uint32_t>* conf =
		new SortReduceTypes::Config<uint32_t,uint32_t>(tmp_dir, "", 8);
	conf->SetUpdateFunction(&vertex_update);
	conf->SetManagedBufferSize(1024*1024*8, 256); // 4 GiB

	SortReduce<uint32_t,uint32_t>* sr = new SortReduce<uint32_t,uint32_t>(conf);
	SortReduce<uint32_t,uint32_t>::IoEndpoint* ep = sr->GetEndpoint(true);




	VertexValues<uint32_t,uint32_t>* vertex_values = new VertexValues<uint32_t,uint32_t>(tmp_dir, 1024*1024*32, 0xffffffff, &is_active);
	EdgeProcess<uint32_t,uint32_t>* edge_process = new EdgeProcess<uint32_t,uint32_t>(idx_path, mat_path, &edge_program, ep);
	for ( int i = 0; i < 1024; i++ ) {
		edge_process->SourceVertex(i,9);
	}
	ep->Finish();
	sr->Finish();
	printf( "Input done\n" );

	SortReduceTypes::Status status = sr->CheckStatus();
	while ( status.done_external == false ) {
		sleep(1);
		status = sr->CheckStatus();
		printf( "%s %s:%d-%d %s:%d-%d\n",
			status.done_input ? "yes":"no",
			status.done_inmem ? "yes":"no",
			status.internal_count, status.sorted_count,
			status.done_external ? "yes":"no",
			status.external_count, status.file_count);
		fflush(stdout);
	}

	printf( "Done?\n" );
	std::tuple<uint32_t,uint32_t,bool> res = sr->Next();
	while ( std::get<2>(res) ) {
		uint32_t key = std::get<0>(res);
		uint32_t val = std::get<1>(res);
		vertex_values->Update(key,val);
		res = sr->Next();
	}

	exit(0);

	uint64_t element_count = 1024;

	for ( uint64_t i = 0; i < element_count; i++ ) {
		vertex_values->Update(i*7, i);
	}
	vertex_values->NextIteration();
	printf( "Next\n" );
	for ( uint64_t i = 0; i < element_count; i++ ) {
		vertex_values->Update(i*17, i);
	}


	printf( "Done?\n" );
	exit(0);

/*
	SortReduceTypes::Config<uint64_t,uint32_t>* conf =
		new SortReduceTypes::Config<uint64_t,uint32_t>(tmp_dir, "out.sr", 8);
	conf->SetUpdateFunction(&vertex_update);
	conf->SetManagedBufferSize(1024*1024*8, 256); // 4 GiB


	std::map<uint64_t,uint32_t> golden_map;
	SortReduce<uint64_t,uint32_t>* sr = new SortReduce<uint64_t,uint32_t>(conf);

	printf( "Started!\n" ); fflush(stdout);

	uint64_t different_items = 0;
	for ( uint64_t i = 0; i < element_count; i++ ) {
		uint64_t key = (uint64_t)(rand()&0xfffff);
		while ( !sr->Update(key, 1) ) {
		}

		if (golden_map.find(key) == golden_map.end()) {
			golden_map[key] = 1;
			different_items++;
		} else {
			uint32_t v = golden_map[key]+1;
			if (v == 0) {
				v = -1;
				std::cout << "WARNING: Overflow prevented in the golden_map\n";
			}
			golden_map[key] = v;
		}
	}
	sr->Finish();

	fflush(stdout);
	sleep(2); // If not it seg. faults
	fflush(stdout);

	std::cout << "Input done, added " << different_items
			 << " different items for a total of " << element_count << " items.\n";
	fflush(stdout);

	SortReduceTypes::Status status = sr->CheckStatus();
	while ( status.done_external == false ) {
		sleep(1);
		status = sr->CheckStatus();
		printf( "%s %s:%d-%d %s:%d-%d\n",
			status.done_input ? "yes":"no",
			status.done_inmem ? "yes":"no",
			status.internal_count, status.sorted_count,
			status.done_external ? "yes":"no",
			status.external_count, status.file_count);
		fflush(stdout);
	}
	printf( "All done!\n" );

	uint64_t last_key = 0;
	uint64_t total_sum   = 0;
	uint64_t total_count = 0;
	uint64_t nonexist_count = 0;
	uint64_t mismatch_count = 0;
	int64_t mismatch_diff  = 0;
	uint64_t correct_count = 0;
	std::tuple<uint64_t,uint32_t,bool> kvp = sr->Next();
	while ( std::get<2>(kvp) ) {
		uint64_t key = std::get<0>(kvp);
		uint32_t val = std::get<1>(kvp);
		total_count ++;
		total_sum += val;

		if ( last_key > key ) {
			printf( "ERROR: %lx Result key order wrong %lx %lx -- %x\n", total_count, last_key, key, val );
		}
		last_key = key;

		if ( golden_map.find(key) == golden_map.end() ) {
			nonexist_count ++;
			printf( "ERROR: %lx nonexist %lx -- %x\n", total_count, key, val );
		} else {
			if ( golden_map[key] != val ) {
				mismatch_count ++;
				mismatch_diff += golden_map[key]-val;
				printf( "ERROR: %lx mismatch %lx: %x -- %x, diff: %d\n",
					total_count, key, golden_map[key], val, golden_map[key]-val);
			} else {
				correct_count ++;
			}
			golden_map.erase(golden_map.find(key));
		}

		kvp = sr->Next();
	}


	printf( "Total: %lu \n", total_count);
	printf( "Total Sum: %lu \n", total_sum);
	printf( "Correct: %lu \n", correct_count);
	printf( "Nonexist: %lu \n", nonexist_count);
	printf( "Mismatch: %lu \n", mismatch_count);
	printf( "Mismatch diff: %lu \n", mismatch_diff);
	printf( "Left: %lu \n", golden_map.size());
	if (total_sum != element_count) {
		std::cout << "ERROR: inserted " << element_count
							<< ", but the total sum is only " << total_sum << "\n";
	}
	*/
}
