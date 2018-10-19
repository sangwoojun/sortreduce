#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <chrono>
#include <ctime>
#include <map>

#include "sortreduce.h"
#include "types.h"

uint32_t update_function(uint32_t a, uint32_t b) {
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

	SortReduceTypes::Config<uint64_t,uint32_t>* conf =
		new SortReduceTypes::Config<uint64_t,uint32_t>(tmp_dir, "out.sr", 8);
	conf->SetUpdateFunction(&update_function);
	conf->SetMaxBytesInFlight(1024*1024*1024); 		// 1 GiB
	conf->SetManagedBufferSize(1024*1024*32, 64); // 4 GiB

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
			printf( "ERROR: Result key order wrong %lx %lx -- %x\n", last_key, key, val );
		}
		last_key = key;

		if ( golden_map.find(key) == golden_map.end() ) {
			nonexist_count ++;
			printf( "ERROR: nonexist %lx -- %x\n", key, val );
		} else {
			if ( golden_map[key] != val ) {
				mismatch_count ++;
				mismatch_diff += golden_map[key]-val;
				printf( "ERROR: mismatch %lx: %x -- %x, diff: %d\n",
					key, golden_map[key], val, golden_map[key]-val);
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
}
