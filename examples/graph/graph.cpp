#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <chrono>
#include <ctime>
#include <map>
#include <tuple>

#include "sortreduce.h"
#include "filekvreader.h"
#include "types.h"

#include "EdgeProcess.h"
#include "VertexValues.h"



inline uint32_t vertex_update(uint32_t a, uint32_t b) {
	uint32_t ret = a;
	return ret;
}
inline uint32_t edge_program(uint32_t vid, uint32_t value, uint32_t fanout) {
	//printf( "Edge-program source: %x val: %x fanout: %x\n", vid, value, fanout);
	return vid;
}

inline uint32_t finalize_program(uint32_t val) {
	return val;
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
		new SortReduceTypes::Config<uint32_t,uint32_t>(tmp_dir, "out.sr", 8);
	conf->SetUpdateFunction(&vertex_update);
	conf->SetManagedBufferSize(1024*1024*8, 256); // 4 GiB


	EdgeProcess<uint32_t,uint32_t>* edge_process = new EdgeProcess<uint32_t,uint32_t>(idx_path, mat_path, &edge_program);
	VertexValues<uint32_t,uint32_t>* vertex_values = new VertexValues<uint32_t,uint32_t>(tmp_dir, 1024*1024*32, 0xffffffff, &is_active);


	int iteration = 0;
	while ( true ) {
		SortReduce<uint32_t,uint32_t>* sr = new SortReduce<uint32_t,uint32_t>(conf);
		SortReduce<uint32_t,uint32_t>::IoEndpoint* ep = sr->GetEndpoint(true);
		edge_process->SetSortReduceEndpoint(ep);

		printf( "Starting iteration %d\n", iteration );
		if ( iteration == 0 ) {
			edge_process->SourceVertex(12,0);
		} else {
			int fd = vertex_values->OpenActiveFile(iteration-1);
			SortReduceUtils::FileKvReader<uint32_t,uint32_t>* reader = new SortReduceUtils::FileKvReader<uint32_t,uint32_t>(fd);
			std::tuple<uint32_t,uint32_t,bool> res = reader->Next();
			while ( std::get<2>(res) ) {
				uint32_t key = std::get<0>(res);
				uint32_t val = std::get<1>(res);
				edge_process->SourceVertex(key,val);
				res = reader->Next();
			}
			delete reader;
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

		printf( "Sort-Reduce done\n" );
		std::tuple<uint32_t,uint32_t,bool> res = sr->Next();
		while ( std::get<2>(res) ) {
			uint32_t key = std::get<0>(res);
			uint32_t val = std::get<1>(res);

			vertex_values->Update(key,finalize_program(val));

			res = sr->Next();
		}


		size_t active_cnt = vertex_values->GetActiveCount();
		printf( "Iteration Done! Active %ld\n", active_cnt );
		if ( active_cnt == 0 ) break;
		vertex_values->NextIteration();
		iteration++;

		delete sr;
	}
	printf( "All Done!\n" );

	exit(0);
}
