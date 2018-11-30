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
#include "utils.h"

#include "EdgeProcess.h"
#include "VertexValues.h"

#include "BCMergeFlip.h"



inline uint32_t vertex_update_add(uint32_t a, uint32_t b) {
	return a+b;
}
inline uint32_t vertex_update(uint32_t a, uint32_t b) {
	uint32_t ret = a;
	return ret;
}

inline uint32_t edge_program(uint32_t vid, uint32_t value, uint32_t fanout) {
	//printf( "Edge-program source: %x val: %x fanout: %x\n", vid, value, fanout);
	return vid;
}

inline uint32_t finalize_program(uint32_t oldval, uint32_t newval) {
	return newval;
}

inline bool is_active(uint32_t old, uint32_t newv, bool marked) {
	//printf( "Comparing %lx %lx %s\n", old, newv, marked?"Y":"N" );
	if ( old == 0xffffffff ) return true;
	//printf( "Comparing %x %x %s\n", old, newv, marked?"Y":"N" );
	return false;
}

int main(int argc, char** argv) {
	srand(time(0));

	if ( argc < 4 ) {
		fprintf(stderr, "usage: %s ridx matrix directory \n", argv[0] );
		exit(1);
	}

	char* tmp_dir = argv[1];
	char* idx_path = argv[2];
	char* mat_path = argv[3];

	int max_thread_count = 12;
	int max_sr_thread_count = 8;
	int max_vertexval_thread_count = 1; // This is important, to maintain ordering in the active vertex list
	int max_edgeproc_thread_count = 8;
	if ( argc > 4 ) {
		max_thread_count = atoi(argv[4]);
	}
	if ( max_thread_count >= 32 ) {
		max_sr_thread_count = 28;
		max_vertexval_thread_count = 1;
		max_edgeproc_thread_count = 8;
	} else if ( max_thread_count >= 24 ) {
		max_sr_thread_count = 20;
		max_vertexval_thread_count = 1;
		max_edgeproc_thread_count = 8;
	}
		
	std::chrono::high_resolution_clock::time_point start_all;
	start_all = std::chrono::high_resolution_clock::now();
	std::chrono::high_resolution_clock::time_point now;
	std::chrono::milliseconds duration_milli;

	//conf->SetManagedBufferSize(1024*1024*4, 256); // 4 GiB


	EdgeProcess<uint32_t,uint32_t>* edge_process = new EdgeProcess<uint32_t,uint32_t>(idx_path, mat_path, &edge_program);
	size_t vertex_count = edge_process->GetVertexCount();
	VertexValues<uint32_t,uint32_t>* vertex_values = new VertexValues<uint32_t,uint32_t>(tmp_dir, vertex_count, 0xffffffff, &is_active, &finalize_program, max_vertexval_thread_count);


	int iteration = 0;
	while ( true ) {
		//char filename[128];
		//sprintf(filename, "out%04d.sr", iteration);

		SortReduceTypes::Config<uint32_t,uint32_t>* conf =
			new SortReduceTypes::Config<uint32_t,uint32_t>(tmp_dir, "", max_sr_thread_count);
		conf->quiet = true;
		conf->SetUpdateFunction(&vertex_update);

		SortReduce<uint32_t,uint32_t>* sr = new SortReduce<uint32_t,uint32_t>(conf);
		SortReduce<uint32_t,uint32_t>::IoEndpoint* ep = sr->GetEndpoint(true);
		edge_process->SetSortReduceEndpoint(ep);
		for ( int i = 0; i < max_edgeproc_thread_count; i++ ) {
			//FIXME this is inefficient...
			edge_process->AddSortReduceEndpoint(sr->GetEndpoint(true));
		}

		std::chrono::high_resolution_clock::time_point start, iteration_start;
		start = std::chrono::high_resolution_clock::now();
		iteration_start = std::chrono::high_resolution_clock::now();
		std::chrono::milliseconds iteration_duration_milli;

		printf( "\t\t++ Starting iteration %d\n", iteration ); fflush(stdout);
		edge_process->Start();
		if ( iteration == 0 ) {
			edge_process->SourceVertex(12,0, true);
		} else {
			// TODO Spawn edge process threads

			int fd = vertex_values->OpenActiveFile(iteration-1);
			SortReduceUtils::FileKvReader<uint32_t,uint32_t>* reader = new SortReduceUtils::FileKvReader<uint32_t,uint32_t>(fd);
			std::tuple<uint32_t,uint32_t,bool> res = reader->Next();
			while ( std::get<2>(res) ) {
				uint32_t key = std::get<0>(res);
				uint32_t val = std::get<1>(res);
				edge_process->SourceVertex(key,val, true);
				res = reader->Next();

				//printf( "Vertex %lx %lx\n", key, val );
			}
			delete reader;

			// TODO kill edge process threads
		}
		edge_process->Finish();
		sr->Finish();
		now = std::chrono::high_resolution_clock::now();
		duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-start);
		printf( "\t\t++ Input done : %lu ms\n", duration_milli.count() );

		start = std::chrono::high_resolution_clock::now();
		SortReduceTypes::Status status = sr->CheckStatus();
		while ( status.done_external == false ) {
			usleep(1000);
			status = sr->CheckStatus();
			/*
			printf( "%s %s:%d-%d %s:%d-%d\n",
				status.done_input ? "yes":"no",
				status.done_inmem ? "yes":"no",
				status.internal_count, status.sorted_count,
				status.done_external ? "yes":"no",
				status.external_count, status.file_count);
			fflush(stdout);
			*/
		}

		now = std::chrono::high_resolution_clock::now();
		duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-start);
		printf( "\t\t++ Sort-Reduce done : %lu ms\n", duration_milli.count() );
		fflush(stdout);

		
		start = std::chrono::high_resolution_clock::now();
		std::tuple<uint32_t,uint32_t,bool> res = sr->Next();
		vertex_values->Start();
		while ( std::get<2>(res) ) {
			uint32_t key = std::get<0>(res);
			uint32_t val = std::get<1>(res);

			//printf( "\t\t++ SRR %x %x\n", key, val );
			while ( !vertex_values->Update(key,val) ) ;

			res = sr->Next();
		}


		vertex_values->Finish();
		size_t active_cnt = vertex_values->GetActiveCount();

		now = std::chrono::high_resolution_clock::now();
		duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-start);
		iteration_duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-iteration_start);
		
		printf( "\t\t++ Iteration done : %lu ms / %lu ms, Active %ld\n", duration_milli.count(), iteration_duration_milli.count(), active_cnt );
		
		iteration++;
		
		delete sr;

		if ( active_cnt == 0 ) break;
		vertex_values->NextIteration();

	}
	now = std::chrono::high_resolution_clock::now();
	duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-start_all);
	printf( "\t\t++ BFS portion Done! %lu ms %d iterations\n", duration_milli.count(), iteration );


	size_t last_file_bytes = 0;
	for ( int ri = iteration-2; ri >= 0; ri-- ) { // iteration-1 has nothing because it has no active vertices
		std::chrono::high_resolution_clock::time_point start;



		int active_fd = vertex_values->OpenActiveFile(ri);
		/*
		SortReduceUtils::FileKvReader<uint32_t,uint32_t>* active_reader = new SortReduceUtils::FileKvReader<uint32_t,uint32_t>(active_fd);
		start = std::chrono::high_resolution_clock::now();
		SortReduceTypes::Config<uint32_t,uint32_t>* active_conf =
			new SortReduceTypes::Config<uint32_t,uint32_t>(tmp_dir, "sorted.sr", max_sr_thread_count);
		active_conf->quiet = true;
		active_conf->SetUpdateFunction(&vertex_update);
		SortReduce<uint32_t,uint32_t>* active_sr = new SortReduce<uint32_t,uint32_t>(active_conf);

		std::tuple<uint32_t,uint32_t,bool> ar = active_reader->Next();
		while (std::get<2>(ar)) {
			active_sr->Update(std::get<0>(ar), std::get<1>(ar));
			ar = active_reader->Next();
		}
		active_sr->Finish();

		while ( active_sr->CheckStatus().done_external == false ) {
			usleep(1000);
		}
		//size_t result_bytes = active_sr->CheckStatus().done_file->bytes;

		now = std::chrono::high_resolution_clock::now();
		duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-start);
		printf( "Sorted active list for iteration %d %lu ms\n", ri, duration_milli.count() );
		*/

		printf( "Starting ri = %d/%d\n", ri, iteration );

		start = std::chrono::high_resolution_clock::now();

		char old_filename[128], dst_filename[128];
		sprintf(old_filename, "reverse%04d.sr", ri+1);
		sprintf(dst_filename, "reverse%04d.sr", ri);

		//TODO give file size... sr results have padding "result_bytes"
		BCMergeFlip<uint32_t,uint32_t>* mflip = NULL;
		if ( ri == iteration-2 ) {
			printf ( "Reversing single file\n" );
			fflush(stdout);
			mflip = new BCMergeFlip<uint32_t,uint32_t>(tmp_dir, "", 0, active_fd);
			//mflip = new BCMergeFlip<uint32_t,uint32_t>(tmp_dir, "", last_file_bytes, "sorted.sr", result_bytes);
		} else {
			printf ( "Reversing two files of size %lu, ---\n", last_file_bytes );
			fflush(stdout);
			mflip = new BCMergeFlip<uint32_t,uint32_t>(tmp_dir, old_filename, last_file_bytes, active_fd);
			//mflip = new BCMergeFlip<uint32_t,uint32_t>(tmp_dir, old_filename, last_file_bytes, "sorted.sr", result_bytes);
		}
		SortReduceTypes::Config<uint32_t,uint32_t>* reverse_conf =
			new SortReduceTypes::Config<uint32_t,uint32_t>(tmp_dir, dst_filename, max_sr_thread_count);
		reverse_conf->quiet = true;
		reverse_conf->SetUpdateFunction(&vertex_update_add);
		SortReduce<uint32_t,uint32_t>* reverse_sr = new SortReduce<uint32_t,uint32_t>(reverse_conf);

/*
		uint32_t last_key = 0;
		uint32_t cur_sum = 0;
	*/
		size_t rev_count = 0;
		std::tuple<uint32_t,uint32_t,bool> rr = mflip->Next();
		while ( std::get<2>(rr) ) {
			uint32_t key = std::get<0>(rr);
			uint32_t val = std::get<1>(rr);

/*
			if ( key == last_key ) {
				cur_sum += val;
			} else {

				if ( cur_sum > 0 ) reverse_sr->Update(last_key,cur_sum);
				last_key = key;
				cur_sum = val;
				rr = mflip->Next();
				rev_count++;
			}
			*/
				reverse_sr->Update(key,val);
				rr = mflip->Next();
				rev_count++;
		}
		/*
		if ( cur_sum > 0 ) {
			reverse_sr->Update(last_key,cur_sum);
			rev_count++;
		}
		*/

		printf( "reverse_sr given %lu kvps\n", rev_count );
		reverse_sr->Finish();
		while ( reverse_sr->CheckStatus().done_external == false ) {
			usleep(1000);
		}
		last_file_bytes = reverse_sr->CheckStatus().done_file->bytes;
		
		//delete active_sr;
		//delete active_reader;

		delete reverse_sr;
		delete mflip;
		
		now = std::chrono::high_resolution_clock::now();
		duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-start);
		printf( "Finished backtrace for iteration %d %lu ms\n", ri, duration_milli.count() );

	}

	now = std::chrono::high_resolution_clock::now();
	duration_milli = std::chrono::duration_cast<std::chrono::milliseconds> (now-start_all);
	printf( "\t\t++ BC All Done! %lu ms\n", duration_milli.count() );
	printf( "BC results are stored in file reverse0000.sr, up to %lu bytes\n", last_file_bytes );

	exit(0);
}
