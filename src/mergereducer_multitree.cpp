#include "mergereducer_multitree.h"
#include "mergereducer_accel.h"


template <class K, class V>
SortReduceReducer::MergeReducer_MultiTree<K,V>::MergeReducer_MultiTree(V (*update)(V,V), std::string temp_directory, int maximum_threads, std::string filename, bool verbose) {
	this->m_done = false;
	this->m_started = false;
	this->m_maximum_threads = maximum_threads;
	this->m_thread_count = 0;
	this->mp_update = update;
	this->m_file_input_cnt = 0;

	this->mp_stream_file_reader = new StreamFileReader(temp_directory, verbose);
	m_temp_directory = temp_directory;
	m_filename = filename;

	//printf( "MergeReducer_MultiTree created with %s-%s %s %s\n", temp_directory, filename, m_temp_directory, m_filename );

	this->mvv_tree_nodes.push_back(std::vector<BlockSource<K,V>*>());

	this->mp_reducer_node_to_file = NULL;
	this->mp_reducer_node_stream = NULL;
	this->mp_block_source_reader = NULL;
}

template <class K, class V>
SortReduceReducer::MergeReducer_MultiTree<K,V>::~MergeReducer_MultiTree() {
	if ( mp_reducer_node_stream != NULL ) {
		delete mp_reducer_node_stream;
	}
	if ( mp_reducer_node_to_file != NULL ) {
		delete mp_reducer_node_to_file;
	}

	for ( int i = mv_tree_nodes_seq.size()-1; i >= 0; i-- ) {
		delete mv_tree_nodes_seq[i];
	}

	delete mp_stream_file_reader;
}

template <class K, class V>
void
SortReduceReducer::MergeReducer_MultiTree<K,V>::PutBlock(SortReduceTypes::Block block) {
	if ( this->m_started ) {
		fprintf(stderr, "Attempting to add data source to started reducer\n" );
		return;
	}
	BlockReaderNode<K,V>* reader = new BlockReaderNode<K,V>(block);
	mv_src_reader.push_back(reader);
	mvv_tree_nodes[0].push_back(reader);

	mv_tree_nodes_seq.push_back(reader);
}

template <class K, class V>
void
SortReduceReducer::MergeReducer_MultiTree<K,V>::PutFile(SortReduceTypes::File* file) {
	if ( this->m_started ) {
		fprintf(stderr, "Attempting to add data source to started reducer\n" );
		return;
	}
	mp_stream_file_reader->PutFile(file);

	FileReaderNode<K,V>* reader = new FileReaderNode<K,V>(mp_stream_file_reader, m_file_input_cnt);
	m_file_input_cnt++;
	mv_src_reader.push_back(reader);
	mvv_tree_nodes[0].push_back(reader);
	
	mv_tree_nodes_seq.push_back(reader);
}

template <class K, class V>
void
SortReduceReducer::MergeReducer_MultiTree<K,V>::Start() {
	this->m_started = true;
	size_t input_count = mv_src_reader.size();

	bool accelerate = false;
	mp_merger_accel = NULL;
#ifdef HW_ACCEL
	//TODO mutex... or something...
	if ( !MergerNodeAccel<K,V>::InstanceExist() ) {
		mp_merger_accel = new MergerNodeAccel<K,V>(NULL, m_temp_directory, m_filename);
		accelerate = true;
		//printf( "MergeReducer_MultiTree creating Accel with %s %s\n", m_temp_directory, m_filename );
	}
#endif



	printf( "MergeReducer_MultiTree started with %lu files and %d threads\n", input_count, m_maximum_threads ); fflush(stdout);


	int cur_level = 0;
	int cur_level_count = input_count;

	int maximum_2to1_nodes = 1; // Actually maximum number of leaves of 2to1 nodes
	int thread_budget_left = m_maximum_threads;
	if ( accelerate ) {
		maximum_2to1_nodes = 32;
		thread_budget_left --; //manager thread
	} else if ( m_maximum_threads >= 8 ) {
		maximum_2to1_nodes = 4;
		//thread_budget_left -= 4; // 3* 2-to-1, reducer
		thread_budget_left = 4; // FIXME
	} else if ( m_maximum_threads >= 4 ) {
		maximum_2to1_nodes = 2;
		//thread_budget_left -= 2; // 2-to-1, reducer
		thread_budget_left = 2; //FIXME
	} else {
		maximum_2to1_nodes = 1;
		//thread_budget_left --; // reducer thread
		thread_budget_left = 1; //FIXME
	}


	while (cur_level_count > 1) {
		mvv_tree_nodes.push_back(std::vector<BlockSource<K,V>*>());


		if ( cur_level_count > maximum_2to1_nodes ) {
			// Does this just once
			int leaves_per_node = cur_level_count/maximum_2to1_nodes;
			if ( cur_level_count % maximum_2to1_nodes > 0 ) leaves_per_node ++;
			int node_count = maximum_2to1_nodes;

			int forward_cnt = 0;
			if ( thread_budget_left < maximum_2to1_nodes ) {
				forward_cnt = maximum_2to1_nodes - thread_budget_left;
				int remain_cnt = cur_level_count - forward_cnt;

				printf( "MergeReducer_MultiTree:: setting forward_cnt to %d\n", forward_cnt );

				if ( thread_budget_left > 0 ) {
					leaves_per_node = remain_cnt/thread_budget_left;
					if ( remain_cnt % thread_budget_left > 0 ) leaves_per_node ++;
					node_count = thread_budget_left;
				} else {
					leaves_per_node = remain_cnt;
					node_count = 1;
					fprintf(stderr, "MergeReducer_MultiTree: Thread budget should never be zero!\n");
				}
			}

			for ( int i = 0; i < node_count; i++ ) {
				MergerNode<K,V>* merger = new MergerNode<K,V>(1024*1024*4, 4, this->mp_update, cur_level);
				for ( int j = 0; j < leaves_per_node; j++ ) {
					if ( (size_t)(forward_cnt + i*leaves_per_node + j) >= mvv_tree_nodes[cur_level].size() ) break;
					merger->AddSource(mvv_tree_nodes[cur_level][forward_cnt + i*leaves_per_node+j]);
					//if ( (size_t)(i*leaves_per_node + j) >= mvv_tree_nodes[cur_level].size() ) break;
					//merger->AddSource(mvv_tree_nodes[cur_level][i*leaves_per_node+j]);
				}
				merger->Start();
				m_thread_count ++;
				mvv_tree_nodes[cur_level+1].push_back(merger);

				mv_tree_nodes_seq.push_back(merger);
			}
			if ( thread_budget_left < maximum_2to1_nodes ) {
				for ( int i = 0; i < forward_cnt; i++ ) {
					mvv_tree_nodes[cur_level+1].push_back(mvv_tree_nodes[cur_level][i]);
				}
			}
		} else {
			if ( accelerate && cur_level_count <= HW_MAXIMUM_SOURCES ) {
				for ( int i = 0; i < cur_level_count; i++ ) {
					mp_merger_accel->AddSource(mvv_tree_nodes[cur_level][i]);
				}
				mp_merger_accel->Start();
				m_thread_count ++;
				BlockSource<K,V>* merger = mp_merger_accel;
				mvv_tree_nodes[cur_level+1].push_back(merger);

				mv_tree_nodes_seq.push_back(merger);
			} else {
				for ( int i = 0; i < cur_level_count/2; i++ ) {
					MergerNode<K,V>* merger = new MergerNode<K,V>(1024*1024*4, 4, cur_level);
					merger->AddSource(mvv_tree_nodes[cur_level][i*2]);
					merger->AddSource(mvv_tree_nodes[cur_level][i*2+1]);
					merger->Start();
					m_thread_count ++;
					mvv_tree_nodes[cur_level+1].push_back(merger);

					mv_tree_nodes_seq.push_back(merger);
				}
				if ( cur_level_count%2 == 1 ) {
					mvv_tree_nodes[cur_level+1].push_back(mvv_tree_nodes[cur_level][cur_level_count-1]);
				}
			}
		}
		cur_level_count = mvv_tree_nodes[cur_level+1].size();
		cur_level++;
	}


	BlockSource<K,V>* root = mvv_tree_nodes[cur_level][0];
	if ( m_temp_directory == "" ) {
		if ( !accelerate ) {
			mp_reducer_node_stream = new ReducerNodeStream<K,V>(mp_update, 1024*1024, 4);
			mp_reducer_node_stream->SetSource(root);
			root = mp_reducer_node_stream;
		}
		mp_block_source_reader = new BlockSourceReader<K,V>(root);
	} else {
		if ( !accelerate ) {
			mp_reducer_node_to_file = new ReducerNode<K,V>(mp_update, m_temp_directory, m_filename);
			mp_reducer_node_to_file->SetSource(root);
		}
	}
}

template <class K, class V>
bool
SortReduceReducer::MergeReducer_MultiTree<K,V>::IsDone() {
	if ( mp_reducer_node_stream != NULL ) {
		return mp_reducer_node_stream->IsDone();
	}
	if ( mp_reducer_node_to_file != NULL ) {
		return mp_reducer_node_to_file->IsDone();
	}
	if ( mp_merger_accel != NULL ) {
		return mp_merger_accel->IsDone();
	}

	printf( "ERROR: MergeReducer_MultiTree reducer node not set! %s:%d\n", __FILE__, __LINE__ );
	return false;
}

template <class K, class V>
SortReduceTypes::File* 
SortReduceReducer::MergeReducer_MultiTree<K,V>::GetOutFile() {
	if ( mp_reducer_node_to_file != NULL ) {
		return this->mp_reducer_node_to_file->GetOutFile(); 
	} 
	
	if ( mp_merger_accel != NULL ) {
		return mp_merger_accel->GetOutFile();
	}

	return NULL;
}

template <class K, class V>
SortReduceReducer::BlockSourceReader<K,V>* 
SortReduceReducer::MergeReducer_MultiTree<K,V>::GetResultReader() {
	return mp_block_source_reader;
}



TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::MergeReducer_MultiTree)
