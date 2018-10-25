#include "mergereducer_multitree.h"
#include "mergereducer_accel.h"


template <class K, class V>
SortReduceReducer::MergeReducer_MultiTree<K,V>::MergeReducer_MultiTree(V (*update)(V,V), std::string temp_directory, int maximum_threads, std::string filename, bool verbose) {
	this->m_done = false;
	this->m_started = false;
	this->m_maximum_threads = maximum_threads;

	this->mp_update = update;

	this->mp_stream_file_reader = new StreamFileReader(temp_directory, verbose);
	if ( filename == "" ) {
		this->mp_reducer_node_stream = new ReducerNodeStream<K,V>(update, 1024*1024, 4);
		this->mp_block_source_reader = new BlockSourceReader<K,V>(mp_reducer_node_stream);
		this->mp_reducer_node_to_file = NULL;
	} else {
		this->mp_reducer_node_to_file = new ReducerNode<K,V>(update, temp_directory, filename);
		this->mp_reducer_node_stream = NULL;
		this->mp_block_source_reader = NULL;
	}

	this->mvv_tree_nodes.push_back(std::vector<BlockSource<K,V>*>());
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
}

template <class K, class V>
void
SortReduceReducer::MergeReducer_MultiTree<K,V>::PutBlock(SortReduceTypes::Block block) {
	fprintf( stderr, "MergeReducer_MultiTree not used with blocks...yet\n" );
}

template <class K, class V>
void
SortReduceReducer::MergeReducer_MultiTree<K,V>::PutFile(SortReduceTypes::File* file) {
	if ( this->m_started ) {
		fprintf(stderr, "Attempting to add data source to started reducer\n" );
		return;
	}
	mp_stream_file_reader->PutFile(file);

	int cur_count = mv_file_reader.size();
	FileReaderNode<K,V>* reader = new FileReaderNode<K,V>(mp_stream_file_reader, cur_count);
	mv_file_reader.push_back(reader);
	mvv_tree_nodes[0].push_back(reader);
}

template <class K, class V>
void
SortReduceReducer::MergeReducer_MultiTree<K,V>::Start() {
	this->m_started = true;

	size_t input_count = mv_file_reader.size();
	printf( "MergeReducer_MultiTree started with %lu files\n", input_count ); fflush(stdout);




	int cur_level = 0;
	int cur_level_count = input_count;

	int maximum_2to1_nodes = 2; // Actually maximum number of leaves of 2to1 nodes
	if ( m_maximum_threads >= 15 ) {
		maximum_2to1_nodes = 8;
	} else if ( m_maximum_threads >= 8 ) {
		maximum_2to1_nodes = 4;
	}
#ifdef HW_ACCEL
	if ( !MergerNodeAccel<K,V>::InstanceExist() ) {
		maximum_2to1_nodes = 32;
	}
#endif

	while (cur_level_count > 1) {
		mvv_tree_nodes.push_back(std::vector<BlockSource<K,V>*>());


		if ( cur_level_count > maximum_2to1_nodes*2 ) {
			int leaves_per_node = cur_level_count/maximum_2to1_nodes;
			if ( cur_level_count % maximum_2to1_nodes > 0 ) leaves_per_node ++;
			int node_count = maximum_2to1_nodes;

			for ( int i = 0; i < node_count; i++ ) {
				MergerNode<K,V>* merger = new MergerNode<K,V>(1024*1024*4, 4, this->mp_update, cur_level);
				for ( int j = 0; j < leaves_per_node; j++ ) {
					if ( (size_t)i*leaves_per_node+j >= mvv_tree_nodes[cur_level].size() ) break;

					merger->AddSource(mvv_tree_nodes[cur_level][i*leaves_per_node+j]);
				}
				merger->Start();
				mvv_tree_nodes[cur_level+1].push_back(merger);

				mv_tree_nodes_seq.push_back(merger);
			}
#ifdef HW_ACCEL
		} else if (MergerNodeAccel<K,V>::MaxSources() >= cur_level_count && !MergerNodeAccel<K,V>::InstanceExist() ) { 
			MergerNodeAccel<K,V>* merger = new MergerNodeAccel<K,V>();
			for ( int i = 0; i < cur_level_count; i++ ) {
				merger->AddSource(mvv_tree_nodes[cur_level][i]);
			}
			//FIXME 
			merger->Start();
			mvv_tree_nodes[cur_level+1].push_back(merger);

			mv_tree_nodes_seq.push_back(merger);
#endif
		} else {
			for ( int i = 0; i < cur_level_count/2; i++ ) {
				MergerNode<K,V>* merger = new MergerNode<K,V>(1024*1024*4, 4, cur_level);
				merger->AddSource(mvv_tree_nodes[cur_level][i*2]);
				merger->AddSource(mvv_tree_nodes[cur_level][i*2+1]);
				merger->Start();
				mvv_tree_nodes[cur_level+1].push_back(merger);

				mv_tree_nodes_seq.push_back(merger);
			}
			if ( cur_level_count%2 == 1 ) {
				mvv_tree_nodes[cur_level+1].push_back(mvv_tree_nodes[cur_level][cur_level_count-1]);
			}
		}

		cur_level_count = mvv_tree_nodes[cur_level+1].size();
		cur_level++;
	}

	if ( mp_reducer_node_stream != NULL ) {
		mp_reducer_node_stream->SetSource(mvv_tree_nodes[cur_level][0]);
	} else if ( mp_reducer_node_to_file != NULL ) {
		mp_reducer_node_to_file->SetSource(mvv_tree_nodes[cur_level][0]);
	} else {
		printf( "ERROR: MergeReducer_MultiTree reducer node not set! %s:%d\n", __FILE__, __LINE__ );
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

	printf( "ERROR: MergeReducer_MultiTree reducer node not set! %s:%d\n", __FILE__, __LINE__ );
	return false;
}

template <class K, class V>
SortReduceTypes::File* 
SortReduceReducer::MergeReducer_MultiTree<K,V>::GetOutFile() {
	if ( mp_reducer_node_to_file != NULL ) {
		return this->mp_reducer_node_to_file->GetOutFile(); 
	} 

	return NULL;
}

template <class K, class V>
SortReduceReducer::BlockSourceReader<K,V>* 
SortReduceReducer::MergeReducer_MultiTree<K,V>::GetResultReader() {
	return mp_block_source_reader;
}




TEMPLATE_EXPLICIT_INSTANTIATION(SortReduceReducer::MergeReducer_MultiTree)
