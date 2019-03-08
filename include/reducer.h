#ifndef __REDUCER_H__
#define __REDUCER_H__

#include <mutex>
#include <vector>
#include <tuple>
#include <chrono>

#include "alignedbuffermanager.h"
#include "tempfilemanager.h"
#include "types.h"
#include "utils.h"

namespace SortReduceReducer {
	//template <class K, class V>
	//size_t ReduceInBuffer(V (*update)(V,V), void* buffer, size_t bytes);

	class StreamFileReader {
	public:
		StreamFileReader(std::string temp_directory, bool verbose = false);
		~StreamFileReader();
		void PutFile(SortReduceTypes::File* file);
		SortReduceTypes::Block LoadNextFileBlock(int src, bool pop = false);
		void ReturnBuffer(SortReduceTypes::Block block);
	private:
		void FileReadReq(int src);

	private:
		bool m_started;

		bool m_verbose;

		std::vector<SortReduceTypes::File*> mv_file_sources;

		AlignedBufferManager* mp_buffer_manager;
		TempFileManager* mp_temp_file_manager;
		
		size_t m_input_file_bytes = 0;

		std::vector<size_t> mv_file_offset;
		std::vector<bool> mv_file_eof;
		std::vector<int> mv_reads_inflight;
		std::queue<std::tuple<int,SortReduceTypes::Block>> mq_read_request_order;
		int m_total_reads_inflight = 0;
	
		std::vector<std::queue<SortReduceTypes::Block>> mvq_ready_blocks;
		const size_t m_file_reads_inflight_target = 3;

		std::mutex m_mutex;

	};

	// parent class for those writing blocks to disk
	template <class K, class V>
	class FileWriterNode {
	public:
		FileWriterNode();
		~FileWriterNode();
		SortReduceTypes::File* GetOutFile() { return m_out_file; };
	protected:
		void CreateFile(std::string temp_directory, std::string filename);
		void EmitBlock(void* buffer, size_t bytes);
		void EmitFlush();
	protected:
		SortReduceTypes::File* m_out_file;
		
		SortReduceTypes::Block m_out_block;
		size_t m_out_offset;
		size_t m_out_file_offset;

		//std::string ms_temp_directory;
		AlignedBufferManager* mp_buffer_manager;
		TempFileManager* mp_temp_file_manager;
	};
	
	template <class K, class V>
	class StreamFileWriterNode {
	public:
		StreamFileWriterNode(std::string temp_directory, std::string filename);
		~StreamFileWriterNode();
		SortReduceTypes::File* GetOutFile() { return m_out_file; };
	protected:
		void EmitKv(K key, V val);
		void EmitFlush();
	
	protected:
		SortReduceTypes::File* m_out_file;
		
		SortReduceTypes::Block m_out_block;
		size_t m_out_offset;
		size_t m_out_file_offset;

		K m_last_key;

		//std::string ms_temp_directory;
		AlignedBufferManager* mp_buffer_manager;
		TempFileManager* mp_temp_file_manager;
	};
	
	template <class K, class V>
	class BlockSource {
	public:
		virtual ~BlockSource();
		virtual SortReduceTypes::Block GetBlock() = 0;
		virtual void ReturnBlock(SortReduceTypes::Block block) = 0;
	};
		
	template <class K, class V>
	class BlockSourceNode : public BlockSource<K,V> {
	public:
		BlockSourceNode(size_t block_bytes, int block_count, int my_id);
		virtual ~BlockSourceNode();
		SortReduceTypes::Block GetBlock();
		void ReturnBlock(SortReduceTypes::Block block);
	protected:
		int m_my_id = -1;
		int m_block_count;
		std::queue<int> mq_ready_idx;
		std::queue<int> mq_free_idx;
		std::vector<SortReduceTypes::Block> ma_blocks;
		int m_cur_free_idx;
		size_t m_out_offset;

		K m_last_key = 0;
		void EmitKvPair(K key, V val);
		/**
		Flushes, and emits a block with "last" flag set.
		NOTE: "last" block needs to be returned as well!
		**/
		void FinishEmit();
		
		std::mutex m_mutex;
	};
	
	template <class K, class V>
	class BlockKvReader {
	public:
		BlockKvReader(BlockSource<K,V>* src);
		~BlockKvReader();
		bool IsEmpty();
		SortReduceTypes::KvPair<K,V> GetNext();
	private:
		BlockSource<K,V>* mp_src = NULL;

		size_t m_offset;
		SortReduceTypes::Block m_cur_block;
		bool m_done;
	};

	template <class K, class V>
	class FileReaderNode : public BlockSource<K,V> {
	public:
		FileReaderNode(StreamFileReader* src, int idx);
		SortReduceTypes::Block GetBlock();
		void ReturnBlock(SortReduceTypes::Block block);
	private:
		StreamFileReader* mp_reader;
		int m_idx;
	};
	
	template <class K, class V>
	class BlockReaderNode : public BlockSource<K,V> {
	public:
		BlockReaderNode(SortReduceTypes::Block block);
		SortReduceTypes::Block GetBlock();
		void ReturnBlock(SortReduceTypes::Block block);
	private:
		bool m_done;
		SortReduceTypes::Block m_block;
	};
	
	template <class K, class V>
	class MergerNode : public BlockSourceNode<K,V> {
	public:
		MergerNode(size_t block_bytes, int block_count, int my_id = -1); //FIXME should use buffer manager!
		MergerNode(size_t block_bytes, int block_count, V (*update)(V,V), int my_id = -1); //FIXME should use buffer manager!
		~MergerNode();
		void AddSource(BlockSource<K,V>* src);
		void Start();
	private:
		class CompareKv {
		public:
			bool operator() (SortReduceTypes::KvPairSrc<K,V> a, SortReduceTypes::KvPairSrc<K,V> b) {
				return (a.key > b.key); // This ordering feels wrong, but this is correct
			}
		};
		std::thread m_worker_thread;
		void WorkerThread2();
		void WorkerThreadN();
		bool m_started;
		bool m_kill;

		std::vector<BlockSource<K,V>*> ma_sources;


		V (*mp_update)(V,V) = NULL;

	};

	template <class K, class V>
	class ReducerNode : public StreamFileWriterNode<K,V> {
	public:
		ReducerNode(V (*update)(V,V), std::string temp_directory, std::string filename = "");
		~ReducerNode();
		void SetSource(BlockSource<K,V>* src);
		bool IsDone() { return m_done; };
	private:
		BlockSource<K,V>* mp_src;

		std::thread m_worker_thread;
		void WorkerThread();
		bool m_done;
		bool m_kill;

		V (*mp_update)(V,V);
	};
	
	template <class K, class V>
	class ReducerNodeStream : public BlockSourceNode<K,V> {
	public:
		ReducerNodeStream(V (*update)(V,V), size_t block_bytes, int block_count);
		~ReducerNodeStream();
		void SetSource(BlockSource<K,V>* src);
		bool IsDone();
	private:
		BlockSource<K,V>* mp_src;
		BlockKvReader<K,V>* mp_reader;

		std::thread m_worker_thread;
		void WorkerThread();
		bool m_done;
		bool m_kill;

		V (*mp_update)(V,V);
	};

	template <class K, class V>
	class BlockSourceReader {
	public:
		BlockSourceReader();
		BlockSourceReader(BlockSource<K,V>* src);
		void AddSource(BlockSource<K,V>* src);
		SortReduceTypes::KvPair<K,V> GetNext();
		bool Empty();
	private:
		bool m_done;
		bool m_kill;
		BlockKvReader<K,V>* mp_reader;
	};

	template <class K, class V>
	class ReducerUtils {
	public:
		static SortReduceTypes::KvPair<K,V> DecodeKvPair(SortReduceTypes::Block* block, size_t* p_off, BlockSource<K,V>* src, bool* kill);
		static K DecodeKey(void* buffer, size_t offset);
		static V DecodeVal(void* buffer, size_t offset);
		static SortReduceTypes::KvPair<K,V> DecodeKvp(void* buffer, size_t offset);
		static void EncodeKvp(void* buffer, size_t offset, K key, V val);
		static void EncodeKey(void* buffer, size_t offset, K key);
		static void EncodeVal(void* buffer, size_t offset, V val);
	};
	
	template <class K, class V>
	class MergeReducer {
	public:
		virtual ~MergeReducer();
		virtual void PutBlock(SortReduceTypes::Block block) = 0;
		virtual void PutFile(SortReduceTypes::File* file) = 0;
		virtual void Start() = 0;
		virtual bool IsDone() = 0;
		virtual SortReduceTypes::File* GetOutFile() = 0;
		virtual int GetThreadCount() = 0;

		virtual size_t GetInputFileBytes() = 0;
	protected:
		V (*mp_update)(V,V);
	};

	template <class K, class V>
	class StreamMergeReducer : public MergeReducer<K,V> {
	public:
		StreamMergeReducer();
		virtual ~StreamMergeReducer() {};
		void PutBlock(SortReduceTypes::Block block);
		void PutFile(SortReduceTypes::File* file);
		virtual void Start() = 0;
		bool IsDone() { return m_done; };
		int GetThreadCount() { return 1; };
		SortReduceTypes::File* GetOutFile() {return this->m_out_file; };

		size_t GetInputFileBytes() { return this->m_input_file_bytes; };

	
	protected: //TODO eventually must become private
		typedef struct {
			bool from_file;
			SortReduceTypes::Block block;
			SortReduceTypes::File* file;
		} DataSource;
		std::vector<DataSource> mv_input_sources;

	protected:

		SortReduceTypes::File* m_out_file;
		SortReduceTypes::Block m_out_block;
		size_t m_out_offset;
		size_t m_out_file_offset;

		//std::string ms_temp_directory;
		AlignedBufferManager* mp_buffer_manager;
		TempFileManager* mp_temp_file_manager;

		void EmitKv(K key, V val);
		void EmitFlush();
	
		//std::vector<size_t> mv_read_offset;
		std::vector<size_t> mv_file_offset;
		std::vector<bool> mv_file_eof;
		std::vector<int> mv_reads_inflight;
		std::queue<std::tuple<int,SortReduceTypes::Block>> mq_read_request_order;
		int m_total_reads_inflight = 0;
	
		std::vector<std::queue<SortReduceTypes::Block>> mvq_ready_blocks;
		const int m_file_reads_inflight_target = 3;

		void FileReadReq(int src);
		SortReduceTypes::Block GetNextFileBlock(int src);

		bool m_started;
		bool m_done;

		size_t m_input_file_bytes = 0;

	private:
		std::mutex m_mutex;
	};

	template <class K, class V>
	class StreamMergeReducer_SinglePriority : public StreamMergeReducer<K,V> {
	public:
		StreamMergeReducer_SinglePriority(V (*update)(V,V), std::string temp_directory, std::string filename = "", bool verbose = false);
		~StreamMergeReducer_SinglePriority();

		//void PutBlock(SortReduceTypes::Block block);
		//void PutFile(SortReduceTypes::File* file);
		void Start();
	private:
		typedef struct {
			K key;
			V val;
			int src;
		} KvPairSrc;
		class CompareKv {
		public:
			bool operator() (KvPairSrc a, KvPairSrc b) {
				return (a.key > b.key); // This ordering feels wrong, but this is correct
			}
		};
		std::priority_queue<KvPairSrc,std::vector<KvPairSrc>, CompareKv> m_priority_queue;


		void WorkerThread();
		std::thread m_worker_thread;
		std::mutex m_mutex;

	};



}

#endif
