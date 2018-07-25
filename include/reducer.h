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

	template <class K, class V>
	class StreamMergeReducer {
	public:
		StreamMergeReducer();
		virtual ~StreamMergeReducer() {};
		void PutBlock(SortReduceTypes::Block block);
		void PutFile(SortReduceTypes::File* file);
		virtual void Start() = 0;
		bool IsDone() { return m_done; };
		virtual SortReduceTypes::File* GetOutFile() = 0;
	
	protected: //TODO eventually must become private
		typedef struct {
			bool from_file;
			SortReduceTypes::Block block;
			SortReduceTypes::File* file;
		} DataSource;
		std::vector<DataSource> mv_input_sources;

	protected:
		V (*mp_update)(V,V);

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

	protected:
		static K DecodeKey(void* buffer, size_t offset);
		static V DecodeVal(void* buffer, size_t offset);
		static void EncodeKvp(void* buffer, size_t offset, K key, V val);
		static void EncodeKey(void* buffer, size_t offset, K key);
		static void EncodeVal(void* buffer, size_t offset, V val);

	private:
		std::mutex m_mutex;
	};

	template <class K, class V>
	class StreamMergeReducer_SinglePriority : public StreamMergeReducer<K,V> {
	public:
		StreamMergeReducer_SinglePriority(V (*update)(V,V), std::string temp_directory, std::string filename = "");
		~StreamMergeReducer_SinglePriority();

		//void PutBlock(SortReduceTypes::Block block);
		//void PutFile(SortReduceTypes::File* file);
		void Start();
		SortReduceTypes::File* GetOutFile() {return this->m_out_file; };
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
