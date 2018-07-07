#ifndef __REDUCER_H__
#define __REDUCER_H__

#include <mutex>
#include <vector>
#include <tuple>

#include "alignedbuffermanager.h"
#include "tempfilemanager.h"
#include "types.h"
#include "utils.h"

namespace SortReduceReducer {
	template <class K, class V>
	size_t ReduceInBuffer(V (*update)(V,V), void* buffer, size_t bytes);



	template <class K, class V>
	class StreamMergeReducer {
	public:
		virtual ~StreamMergeReducer() = 0;
		virtual void PutBlock(SortReduceTypes::Block block) = 0;
		virtual void PutFile(SortReduceTypes::File* file) = 0;
		virtual void Start() = 0;
		virtual bool IsDone() = 0;

		typedef struct {
			K key;
			V val;
		} KvPair;
	
	private:
		static K DecodeKey(void* buffer, size_t offset);
		static V DecodeVal(void* buffer, size_t offset);
		static void EncodeKvp(void* buffer, size_t offset, K key, V val);
	};

	template <class K, class V>
	class StreamMergeReducer_SinglePriority : public StreamMergeReducer<K,V> {
	public:
		StreamMergeReducer_SinglePriority(V (*update)(V,V), std::string temp_directory);
		~StreamMergeReducer_SinglePriority();

		void PutBlock(SortReduceTypes::Block block);
		void PutFile(SortReduceTypes::File* file);
		void Start();
		bool IsDone() { return m_done; };
	private:
		typedef struct {
			bool from_file;
			SortReduceTypes::Block block;
			SortReduceTypes::File* file;
		} DataSource;
		typedef struct {
			K key;
			V val;
			int src;
		} KvPairSrc;
		class CompareKv {
		public:
			bool operator() (KvPairSrc a, KvPairSrc b)
			{
				return (a.key < b.key);
			}
		};
		std::priority_queue<KvPairSrc,std::vector<KvPairSrc>, CompareKv> m_priority_queue;

		int m_src_count;

		std::vector<DataSource> mv_input_sources;
		int m_input_src_idx = 0;
		SortReduceTypes::File* m_out_file;

		V (*mp_update)(V,V);


		void WorkerThread();
		std::thread m_worker_thread;
		std::mutex m_mutex;

		std::string ms_temp_directory;
		TempFileManager* mp_temp_file_manager;
		

		bool m_started;
		bool m_done;
	};

}

#endif
