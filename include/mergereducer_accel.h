#ifndef __REDUCER_ACCEL_H__
#define __REDUCER_ACCEL_H__

#define HW_MAXIMUM_SOURCES 32


#include <mutex>
#include <vector>
#include <tuple>
#include <chrono>

#include "reducer.h"

#include "alignedbuffermanager.h"
#include "tempfilemanager.h"
#include "types.h"
#include "utils.h"

#ifdef HW_ACCEL
#include "bdbmpcie.h"
#endif // HW_ACCEL


namespace SortReduceReducer {
	template <class K, class V>
	class MergerNodeAccel : public BlockSource<K,V>, public FileWriterNode<K,V> {
	public:
		MergerNodeAccel(V (*update)(V,V) = NULL, std::string temp_directory = "", std::string filename = ""); 
		~MergerNodeAccel();
		void AddSource(BlockSource<K,V>* src);
		void Start();
		
		SortReduceTypes::Block GetBlock();
		void ReturnBlock(SortReduceTypes::Block block);

		bool IsDone() { return m_done; };

#ifdef HW_ACCEL
	private:
		void WorkerThread();
		bool SendReadBlock(BlockSource<K,V>* src, int idx);
		void SendReadBlockDone(int idx);
		void SendWriteBlock();
		void EmitDmaBlock(size_t offset, size_t bytes, bool last);
#endif
	public:
		static bool InstanceExist() {return m_instance_exist;};
		static int MaxSources() { return m_max_sources; };
	private:
		static bool m_instance_exist;
		static const int m_max_sources = 32;
	private:
		std::thread m_worker_thread;
		bool m_started;
		bool m_kill;
		bool m_done;

		std::mutex m_mutex;

		bool m_write_file;

		size_t m_total_write_bytes;
		size_t m_total_read_bytes;



		int m_source_count;
		BlockSource<K,V>* ma_sources[HW_MAXIMUM_SOURCES];
		size_t ma_sources_offset[HW_MAXIMUM_SOURCES];
		SortReduceTypes::Block ma_cur_read_blocks[HW_MAXIMUM_SOURCES];
		std::queue<int> maq_read_buffers_inflight[HW_MAXIMUM_SOURCES];
		
		bool ma_last_sent[HW_MAXIMUM_SOURCES];

		V (*mp_update)(V,V) = NULL;

		std::queue<int> mq_free_dma_idx; // for host->fpga
		static const int m_write_buffer_idx_start = 128;
		static const int m_write_buffer_idx_end = 256;
		int m_cur_write_buffer_idx;
		int m_write_buffers_inflight = 0;
		
		// Output stuff
		std::queue<int> mq_ready_idx;
		std::queue<int> mq_free_idx;
		std::vector<SortReduceTypes::Block> ma_blocks;
		int m_cur_out_idx;
		size_t m_out_offset;
		size_t m_total_out_bytes;
		SortReduceTypes::Block m_cur_out_block;
	};
}

#endif // ifndef __REDUCER_ACCEL__
