#ifndef __REDUCER_ACCEL_H__
#define __REDUCER_ACCEL_H__
#ifdef HW_ACCEL

#define HW_MAXIMUM_SOURCES 8

#include <mutex>
#include <vector>
#include <tuple>
#include <chrono>

#include "reducer.h"

#include "alignedbuffermanager.h"
#include "tempfilemanager.h"
#include "types.h"
#include "utils.h"

#include "bdbmpcie.h"
#include "DRAMHostDMA.h"

/**
TODO: only allow ONE instance
**/

namespace SortReduceReducer {
	template <class K, class V>
	class MergerNodeAccel : public BlockSource<K,V> {
	public:
		MergerNodeAccel(V (*update)(V,V) = NULL); //FIXME should use buffer manager!
		~MergerNodeAccel();
		void AddSource(BlockSource<K,V>* src);
		bool Start();
		
		SortReduceTypes::Block GetBlock();
		void ReturnBlock(SortReduceTypes::Block block);

	private:
		// Non-inherited stuff
		void SendDone(int src);

		bool SendReadBlockDone(int idx);
		bool SendReadBlock(BlockSource<K,V>* src, int idx);
		bool SendWriteBlock();
		int GetDoneBuffer();
		bool HardwareDone();
		std::queue<int> mq_fpga_free_buffer_idx;
		std::queue<int> maq_fpga_inflight_idx_src[HW_MAXIMUM_SOURCES];
		std::queue<int> mq_fpga_inflight_idx_dst;
		static const size_t fpga_buffer_size = 1024*1024*4;
	public:
		static bool InstanceExist() {return m_instance_exist;};
		static int MaxSources() { return m_max_sources; };
	private:
		static bool m_instance_exist;
		static const int m_max_sources = 8;
	private:
		std::thread m_worker_thread;
		void WorkerThread();
		bool m_started;
		bool m_kill;

		bool ma_done[HW_MAXIMUM_SOURCES];
		bool ma_flushed[HW_MAXIMUM_SOURCES];
		int ma_read_buffers_inflight[HW_MAXIMUM_SOURCES];

		int m_cur_write_buffer_idx;
		std::queue<int> mq_cur_write_buffer_idxs;

		size_t m_read_block_total_bytes;


		std::mutex m_mutex;

		std::vector<BlockSource<K,V>*> ma_sources;

		V (*mp_update)(V,V) = NULL;
		
		// Output stuff
		int m_block_count;
		std::queue<int> mq_ready_idx;
		std::queue<int> mq_free_idx;
		std::vector<SortReduceTypes::Block> ma_blocks;
		size_t m_out_offset;
	};
}

#endif // HW_ACCEL
#endif // ifndef __REDUCER_ACCEL__
