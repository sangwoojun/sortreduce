#ifndef __KMEREXPAND_D__
#define __KMEREXPAND_D__

#include "sortreduce.h"

class KMerExpand {
public:
	KMerExpand(SortReduce::Config* conf);
	bool PutBlock(void* buffer, size_t bytes);
	SortReduce::Status CheckStatus();

private:
	SortReduce::Config* mp_config;
};

#endif
