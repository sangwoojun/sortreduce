#include "kmerexpand.h"

KMerExpand::KMerExpand(SortReduce::Config* conf) {
}
	
bool 
KMerExpand::PutBlock(void* buffer, size_t bytes) {
	return false;
}

SortReduce::Status 
KMerExpand::CheckStatus() {
	SortReduce::Status ret;
	ret.done = false;
	return ret;
}
