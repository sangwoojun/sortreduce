#ifndef __BCMERGEFLIP_H__
#define __BCMERGEFLIP_H__

#include <string>
#include <tuple>

#include "filekvreader.h"
#include "types.h"
#include "utils.h"

template <class K, class V>
class BCMergeFlip {
public:
	BCMergeFlip(std::string dir, std::string old_name, int toflip_fd);
	~BCMergeFlip();
	std::tuple<V,K,bool> Next();

private:
	SortReduceUtils::FileKvReader<V,K>* mp_old_reader;
	SortReduceUtils::FileKvReader<K,V>* mp_toflip_reader;

	std::tuple<V,K,bool> cur_buffered_val;

	int m_old_fd;
	int m_toflip_fd;
};

#endif
