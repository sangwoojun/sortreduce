#include "BCMergeFlip.h"

template <class K, class V>
BCMergeFlip<K,V>::BCMergeFlip(std::string dir, std::string old_name, std::string toflip_name ) {
	m_old_fd = open((dir+"/"+old_name).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);
	m_toflip_fd = open((dir+"/"+toflip_name).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);

	if ( m_old_fd < 0 ) {
		mp_old_reader = NULL;
	} else {
		mp_old_reader = new SortReduceUtils::FileKvReader<V,K>(m_old_fd);
	}
	mp_toflip_reader = new SortReduceUtils::FileKvReader<K,V>(m_toflip_fd);

	cur_buffered_val = mp_toflip_reader->Next();

	//cur_old_val = std::make_tuple(0,0,false);
	//cur_toflip_val = std::make_tuple(0,0,false);
}

template <class K, class V>
BCMergeFlip<K,V>::~BCMergeFlip() {
	delete mp_toflip_reader;
	if ( mp_old_reader != NULL ) {
		delete mp_old_reader;
	}
}

template <class K, class V>
inline std::tuple<V,K,bool> 
BCMergeFlip<K,V>::Next() {
	std::tuple<V,K, bool> ret = std::make_tuple(0,0,false);

	if ( mp_old_reader != NULL ) {
		if ( std::get<2>(cur_buffered_val) ) {
			std::tuple<K,V, bool> ret = mp_toflip_reader->Next();

			if (std::get<2>(ret) == false ) {
				std::tuple<V,K, bool> rold = cur_buffered_val;
				cur_buffered_val = mp_old_reader->Next();

				return rold;
			} else if (std::get<0>(ret) == std::get<0>(cur_buffered_val)) {
				std::tuple<V,K,bool> rold = cur_buffered_val;

				// commented so that cur_buffered_val also gets returned next call
				//cur_buffered_val = mp_old_reader->Next();

				return std::make_tuple(std::get<1>(ret), std::get<1>(rold), true);
			} else if ( std::get<0>(ret) > std::get<1>(cur_buffered_val) ) { // old first
				std::tuple<V,K, bool> rold = cur_buffered_val;
				cur_buffered_val = mp_old_reader->Next();

				return rold;
			} else { // toflip first
				std::tuple<K,V, bool> tf = mp_toflip_reader->Next();
				return std::make_tuple(std::get<1>(tf),1,std::get<2>(tf));
			}
		} else {
			std::tuple<K,V, bool> tf = mp_toflip_reader->Next();
			return std::make_tuple(std::get<1>(tf),1,std::get<2>(tf));
		}
	} else {
		std::tuple<K,V, bool> tf = mp_toflip_reader->Next();
		if ( std::get<2>(tf) ) {
			V val = std::get<1>(tf);
			return std::make_tuple(val,1,true);
		} else {
			return std::make_tuple(0,0,false);
		}
	}

	return ret;
}

TEMPLATE_EXPLICIT_INSTANTIATION(BCMergeFlip)
